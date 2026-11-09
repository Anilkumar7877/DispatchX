from datetime import datetime, timezone, timedelta
import asyncio
from contextlib import asynccontextmanager
import json
import os
import time
from typing import Optional, List, Dict, Any
import uuid

from fastapi import FastAPI, HTTPException, Body, Query, Header, Response, WebSocket, WebSocketDisconnect, status as http_status
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel, Field
import redis.asyncio as aioredis

try:
    from app.redis_client import redis_client
except ImportError:
    try:
        from dispatchx.app.redis_client import redis_client
    except ImportError:
        from .redis_client import redis_client


# ===========================================================================
# 4. KEYSPACE NOTIFICATIONS & BACKGROUND EXPIRY LISTENER
# ===========================================================================
def handle_unit_expired(unit_id: str):
    """
    Handles expiration of a unit's heartbeat key (hb:unit:{id}).
    Marks the unit offline across all data structures and broadcasts the offline event.
    """
    unit_key = f"unit:{unit_id}"
    geo_member = f"unit:{unit_id}"

    # Redis Command: ZREM units:geo unit:{id}
    # Why: Remove the offline ambulance from geospatial indexing so it is omitted from proximity queries.
    redis_client.zrem("units:geo", geo_member)

    # Redis Command: SREM units:free {id} & SREM units:busy {id}
    # Why: An offline ambulance is unavailable; purge it from availability tracking sets.
    redis_client.srem("units:free", unit_id)
    redis_client.srem("units:busy", unit_id)

    # Redis Command: HSET unit:{id} status offline
    # Why: Update the unit hash record to reflect its offline status.
    redis_client.hset(unit_key, "status", "offline")

    # Redis Command: PUBLISH unit:updates <json>
    # Why: Broadcast the offline event so connected WebSockets/dashboards remove the marker in real time.
    offline_msg = {
        "unit_id": unit_id,
        "lat": None,
        "lon": None,
        "status": "offline",
        "timestamp": time.time(),
    }
    redis_client.publish("unit:updates", json.dumps(offline_msg))


async def listen_to_keyspace_expirations():
    """
    Background worker that listens for Redis keyspace expiry events on __keyevent@0__:expired.
    """
    async_redis = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
    pubsub = async_redis.pubsub()

    # Redis Command: SUBSCRIBE __keyevent@0__:expired
    # Why: Redis keyspace notifications emit an event to this channel whenever any key expires.
    await pubsub.subscribe("__keyevent@0__:expired")

    try:
        async for message in pubsub.listen():
            if message and message.get("type") == "message":
                expired_key = message.get("data", "")
                if expired_key.startswith("hb:unit:"):
                    unit_id = expired_key.split("hb:unit:", 1)[1]
                    handle_unit_expired(unit_id)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            await pubsub.unsubscribe("__keyevent@0__:expired")
            await pubsub.aclose()
            await async_redis.aclose()
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup:
    # 1. Redis Command: CONFIG SET notify-keyspace-events Ex
    # Why: Enables keyspace notifications specifically for expired events ('x') on keyspace ('E').
    redis_client.config_set("notify-keyspace-events", "Ex")

    # 2. Redis Command: XGROUP CREATE incidents:stream dispatchers $ MKSTREAM
    # Why: Creates a named consumer group ('dispatchers') on 'incidents:stream' for worker distribution.
    # id='$' means consumers will receive only new messages written after group creation.
    # mkstream=True automatically creates the stream if it does not already exist.
    try:
        redis_client.xgroup_create("incidents:stream", "dispatchers", id="$", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            pass  # Consumer group already exists, ignore
        else:
            raise exc

    # Start background task to listen for heartbeat expirations
    expiry_task = asyncio.create_task(listen_to_keyspace_expirations())

    yield

    # Shutdown:
    expiry_task.cancel()
    try:
        await expiry_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="RedAlert",
    description="Real-time ambulance dispatch system - Phases 1 to 5",
    version="5.0.0",
    lifespan=lifespan,
)


class UnitRegisterRequest(BaseModel):
    id: str = Field(..., description="Unique identifier for the ambulance unit")
    type: str = Field(..., description="Ambulance unit type, e.g., ALS, BLS")
    status: str = Field(default="free", description="Current status of the unit (default: 'free')")
    driver: str = Field(..., description="Assigned driver name")


class UnitStatusUpdateRequest(BaseModel):
    status: str = Field(..., description="New status to set for the unit")


class UnitLocationUpdateRequest(BaseModel):
    lat: Optional[float] = Field(None, description="Latitude of the unit (-90 to 90)")
    lon: Optional[float] = Field(None, description="Longitude of the unit (-180 to 180)")


class IncidentCreateRequest(BaseModel):
    type: str = Field(..., description="Type of incident, e.g., Cardiac Arrest, Traffic Accident")
    lat: float = Field(..., description="Latitude of the incident")
    lon: float = Field(..., description="Longitude of the incident")
    severity: int = Field(..., ge=1, le=10, description="Severity rating from 1 (minor) to 10 (critical)")
    zone: Optional[str] = Field(default="central", description="Geographic zone of the incident (default: 'central')")


class ManualDispatchRequest(BaseModel):
    incident_id: str = Field(..., description="ID of the emergency incident to assign")
    unit_id: str = Field(..., description="ID of the ambulance unit to dispatch")


# ---------------------------------------------------------------------------
# GET /dashboard - Serves Leaflet Dashboard HTML
# ---------------------------------------------------------------------------
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    paths = [
        os.path.join(os.path.dirname(__file__), "..", "dashboard.html"),
        os.path.join(os.path.dirname(__file__), "dashboard.html"),
        "dashboard.html",
        "dispatchx/dashboard.html",
    ]
    for p in paths:
        if os.path.exists(p):
            return FileResponse(p)
    return HTMLResponse("<h1>Dashboard HTML not found</h1>", status_code=404)


@app.get("/")
def get_root():
    return {
        "service": "RedAlert",
        "description": "Real-time Ambulance Dispatch System",
        "phases_implemented": [1, 2, 3, 4, 5],
        "dashboard_url": "/dashboard",
        "websocket_url": "/ws",
    }


# ===========================================================================
# PHASE 1 ENDPOINTS
# ===========================================================================

# ---------------------------------------------------------------------------
# POST /units
# ---------------------------------------------------------------------------
@app.post("/units", status_code=http_status.HTTP_201_CREATED)
def register_unit(unit: UnitRegisterRequest):
    unit_key = f"unit:{unit.id}"

    # Redis Command: HSET unit:{id} mapping
    # Why: HSET stores the ambulance attributes (type, status, driver) as structured fields
    # within a single Redis hash at key unit:{id}.
    redis_client.hset(
        name=unit_key,
        mapping={
            "type": unit.type,
            "status": unit.status,
            "driver": unit.driver,
        },
    )

    # Redis Command: SADD units:free {id} (or units:busy)
    # Why Set: Sets guarantee member uniqueness and allow O(1) membership operations.
    if unit.status == "free":
        redis_client.sadd("units:free", unit.id)
        redis_client.srem("units:busy", unit.id)
    else:
        redis_client.sadd("units:busy", unit.id)
        redis_client.srem("units:free", unit.id)

    return {
        "id": unit.id,
        "type": unit.type,
        "status": unit.status,
        "driver": unit.driver,
    }


# ---------------------------------------------------------------------------
# GET /units/nearest (Phase 2 - defined before /units/{unit_id} to avoid collision)
# ---------------------------------------------------------------------------
@app.get("/units/nearest")
def get_nearest_units(
    lat: float = Query(..., description="Search origin latitude"),
    lon: float = Query(..., description="Search origin longitude"),
    radius_km: float = Query(default=5.0, description="Search radius in kilometers"),
    count: int = Query(default=5, description="Maximum number of units to return"),
):
    # Redis Command: GEOSEARCH units:geo FROMLONLAT <lon> <lat> BYRADIUS <radius> km ASC COUNT <count> WITHDIST
    # Why: GEOSEARCH efficiently queries the geospatial sorted set centered at (lon, lat)
    # within radius_km, sorted by proximity in ascending order (ASC), returning member keys and distances.
    raw_results = redis_client.geosearch(
        "units:geo",
        longitude=lon,
        latitude=lat,
        radius=radius_km,
        unit="km",
        sort="ASC",
        count=count,
        withdist=True,
    )

    nearest_free_units: List[Dict[str, Any]] = []

    # Filter to only include units whose Redis hash status is "free"
    for member, distance in raw_results:
        unit_key = member if member.startswith("unit:") else f"unit:{member}"

        # Redis Command: HGET unit:{id} status
        # Why: Inspects the unit's hash in Redis to confirm whether its current status is 'free'.
        unit_status = redis_client.hget(unit_key, "status")
        if unit_status == "free":
            unit_id_clean = member.split("unit:", 1)[1] if member.startswith("unit:") else member
            nearest_free_units.append({
                "id": unit_id_clean,
                "unit_id": unit_id_clean,
                "distance": round(distance, 4),
                "distance_km": round(distance, 4),
                "status": unit_status,
            })

    return nearest_free_units


# ---------------------------------------------------------------------------
# GET /units/{id}
# ---------------------------------------------------------------------------
@app.get("/units/{unit_id}")
def get_unit(unit_id: str):
    unit_key = f"unit:{unit_id}"

    # Redis Command: HGETALL unit:{id}
    # Why: HGETALL retrieves all field-value pairs stored in the Redis hash.
    unit_data = redis_client.hgetall(unit_key)

    if not unit_data:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"Unit '{unit_id}' not found",
        )

    return unit_data


# ---------------------------------------------------------------------------
# PATCH /units/{id}/status (Phases 1, 3, & 4: SMOVE and PUBLISH unit:updates)
# ---------------------------------------------------------------------------
@app.patch("/units/{unit_id}/status")
def update_unit_status(unit_id: str, payload: UnitStatusUpdateRequest):
    unit_key = f"unit:{unit_id}"

    # Redis Command: EXISTS unit:{id}
    # Why: Check if the unit hash exists before updating.
    if not redis_client.exists(unit_key):
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"Unit '{unit_id}' not found",
        )

    # Redis Command: HSET unit:{id} status <new_status>
    # Why: HSET updates only the specific 'status' field in the unit hash.
    redis_client.hset(unit_key, "status", payload.status)

    # Redis Command: SMOVE source destination member
    # Why Set: SMOVE atomically moves the unit between availability sets in O(1) time.
    if payload.status == "free":
        moved = redis_client.smove("units:busy", "units:free", unit_id)
        if not moved:
            redis_client.sadd("units:free", unit_id)
            redis_client.srem("units:busy", unit_id)
    else:
        moved = redis_client.smove("units:free", "units:busy", unit_id)
        if not moved:
            redis_client.sadd("units:busy", unit_id)
            redis_client.srem("units:free", unit_id)

    # Retrieve current position for the broadcast
    pos = redis_client.geopos("units:geo", f"unit:{unit_id}")
    unit_lon, unit_lat = pos[0] if (pos and pos[0] is not None) else (None, None)

    # Redis Command: PUBLISH unit:updates <json>
    # Why Pub/Sub: Notifies real-time WebSockets of the unit's new status.
    status_update_event = {
        "unit_id": unit_id,
        "lat": unit_lat,
        "lon": unit_lon,
        "status": payload.status,
        "timestamp": time.time(),
    }
    redis_client.publish("unit:updates", json.dumps(status_update_event))

    return {
        "id": unit_id,
        "status": payload.status,
    }


# ---------------------------------------------------------------------------
# POST /units/{id}/heartbeat (Phases 1 & 7: Ephemeral TTL + Daily Uptime Bitmaps)
# ---------------------------------------------------------------------------
@app.post("/units/{unit_id}/heartbeat")
def send_heartbeat(unit_id: str):
    heartbeat_key = f"hb:unit:{unit_id}"

    # Redis Command: SET hb:unit:{id} 1 EX 10
    # Why: Sets ephemeral key with 10-second TTL to maintain online liveness.
    redis_client.set(heartbeat_key, 1, ex=10)

    # -----------------------------------------------------------------------
    # Phase 7: Uptime Bitmap Tracking
    # -----------------------------------------------------------------------
    # Redis Command: SETBIT unit:{id}:online:{YYYY-MM-DD} <hour> 1
    # Why Bitmaps: Storing 24 hours of uptime in a Redis string bitmap requires only
    # 3 bytes (24 bits) per unit per day, instead of hundreds of bytes in sets or lists.
    # Trade-off: Bitmaps offer massive memory savings for binary flags, but cannot store
    # granular metadata (like sub-hour timestamps or duration) per bit.
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    current_hour = now.hour
    bitmap_key = f"unit:{unit_id}:online:{today_str}"
    redis_client.setbit(bitmap_key, current_hour, 1)

    return {
        "unit_id": unit_id,
        "status": "ok",
        "message": "Heartbeat registered",
        "ttl": 10,
        "hour_recorded": current_hour,
        "date": today_str,
    }


# ---------------------------------------------------------------------------
# GET /units/{id}/uptime (Phase 7: BITCOUNT & Bit Inspection)
# ---------------------------------------------------------------------------
@app.get("/units/{unit_id}/uptime")
def get_unit_uptime(
    unit_id: str,
    date: str = Query(..., description="Date formatted as YYYY-MM-DD"),
):
    bitmap_key = f"unit:{unit_id}:online:{date}"

    # Redis Command: BITCOUNT unit:{id}:online:{YYYY-MM-DD}
    # Why: In-memory hardware-level population count (POPCNT) returning total bits set to 1.
    hours_online = redis_client.bitcount(bitmap_key)

    # Redis Command: GETBIT unit:{id}:online:{YYYY-MM-DD} <offset>
    # Why: Reads individual bits 0..23 representing each hour of the day.
    active_hours: List[int] = []
    for h in range(24):
        bit_val = redis_client.getbit(bitmap_key, h)
        if bit_val == 1:
            active_hours.append(h)

    return {
        "unit_id": unit_id,
        "date": date,
        "hours_online": hours_online,
        "active_hours": active_hours,
    }


# ---------------------------------------------------------------------------
# GET /units/{id}/online
# ---------------------------------------------------------------------------
@app.get("/units/{unit_id}/online")
def get_unit_online_status(unit_id: str):
    heartbeat_key = f"hb:unit:{unit_id}"

    # Redis Command: TTL hb:unit:{id}
    # Why: TTL returns remaining lifespan in seconds.
    remaining_ttl = redis_client.ttl(heartbeat_key)
    is_online = remaining_ttl > 0

    return {
        "online": is_online,
        "ttl": remaining_ttl if is_online else 0,
    }


# ===========================================================================
# PHASE 2 & PHASE 4: LOCATION & PUBLISHING
# ===========================================================================

# ---------------------------------------------------------------------------
# POST /units/{id}/location (GEOADD and PUBLISH unit:updates)
# ---------------------------------------------------------------------------
@app.post("/units/{unit_id}/location")
def update_unit_location(
    unit_id: str,
    payload: Optional[UnitLocationUpdateRequest] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
):
    latitude = payload.lat if (payload and payload.lat is not None) else lat
    longitude = payload.lon if (payload and payload.lon is not None) else lon

    if latitude is None or longitude is None:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Both 'lat' and 'lon' are required in request body or query parameters",
        )

    member = f"unit:{unit_id}" if not unit_id.startswith("unit:") else unit_id

    # Redis Command: GEOADD units:geo lon lat unit:{id}
    # Why: Stores coordinates in the geospatial sorted set. Longitude comes first.
    redis_client.geoadd("units:geo", (longitude, latitude, member))

    # Retrieve current status from unit hash
    current_status = redis_client.hget(f"unit:{unit_id}", "status") or "free"

    # Redis Command: PUBLISH unit:updates <json>
    # Why Pub/Sub: Real-time broadcast of unit location to the WebSocket dashboard.
    location_update_event = {
        "unit_id": unit_id,
        "lat": latitude,
        "lon": longitude,
        "status": current_status,
        "timestamp": time.time(),
    }
    redis_client.publish("unit:updates", json.dumps(location_update_event))

    return {
        "id": unit_id,
        "lat": latitude,
        "lon": longitude,
        "message": "Unit location updated successfully",
    }


# ---------------------------------------------------------------------------
# DELETE /units/{id}/location
# ---------------------------------------------------------------------------
@app.delete("/units/{unit_id}/location")
def delete_unit_location(unit_id: str):
    member = f"unit:{unit_id}" if not unit_id.startswith("unit:") else unit_id

    # Redis Command: ZREM units:geo unit:{id}
    # Why: Removes member from geospatial index.
    removed_count = redis_client.zrem("units:geo", member)

    return {
        "id": unit_id,
        "removed": bool(removed_count),
        "message": "Unit location removed from geo index" if removed_count else "Unit location was not found in geo index",
    }


# ===========================================================================
# PHASE 3: ZONES, AVAILABILITY, & INCIDENTS
# ===========================================================================

# ---------------------------------------------------------------------------
# POST /zones/{zone}/units/{id}
# ---------------------------------------------------------------------------
@app.post("/zones/{zone}/units/{unit_id}")
def add_unit_to_zone(zone: str, unit_id: str):
    zone_key = f"zone:{zone}:units"

    # Redis Command: SADD zone:{zone}:units {id}
    # Why Set: Guarantees unique members and O(1) addition.
    redis_client.sadd(zone_key, unit_id)

    return {
        "zone": zone,
        "unit_id": unit_id,
        "message": f"Unit '{unit_id}' added to zone '{zone}'",
    }


# ---------------------------------------------------------------------------
# DELETE /zones/{zone}/units/{id}
# ---------------------------------------------------------------------------
@app.delete("/zones/{zone}/units/{unit_id}")
def remove_unit_from_zone(zone: str, unit_id: str):
    zone_key = f"zone:{zone}:units"

    # Redis Command: SREM zone:{zone}:units {id}
    # Why Set: O(1) removal of member from set.
    removed_count = redis_client.srem(zone_key, unit_id)

    return {
        "zone": zone,
        "unit_id": unit_id,
        "removed": bool(removed_count),
        "message": f"Unit '{unit_id}' removed from zone '{zone}'" if removed_count else f"Unit '{unit_id}' was not found in zone '{zone}'",
    }


# ---------------------------------------------------------------------------
# GET /zones/{zone}/units/free
# ---------------------------------------------------------------------------
@app.get("/zones/{zone}/units/free")
def get_free_units_in_zone(zone: str):
    zone_key = f"zone:{zone}:units"

    # Redis Command: SINTER zone:{zone}:units units:free
    # Why Set: In-memory set intersection directly on Redis server.
    free_units = redis_client.sinter(zone_key, "units:free")

    return sorted(list(free_units))


# ---------------------------------------------------------------------------
# GET /zones/{zone}/units
# ---------------------------------------------------------------------------
@app.get("/zones/{zone}/units")
def list_units_in_zone(zone: str):
    zone_key = f"zone:{zone}:units"

    # Redis Command: SMEMBERS zone:{zone}:units
    # Why Set: Retrieves all members of the set.
    members = redis_client.smembers(zone_key)

    return sorted(list(members))


# ---------------------------------------------------------------------------
# POST /incidents (Phases 3, 4, 5 & 7: Rate Limiting, HLL, Stream, Pub/Sub)
# ---------------------------------------------------------------------------
@app.post("/incidents", status_code=http_status.HTTP_201_CREATED)
def create_incident(
    incident: IncidentCreateRequest,
    response: Response,
    x_source: Optional[str] = Header(None, alias="X-Source", description="Unique identifier for incident source (e.g. phone number or client ID)"),
):
    if not x_source:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail="Header 'X-Source' is required to report an incident",
        )

    # -----------------------------------------------------------------------
    # Phase 7: Fixed Window Rate Limiter
    # -----------------------------------------------------------------------
    # Redis Commands: INCR rl:{source}:{minute} + EXPIRE ... 60 NX in a pipeline
    # Why Pipeline: Bundles INCR and conditional EXPIRE into 1 network round-trip.
    # Fixed Window Trade-off: Fixed window limiters are computationally minimal (O(1) counter),
    # but suffer from the boundary burst weakness: up to 2x the limit can be admitted
    # across a minute boundary (e.g., 5 at 00:59 and 5 at 01:00).
    now_dt = datetime.now()
    window_minute = now_dt.strftime("%Y-%m-%dT%H:%M")
    rate_limit_key = f"rl:{x_source}:{window_minute}"

    pipe = redis_client.pipeline(transaction=False)
    pipe.incr(rate_limit_key)
    pipe.expire(rate_limit_key, 60, nx=True)
    pipe_results = pipe.execute()
    current_request_count = pipe_results[0]

    if current_request_count > 5:
        # Exceeded limit: return 429 with Retry-After header equal to key TTL
        ttl_remaining = redis_client.ttl(rate_limit_key)
        retry_after_sec = max(1, ttl_remaining if ttl_remaining > 0 else 60)
        return Response(
            content=json.dumps({
                "detail": "Rate limit exceeded. Maximum 5 requests per minute allowed.",
                "source": x_source,
                "current_count": current_request_count,
                "retry_after": retry_after_sec,
            }),
            status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
            media_type="application/json",
            headers={"Retry-After": str(retry_after_sec)},
        )

    # -----------------------------------------------------------------------
    # Phase 7: HyperLogLog Cardinality Tracking
    # -----------------------------------------------------------------------
    # Redis Commands: PFADD callers:{YYYY-MM-DD} <source> & PFADD areas:{YYYY-MM-DD} <zone>
    # Why HyperLogLog: Probabilistic cardinality estimation using a constant ~12 KB memory
    # per key regardless of cardinality (whether 10 callers or 100 million).
    # Trade-off: Provides a fast approximation with a standard error of ~0.81%, but cannot
    # recall individual member identities.
    today_date = now_dt.strftime("%Y-%m-%d")
    incident_zone = incident.zone or "central"
    redis_client.pfadd(f"callers:{today_date}", x_source)
    redis_client.pfadd(f"areas:{today_date}", incident_zone)

    # -----------------------------------------------------------------------
    # Incident Persistence (Phases 3, 4, 5)
    # -----------------------------------------------------------------------
    # Redis Command: INCR incident:id
    # Why: Atomic auto-incrementing ID generator.
    incident_id = redis_client.incr("incident:id")
    incident_key = f"incident:{incident_id}"

    # Redis Command: HSET incident:{id} mapping
    # Why Hash: Stores structured entity fields efficiently.
    redis_client.hset(
        incident_key,
        mapping={
            "id": str(incident_id),
            "type": incident.type,
            "lat": str(incident.lat),
            "lon": str(incident.lon),
            "severity": str(incident.severity),
            "zone": incident_zone,
            "source": x_source,
            "status": "pending",
        },
    )

    # Redis Command: ZADD incidents:queue <severity> incident:{id}
    # Why Sorted Set: Forms a priority queue ordered by severity score.
    redis_client.zadd("incidents:queue", {incident_key: incident.severity})

    # Redis Command: XADD incidents:stream * <fields...>
    # Why Stream: Streams provide durable, persistent message logging with consumer groups and acknowledgments (XACK).
    # Unlike Pub/Sub which is fire-and-forget, Streams keep messages safe in memory and disk until acknowledged.
    stream_fields = {
        "incident_id": str(incident_id),
        "type": incident.type,
        "lat": str(incident.lat),
        "lon": str(incident.lon),
        "severity": str(incident.severity),
        "zone": incident_zone,
        "source": x_source,
    }
    redis_client.xadd("incidents:stream", stream_fields)

    # Redis Command: PUBLISH incident:updates <json>
    # Why Pub/Sub: Real-time notification of newly created emergency incidents.
    incident_created_event = {
        "event": "created",
        "id": str(incident_id),
        "incident_id": str(incident_id),
        "type": incident.type,
        "lat": incident.lat,
        "lon": incident.lon,
        "severity": incident.severity,
        "zone": incident_zone,
        "source": x_source,
        "status": "pending",
        "timestamp": time.time(),
    }
    redis_client.publish("incident:updates", json.dumps(incident_created_event))

    return {
        "id": str(incident_id),
        "incident_key": incident_key,
        "type": incident.type,
        "lat": incident.lat,
        "lon": incident.lon,
        "severity": incident.severity,
        "zone": incident_zone,
        "source": x_source,
        "status": "pending",
    }


# ---------------------------------------------------------------------------
# GET /incidents/next (Phase 3 & 4: ZPOPMAX and PUBLISH incident:updates)
# ---------------------------------------------------------------------------
@app.get("/incidents/next")
def get_next_incident():
    # Redis Command: ZPOPMAX incidents:queue 1
    # Why Sorted Set: Atomically pops the highest severity emergency call.
    popped = redis_client.zpopmax("incidents:queue", count=1)

    if not popped:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="Incident queue is empty",
        )

    incident_key, score = popped[0]

    # Redis Command: HGETALL incident:{id}
    # Why Hash: Retrieves full details of the popped incident entity.
    incident_data = redis_client.hgetall(incident_key)

    incident_id = incident_key.split("incident:", 1)[1] if incident_key.startswith("incident:") else incident_key

    # Redis Command: PUBLISH incident:updates <json>
    # Why Pub/Sub: Real-time broadcast that an incident was popped/dispatched.
    incident_popped_event = {
        "event": "popped",
        "id": str(incident_id),
        "incident_id": str(incident_id),
        "type": incident_data.get("type", ""),
        "lat": float(incident_data.get("lat", 0)) if "lat" in incident_data else None,
        "lon": float(incident_data.get("lon", 0)) if "lon" in incident_data else None,
        "severity": int(score) if isinstance(score, (int, float)) else score,
        "timestamp": time.time(),
    }
    redis_client.publish("incident:updates", json.dumps(incident_popped_event))

    if not incident_data:
        return {"id": incident_id, "incident_key": incident_key, "severity": int(score)}

    return incident_data


# ---------------------------------------------------------------------------
# GET /incidents/queue
# ---------------------------------------------------------------------------
@app.get("/incidents/queue")
def get_incidents_queue():
    # Redis Command: ZRANGE incidents:queue 0 -1 REV WITHSCORES
    # Why Sorted Set: Non-destructively inspects queue from highest to lowest severity.
    queue_entries = redis_client.zrange(
        "incidents:queue", 0, -1, desc=True, withscores=True
    )

    return [
        {
            "id": member.split("incident:", 1)[1] if member.startswith("incident:") else member,
            "incident": member,
            "severity": int(score) if score.is_integer() else score,
            "score": score,
        }
        for member, score in queue_entries
    ]


# ===========================================================================
# PHASE 7: STATS & ANALYTICS (HYPERLOGLOG & BITMAP OVERLAP)
# ===========================================================================

# ---------------------------------------------------------------------------
# GET /stats/daily (Phase 7: PFCOUNT for callers & areas)
# ---------------------------------------------------------------------------
@app.get("/stats/daily")
def get_daily_stats(date: str = Query(..., description="Date formatted as YYYY-MM-DD")):
    callers_key = f"callers:{date}"
    areas_key = f"areas:{date}"

    # Redis Command: PFCOUNT callers:{YYYY-MM-DD}
    # Why: Retrieves estimated unique cardinality without needing to store all caller strings.
    unique_callers = redis_client.pfcount(callers_key)

    # Redis Command: PFCOUNT areas:{YYYY-MM-DD}
    unique_areas = redis_client.pfcount(areas_key)

    return {
        "date": date,
        "unique_callers": unique_callers,
        "unique_areas": unique_areas,
    }


# ---------------------------------------------------------------------------
# GET /stats/range (Phase 7: PFMERGE multi-day union)
# ---------------------------------------------------------------------------
@app.get("/stats/range")
def get_range_stats(
    from_date: str = Query(..., alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: str = Query(..., alias="to", description="End date (YYYY-MM-DD)"),
):
    try:
        start_dt = datetime.strptime(from_date, "%Y-%m-%d")
        end_dt = datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail="Dates must be in format YYYY-MM-DD",
        )

    if start_dt > end_dt:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail="'from' date must be before or equal to 'to' date",
        )

    # Generate day keys in range
    day_keys: List[str] = []
    curr = start_dt
    while curr <= end_dt:
        day_keys.append(f"callers:{curr.strftime('%Y-%m-%d')}")
        curr += timedelta(days=1)

    # Filter to existing keys
    existing_keys = [k for k in day_keys if redis_client.exists(k)]

    if not existing_keys:
        return {
            "from": from_date,
            "to": to_date,
            "days_checked": len(day_keys),
            "unique_callers": 0,
        }

    # Redis Command: PFMERGE temp_key key1 key2 ...
    # Why: In-memory union of multiple HyperLogLog registers into one key.
    temp_key = f"temp:callers:range:{uuid.uuid4()}"
    try:
        redis_client.pfmerge(temp_key, *existing_keys)
        unique_callers = redis_client.pfcount(temp_key)
    finally:
        # Clean up temporary key
        redis_client.delete(temp_key)

    return {
        "from": from_date,
        "to": to_date,
        "days_checked": len(day_keys),
        "days_found": len(existing_keys),
        "unique_callers": unique_callers,
    }


# ---------------------------------------------------------------------------
# GET /stats/units-overlap (Phase 7: BITOP AND multi-unit uptime overlap)
# ---------------------------------------------------------------------------
@app.get("/stats/units-overlap")
def get_units_overlap(
    ids: str = Query(..., description="Comma-separated unit ids, e.g. '1,2,3' or 'sim-01,sim-02'"),
    date: str = Query(..., description="Date formatted as YYYY-MM-DD"),
):
    unit_ids = [uid.strip() for uid in ids.split(",") if uid.strip()]
    if not unit_ids:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail="At least one unit id must be provided in 'ids'",
        )

    bitmap_keys = [f"unit:{uid}:online:{date}" for uid in unit_ids]

    # Temporary key for bitwise AND operation
    temp_key = f"temp:overlap:{uuid.uuid4()}"
    try:
        # Redis Command: BITOP AND temp_key key1 key2 ...
        # Why: Bitwise AND computes the intersection of 24-hour schedules in hardware-speed bit operations.
        redis_client.bitop("AND", temp_key, *bitmap_keys)

        # Redis Command: BITCOUNT temp_key
        overlap_hours_count = redis_client.bitcount(temp_key)

        # Inspect bits 0..23
        overlap_hours: List[int] = []
        for h in range(24):
            if redis_client.getbit(temp_key, h) == 1:
                overlap_hours.append(h)
    finally:
        redis_client.delete(temp_key)

    return {
        "date": date,
        "unit_ids": unit_ids,
        "overlap_hours_count": overlap_hours_count,
        "overlap_hours": overlap_hours,
    }


# ---------------------------------------------------------------------------
# 5. GET /stream/stats (Phase 5: XLEN, XPENDING, XINFO CONSUMERS)
# ---------------------------------------------------------------------------
@app.get("/stream/stats")
def get_stream_stats():
    # Redis Command: XLEN incidents:stream
    # Why: Returns the total number of entries in the stream.
    try:
        stream_length = redis_client.xlen("incidents:stream")
    except Exception:
        stream_length = 0

    # Redis Command: XPENDING incidents:stream dispatchers
    # Why: Returns the Pending Entries List (PEL) summary showing unacknowledged messages.
    try:
        pending_summary = redis_client.xpending("incidents:stream", "dispatchers")
    except Exception:
        pending_summary = {"pending": 0, "min": None, "max": None, "consumers": []}

    # Redis Command: XINFO CONSUMERS incidents:stream dispatchers
    # Why: Retrieves state for all registered consumers in the group (pending counts, idle times).
    try:
        consumers_info = redis_client.xinfo_consumers("incidents:stream", "dispatchers")
    except Exception:
        consumers_info = []

    return {
        "stream": "incidents:stream",
        "group": "dispatchers",
        "length": stream_length,
        "pending": pending_summary,
        "consumers": consumers_info,
    }


# ===========================================================================
# PHASE 5: AUTOMATED DISPATCH ENGINE & MISSION LIFECYCLE
# ===========================================================================

# ---------------------------------------------------------------------------
# POST /dispatch/auto - Auto-assigns closest free unit to highest priority incident
# ---------------------------------------------------------------------------
@app.post("/dispatch/auto")
def auto_dispatch_incident(
    radius_km: float = Query(default=10.0, description="Search radius around the emergency in km")
):
    # 1. Pop highest severity incident from priority queue
    # Redis Command: ZPOPMAX incidents:queue 1
    popped = redis_client.zpopmax("incidents:queue", count=1)
    if not popped:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="No pending incidents in queue to dispatch",
        )

    incident_key, score = popped[0]
    incident_data = redis_client.hgetall(incident_key)

    if not incident_data or "lat" not in incident_data or "lon" not in incident_data:
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Corrupt or missing incident data for '{incident_key}'",
        )

    incident_lat = float(incident_data["lat"])
    incident_lon = float(incident_data["lon"])
    incident_id = incident_data.get("id") or incident_key.split(":")[-1]

    # 2. Find closest ambulances via Geospatial search
    # Redis Command: GEOSEARCH units:geo FROMLONLAT <lon> <lat> BYRADIUS <radius> km ASC WITHDIST
    nearby_units = redis_client.geosearch(
        "units:geo",
        longitude=incident_lon,
        latitude=incident_lat,
        radius=radius_km,
        unit="km",
        sort="ASC",
        withdist=True,
    )

    selected_unit_id: Optional[str] = None
    selected_distance: float = 0.0

    # 3. Locate closest available unit that is currently free and online
    for member, distance in nearby_units:
        raw_id = member.split("unit:", 1)[1] if member.startswith("unit:") else member

        # Redis Commands: SISMEMBER units:free {id} & EXISTS hb:unit:{id}
        # Why: Confirms the ambulance is both marked 'free' and actively sending heartbeats
        is_free = redis_client.sismember("units:free", raw_id) == 1
        is_online = redis_client.exists(f"hb:unit:{raw_id}") == 1

        if is_free and is_online:
            selected_unit_id = raw_id
            selected_distance = float(distance)
            break

    # If no online free ambulance found within radius:
    if not selected_unit_id:
        # Re-queue the incident so it is not lost!
        # Redis Command: ZADD incidents:queue <score> incident:{id}
        redis_client.zadd("incidents:queue", {incident_key: score})
        raise HTTPException(
            status_code=http_status.HTTP_409_CONFLICT,
            detail=f"No online free ambulances within {radius_km} km of Incident #{incident_id}. Incident has been returned to queue.",
        )

    # 4. Perform atomic dispatch assignment
    unit_key = f"unit:{selected_unit_id}"
    dispatched_timestamp = time.time()

    # Move unit availability: units:free -> units:busy
    # Redis Command: SMOVE units:free units:busy {id}
    redis_client.smove("units:free", "units:busy", selected_unit_id)

    # Update unit status and link active mission
    # Redis Command: HSET unit:{id} status dispatched current_incident incident:{id}
    redis_client.hset(
        unit_key,
        mapping={
            "status": "dispatched",
            "current_incident": incident_key,
        },
    )

    # Update incident status and link assigned ambulance
    # Redis Command: HSET incident:{id} status dispatched assigned_unit unit:{id} dispatched_at <ts>
    redis_client.hset(
        incident_key,
        mapping={
            "status": "dispatched",
            "assigned_unit": selected_unit_id,
            "dispatched_at": str(dispatched_timestamp),
        },
    )

    # Fetch vehicle telemetry for broadcast
    pos = redis_client.geopos("units:geo", f"unit:{selected_unit_id}")
    unit_lon, unit_lat = pos[0] if (pos and pos[0] is not None) else (None, None)
    unit_driver = redis_client.hget(unit_key, "driver") or "Unknown"
    unit_type = redis_client.hget(unit_key, "type") or "ALS"

    # 5. Broadcast dispatch events across WebSockets
    # Redis Command: PUBLISH unit:updates & PUBLISH incident:updates
    redis_client.publish(
        "unit:updates",
        json.dumps({
            "unit_id": selected_unit_id,
            "lat": unit_lat,
            "lon": unit_lon,
            "status": "dispatched",
            "current_incident": incident_key,
            "timestamp": dispatched_timestamp,
        }),
    )
    redis_client.publish(
        "incident:updates",
        json.dumps({
            "event": "dispatched",
            "id": str(incident_id),
            "incident_id": str(incident_id),
            "type": incident_data.get("type", "Emergency"),
            "assigned_unit": selected_unit_id,
            "distance_km": round(selected_distance, 3),
            "timestamp": dispatched_timestamp,
        }),
    )

    # Estimate city ETA (assuming average urban speed of 35 km/h)
    estimated_eta_min = round((selected_distance / 35.0) * 60.0, 1)

    return {
        "status": "dispatched",
        "message": f"Ambulance '{selected_unit_id}' successfully dispatched to Incident #{incident_id}",
        "incident": {
            "id": str(incident_id),
            "type": incident_data.get("type"),
            "severity": int(score) if isinstance(score, (int, float)) else score,
            "coordinates": {"lat": incident_lat, "lon": incident_lon},
        },
        "assigned_unit": {
            "id": selected_unit_id,
            "driver": unit_driver,
            "type": unit_type,
            "distance_km": round(selected_distance, 3),
            "estimated_eta_minutes": max(1.0, estimated_eta_min),
        },
    }


# ---------------------------------------------------------------------------
# POST /dispatch/manual - Manually assign specific unit to an incident
# ---------------------------------------------------------------------------
@app.post("/dispatch/manual")
def manual_dispatch_incident(assignment: ManualDispatchRequest):
    incident_key = f"incident:{assignment.incident_id}" if not assignment.incident_id.startswith("incident:") else assignment.incident_id
    unit_key = f"unit:{assignment.unit_id}" if not assignment.unit_id.startswith("unit:") else assignment.unit_id
    raw_unit_id = assignment.unit_id.split("unit:", 1)[1] if assignment.unit_id.startswith("unit:") else assignment.unit_id
    raw_incident_id = assignment.incident_id.split("incident:", 1)[1] if assignment.incident_id.startswith("incident:") else assignment.incident_id

    if not redis_client.exists(incident_key):
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail=f"Incident '{assignment.incident_id}' not found")

    if not redis_client.exists(unit_key):
        raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail=f"Unit '{assignment.unit_id}' not found")

    if not redis_client.sismember("units:free", raw_unit_id):
        raise HTTPException(status_code=http_status.HTTP_409_CONFLICT, detail=f"Unit '{assignment.unit_id}' is not free")

    # Remove incident from queue if present
    # Redis Command: ZREM incidents:queue incident:{id}
    redis_client.zrem("incidents:queue", incident_key)

    # Atomically move unit:free -> units:busy
    # Redis Command: SMOVE units:free units:busy {id}
    redis_client.smove("units:free", "units:busy", raw_unit_id)

    dispatched_timestamp = time.time()
    redis_client.hset(unit_key, mapping={"status": "dispatched", "current_incident": incident_key})
    redis_client.hset(incident_key, mapping={"status": "dispatched", "assigned_unit": raw_unit_id, "dispatched_at": str(dispatched_timestamp)})

    pos = redis_client.geopos("units:geo", f"unit:{raw_unit_id}")
    unit_lon, unit_lat = pos[0] if (pos and pos[0] is not None) else (None, None)

    redis_client.publish("unit:updates", json.dumps({
        "unit_id": raw_unit_id,
        "lat": unit_lat,
        "lon": unit_lon,
        "status": "dispatched",
        "current_incident": incident_key,
        "timestamp": dispatched_timestamp,
    }))
    redis_client.publish("incident:updates", json.dumps({
        "event": "dispatched",
        "id": raw_incident_id,
        "incident_id": raw_incident_id,
        "assigned_unit": raw_unit_id,
        "timestamp": dispatched_timestamp,
    }))

    return {
        "status": "dispatched",
        "message": f"Unit '{raw_unit_id}' manually dispatched to Incident #{raw_incident_id}",
        "incident_id": raw_incident_id,
        "unit_id": raw_unit_id,
    }


# ---------------------------------------------------------------------------
# POST /dispatch/{incident_id}/resolve - Complete mission & release ambulance
# ---------------------------------------------------------------------------
@app.post("/dispatch/{incident_id}/resolve")
def resolve_incident(incident_id: str):
    incident_key = f"incident:{incident_id}" if not incident_id.startswith("incident:") else incident_id
    raw_incident_id = incident_id.split("incident:", 1)[1] if incident_id.startswith("incident:") else incident_id

    if not redis_client.exists(incident_key):
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"Incident '{incident_id}' not found",
        )

    assigned_unit_id = redis_client.hget(incident_key, "assigned_unit")
    resolved_timestamp = time.time()

    # Redis Command: HSET incident:{id} status resolved
    redis_client.hset(
        incident_key,
        mapping={
            "status": "resolved",
            "resolved_at": str(resolved_timestamp),
        },
    )

    # Release the assigned ambulance back to free status
    if assigned_unit_id:
        unit_key = f"unit:{assigned_unit_id}"
        # Redis Command: HDEL unit:{id} current_incident
        redis_client.hdel(unit_key, "current_incident")
        # Redis Command: HSET unit:{id} status free
        redis_client.hset(unit_key, "status", "free")
        # Redis Command: SMOVE units:busy units:free {id}
        redis_client.smove("units:busy", "units:free", assigned_unit_id)

        pos = redis_client.geopos("units:geo", f"unit:{assigned_unit_id}")
        unit_lon, unit_lat = pos[0] if (pos and pos[0] is not None) else (None, None)

        redis_client.publish(
            "unit:updates",
            json.dumps({
                "unit_id": assigned_unit_id,
                "lat": unit_lat,
                "lon": unit_lon,
                "status": "free",
                "timestamp": resolved_timestamp,
            }),
        )

    # Publish incident resolution event
    redis_client.publish(
        "incident:updates",
        json.dumps({
            "event": "resolved",
            "id": raw_incident_id,
            "incident_id": raw_incident_id,
            "released_unit": assigned_unit_id,
            "timestamp": resolved_timestamp,
        }),
    )

    return {
        "status": "resolved",
        "message": f"Incident #{raw_incident_id} successfully marked as resolved",
        "released_unit": assigned_unit_id,
    }


# ---------------------------------------------------------------------------
# GET /dispatch/active - List all currently active dispatches
# ---------------------------------------------------------------------------
@app.get("/dispatch/active")
def get_active_dispatches():
    # Redis Command: SMEMBERS units:busy
    busy_units = redis_client.smembers("units:busy")
    active_missions = []

    for unit_id in busy_units:
        unit_key = f"unit:{unit_id}"
        current_inc_key = redis_client.hget(unit_key, "current_incident")

        if current_inc_key:
            inc_data = redis_client.hgetall(current_inc_key)
            driver = redis_client.hget(unit_key, "driver")
            vehicle_type = redis_client.hget(unit_key, "type")
            pos = redis_client.geopos("units:geo", f"unit:{unit_id}")
            unit_lon, unit_lat = pos[0] if (pos and pos[0] is not None) else (None, None)

            active_missions.append({
                "unit": {
                    "id": unit_id,
                    "driver": driver,
                    "type": vehicle_type,
                    "location": {"lat": unit_lat, "lon": unit_lon},
                },
                "incident": inc_data,
                "dispatched_at": inc_data.get("dispatched_at"),
            })

    return active_missions


# ===========================================================================
# 2. WEBSOCKET ENDPOINT (/ws)
# ===========================================================================
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    # Redis Async Connection & PubSub
    async_redis = aioredis.Redis(host="localhost", port=6379, decode_responses=True)
    pubsub = async_redis.pubsub()

    # Redis Command: SUBSCRIBE unit:updates incident:updates
    # Why Pub/Sub: Low-latency streaming of telemetry & dispatch events to WebSocket clients.
    await pubsub.subscribe("unit:updates", "incident:updates")

    async def forward_pubsub_messages():
        try:
            async for message in pubsub.listen():
                if message and message.get("type") == "message":
                    raw_data = message.get("data")
                    channel = message.get("channel")
                    try:
                        payload = json.loads(raw_data)
                    except Exception:
                        payload = {"data": raw_data}

                    if isinstance(payload, dict) and "channel" not in payload:
                        payload["channel"] = channel

                    await websocket.send_json(payload)
        except Exception:
            pass

    forward_task = asyncio.create_task(forward_pubsub_messages())

    try:
        # Await incoming client frames to promptly detect disconnections
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        # Handle disconnect cleanly: cancel forward task, unsubscribe, and close connections
        forward_task.cancel()
        try:
            await forward_task
        except asyncio.CancelledError:
            pass
        try:
            # Redis Command: UNSUBSCRIBE unit:updates incident:updates
            await pubsub.unsubscribe("unit:updates", "incident:updates")
            await pubsub.aclose()
            await async_redis.aclose()
        except Exception:
            pass
