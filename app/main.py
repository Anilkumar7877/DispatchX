import asyncio
from contextlib import asynccontextmanager
import json
import os
import time
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Body, Query, WebSocket, WebSocketDisconnect, status as http_status
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
    # Note on Fire-and-Forget: Pub/Sub delivers messages only to currently connected subscribers;
    # if a client is disconnected at the time of publish, it will not receive the notification later.
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
    # Redis Command: CONFIG SET notify-keyspace-events Ex
    # Why: Enables keyspace notifications specifically for expired events ('x') on keyspace ('E').
    redis_client.config_set("notify-keyspace-events", "Ex")

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
    description="Real-time ambulance dispatch system - Phases 1, 2, 3, & 4",
    version="4.0.0",
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


# ---------------------------------------------------------------------------
# 3. GET /dashboard - Serves Dashboard HTML
# ---------------------------------------------------------------------------
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard():
    # Attempt to locate dashboard.html in app root or workspace root
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
        "phase": 4,
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
    # Note on Fire-and-Forget: Pub/Sub does not persist messages; subscribers must be connected to receive.
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
# POST /units/{id}/heartbeat
# ---------------------------------------------------------------------------
@app.post("/units/{unit_id}/heartbeat")
def send_heartbeat(unit_id: str):
    heartbeat_key = f"hb:unit:{unit_id}"

    # Redis Command: SET hb:unit:{id} 1 EX 10
    # Why: Sets ephemeral key with 10-second TTL to maintain online liveness.
    redis_client.set(heartbeat_key, 1, ex=10)

    return {
        "unit_id": unit_id,
        "status": "ok",
        "message": "Heartbeat registered",
        "ttl": 10,
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
# 1. POST /units/{id}/location (GEOADD and PUBLISH unit:updates)
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
    # Note on Fire-and-Forget: If no WebSocket is connected at this instant, the message is dropped.
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
# PHASE 3 & PHASE 4: ZONES, AVAILABILITY, & INCIDENTS (WITH PUBLISHING)
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
# POST /incidents (Phase 3 & 4: ZADD and PUBLISH incident:updates)
# ---------------------------------------------------------------------------
@app.post("/incidents", status_code=http_status.HTTP_201_CREATED)
def create_incident(incident: IncidentCreateRequest):
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
        },
    )

    # Redis Command: ZADD incidents:queue <severity> incident:{id}
    # Why Sorted Set: Forms a priority queue ordered by severity score.
    redis_client.zadd("incidents:queue", {incident_key: incident.severity})

    # Redis Command: PUBLISH incident:updates <json>
    # Why Pub/Sub: Real-time notification of newly created emergency incidents.
    # Note on Fire-and-Forget: Pub/Sub does not persist messages; subscribers must be connected to receive.
    incident_created_event = {
        "event": "created",
        "id": str(incident_id),
        "incident_id": str(incident_id),
        "type": incident.type,
        "lat": incident.lat,
        "lon": incident.lon,
        "severity": incident.severity,
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
    # Note on Fire-and-Forget: Pub/Sub does not hold a buffer. Disconnected clients miss messages
    # published while offline; they receive only live messages published after subscribing.
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
        except Exception as e:
            print("Error in forward_pubsub_messages:", e)

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
