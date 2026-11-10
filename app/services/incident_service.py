from datetime import datetime, timedelta
import json
import time
from typing import Optional, Dict, Any, List, Tuple
import uuid

try:
    from app.redis_client import redis_client
    from app.schemas import IncidentCreateRequest, ManualDispatchRequest
except ImportError:
    from dispatchx.app.redis_client import redis_client
    from dispatchx.app.schemas import IncidentCreateRequest, ManualDispatchRequest


def check_rate_limit(x_source: str) -> Tuple[bool, int, int]:
    """
    Checks rate limit for x_source (max 5 requests per minute).
    Returns (is_allowed, current_count, retry_after_sec).
    """
    now_dt = datetime.now()
    window_minute = now_dt.strftime("%Y-%m-%dT%H:%M")
    rate_limit_key = f"rl:{x_source}:{window_minute}"

    pipe = redis_client.pipeline(transaction=False)
    pipe.incr(rate_limit_key)
    pipe.expire(rate_limit_key, 60, nx=True)
    pipe_results = pipe.execute()
    current_request_count = pipe_results[0]

    if current_request_count > 5:
        ttl_remaining = redis_client.ttl(rate_limit_key)
        retry_after_sec = max(1, ttl_remaining if ttl_remaining > 0 else 60)
        return False, current_request_count, retry_after_sec

    return True, current_request_count, 0


def create_incident_service(incident: IncidentCreateRequest, x_source: str) -> Dict[str, Any]:
    now_dt = datetime.now()
    today_date = now_dt.strftime("%Y-%m-%d")
    incident_zone = incident.zone or "central"

    # Phase 7: HyperLogLog Cardinality Tracking
    redis_client.pfadd(f"callers:{today_date}", x_source)
    redis_client.pfadd(f"areas:{today_date}", incident_zone)

    # Incident Persistence (Phases 3, 4, 5)
    incident_id = redis_client.incr("incident:id")
    incident_key = f"incident:{incident_id}"

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

    # Priority queue
    redis_client.zadd("incidents:queue", {incident_key: incident.severity})

    # Durable Stream
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

    # Real-time WebSocket event
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


def pop_next_incident_service() -> Optional[Dict[str, Any]]:
    popped = redis_client.zpopmax("incidents:queue", count=1)
    if not popped:
        return None

    incident_key, score = popped[0]
    incident_data = redis_client.hgetall(incident_key)
    incident_id = incident_key.split("incident:", 1)[1] if incident_key.startswith("incident:") else incident_key

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


def get_incidents_queue_service() -> List[Dict[str, Any]]:
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


def auto_dispatch_incident_service(radius_km: float = 10.0) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    """
    Returns (result_dict, error_message, http_status_code).
    """
    popped = redis_client.zpopmax("incidents:queue", count=1)
    if not popped:
        return None, "No pending incidents in queue to dispatch", 404

    incident_key, score = popped[0]
    incident_data = redis_client.hgetall(incident_key)

    if not incident_data or "lat" not in incident_data or "lon" not in incident_data:
        return None, f"Corrupt or missing incident data for '{incident_key}'", 500

    incident_lat = float(incident_data["lat"])
    incident_lon = float(incident_data["lon"])
    incident_id = incident_data.get("id") or incident_key.split(":")[-1]

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

    for member, distance in nearby_units:
        raw_id = member.split("unit:", 1)[1] if member.startswith("unit:") else member
        is_free = redis_client.sismember("units:free", raw_id) == 1
        is_online = redis_client.exists(f"hb:unit:{raw_id}") == 1

        if is_free and is_online:
            selected_unit_id = raw_id
            selected_distance = float(distance)
            break

    if not selected_unit_id:
        redis_client.zadd("incidents:queue", {incident_key: score})
        return None, f"No online free ambulances within {radius_km} km of Incident #{incident_id}. Incident has been returned to queue.", 409

    unit_key = f"unit:{selected_unit_id}"
    dispatched_timestamp = time.time()

    redis_client.smove("units:free", "units:busy", selected_unit_id)

    redis_client.hset(
        unit_key,
        mapping={
            "status": "dispatched",
            "current_incident": incident_key,
        },
    )

    redis_client.hset(
        incident_key,
        mapping={
            "status": "dispatched",
            "assigned_unit": selected_unit_id,
            "dispatched_at": str(dispatched_timestamp),
        },
    )

    pos = redis_client.geopos("units:geo", f"unit:{selected_unit_id}")
    unit_lon, unit_lat = pos[0] if (pos and pos[0] is not None) else (None, None)
    unit_driver = redis_client.hget(unit_key, "driver") or "Unknown"
    unit_type = redis_client.hget(unit_key, "type") or "ALS"

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

    estimated_eta_min = round((selected_distance / 35.0) * 60.0, 1)

    result = {
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
    return result, None, 200


def manual_dispatch_incident_service(assignment: ManualDispatchRequest) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
    incident_key = f"incident:{assignment.incident_id}" if not assignment.incident_id.startswith("incident:") else assignment.incident_id
    unit_key = f"unit:{assignment.unit_id}" if not assignment.unit_id.startswith("unit:") else assignment.unit_id
    raw_unit_id = assignment.unit_id.split("unit:", 1)[1] if assignment.unit_id.startswith("unit:") else assignment.unit_id
    raw_incident_id = assignment.incident_id.split("incident:", 1)[1] if assignment.incident_id.startswith("incident:") else assignment.incident_id

    if not redis_client.exists(incident_key):
        return None, f"Incident '{assignment.incident_id}' not found", 404

    if not redis_client.exists(unit_key):
        return None, f"Unit '{assignment.unit_id}' not found", 404

    if not redis_client.sismember("units:free", raw_unit_id):
        return None, f"Unit '{assignment.unit_id}' is not free", 409

    redis_client.zrem("incidents:queue", incident_key)
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

    result = {
        "status": "dispatched",
        "message": f"Unit '{raw_unit_id}' manually dispatched to Incident #{raw_incident_id}",
        "incident_id": raw_incident_id,
        "unit_id": raw_unit_id,
    }
    return result, None, 200


def resolve_incident_service(incident_id: str) -> Optional[Dict[str, Any]]:
    incident_key = f"incident:{incident_id}" if not incident_id.startswith("incident:") else incident_id
    raw_incident_id = incident_id.split("incident:", 1)[1] if incident_id.startswith("incident:") else incident_id

    if not redis_client.exists(incident_key):
        return None

    assigned_unit_id = redis_client.hget(incident_key, "assigned_unit")
    resolved_timestamp = time.time()

    redis_client.hset(
        incident_key,
        mapping={
            "status": "resolved",
            "resolved_at": str(resolved_timestamp),
        },
    )

    if assigned_unit_id:
        unit_key = f"unit:{assigned_unit_id}"
        redis_client.hdel(unit_key, "current_incident")
        redis_client.hset(unit_key, "status", "free")
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


def get_active_dispatches_service() -> List[Dict[str, Any]]:
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

