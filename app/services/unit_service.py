from datetime import datetime
import json
import time
from typing import List, Dict, Any, Optional

try:
    from app.redis_client import redis_client
    from app.schemas import UnitRegisterRequest, UnitStatusUpdateRequest
except ImportError:
    from dispatchx.app.redis_client import redis_client
    from dispatchx.app.schemas import UnitRegisterRequest, UnitStatusUpdateRequest


def register_unit_service(unit: UnitRegisterRequest) -> Dict[str, Any]:
    unit_key = f"unit:{unit.id}"

    # Redis Command: HSET unit:{id} mapping
    redis_client.hset(
        name=unit_key,
        mapping={
            "type": unit.type,
            "status": unit.status,
            "driver": unit.driver,
        },
    )

    # Redis Command: SADD units:free {id} (or units:busy)
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


def get_nearest_units_service(
    lat: float,
    lon: float,
    radius_km: float = 5.0,
    count: int = 5,
) -> List[Dict[str, Any]]:
    # Redis Command: GEOSEARCH units:geo FROMLONLAT <lon> <lat> BYRADIUS <radius> km ASC COUNT <count> WITHDIST
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
    for member, distance in raw_results:
        unit_key = member if member.startswith("unit:") else f"unit:{member}"
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


def get_unit_by_id_service(unit_id: str) -> Optional[Dict[str, Any]]:
    unit_key = f"unit:{unit_id}"
    unit_data = redis_client.hgetall(unit_key)
    return unit_data if unit_data else None


def update_unit_status_service(unit_id: str, payload: UnitStatusUpdateRequest) -> Optional[Dict[str, Any]]:
    unit_key = f"unit:{unit_id}"
    if not redis_client.exists(unit_key):
        return None

    redis_client.hset(unit_key, "status", payload.status)

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

    pos = redis_client.geopos("units:geo", f"unit:{unit_id}")
    unit_lon, unit_lat = pos[0] if (pos and pos[0] is not None) else (None, None)

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


def record_unit_heartbeat_service(unit_id: str) -> Dict[str, Any]:
    heartbeat_key = f"hb:unit:{unit_id}"
    redis_client.set(heartbeat_key, 1, ex=10)

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


def get_unit_uptime_service(unit_id: str, date: str) -> Dict[str, Any]:
    bitmap_key = f"unit:{unit_id}:online:{date}"
    hours_online = redis_client.bitcount(bitmap_key)

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


def get_unit_online_status_service(unit_id: str) -> Dict[str, Any]:
    heartbeat_key = f"hb:unit:{unit_id}"
    remaining_ttl = redis_client.ttl(heartbeat_key)
    is_online = remaining_ttl > 0

    return {
        "online": is_online,
        "ttl": remaining_ttl if is_online else 0,
    }


def update_unit_location_service(unit_id: str, latitude: float, longitude: float) -> Dict[str, Any]:
    member = f"unit:{unit_id}" if not unit_id.startswith("unit:") else unit_id
    redis_client.geoadd("units:geo", (longitude, latitude, member))

    current_status = redis_client.hget(f"unit:{unit_id}", "status") or "free"

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


def delete_unit_location_service(unit_id: str) -> Dict[str, Any]:
    member = f"unit:{unit_id}" if not unit_id.startswith("unit:") else unit_id
    removed_count = redis_client.zrem("units:geo", member)

    return {
        "id": unit_id,
        "removed": bool(removed_count),
        "message": "Unit location removed from geo index" if removed_count else "Unit location was not found in geo index",
    }

