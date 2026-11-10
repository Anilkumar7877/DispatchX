from typing import List

try:
    from app.redis_client import redis_client
except ImportError:
    from dispatchx.app.redis_client import redis_client


def add_unit_to_zone_service(zone: str, unit_id: str) -> None:
    zone_key = f"zone:{zone}:units"
    redis_client.sadd(zone_key, unit_id)


def remove_unit_from_zone_service(zone: str, unit_id: str) -> bool:
    zone_key = f"zone:{zone}:units"
    removed_count = redis_client.srem(zone_key, unit_id)
    return bool(removed_count)


def get_free_units_in_zone_service(zone: str) -> List[str]:
    zone_key = f"zone:{zone}:units"
    free_units = redis_client.sinter(zone_key, "units:free")
    return sorted(list(free_units))


def list_units_in_zone_service(zone: str) -> List[str]:
    zone_key = f"zone:{zone}:units"
    members = redis_client.smembers(zone_key)
    return sorted(list(members))

