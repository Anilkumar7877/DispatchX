from typing import List
from fastapi import APIRouter

try:
    from app.services import zone_service
except ImportError:
    from dispatchx.app.services import zone_service

router = APIRouter(prefix="/zones", tags=["Zones"])


@router.post("/{zone}/units/{unit_id}")
def add_unit_to_zone(zone: str, unit_id: str):
    zone_service.add_unit_to_zone_service(zone, unit_id)
    return {
        "zone": zone,
        "unit_id": unit_id,
        "message": f"Unit '{unit_id}' added to zone '{zone}'",
    }


@router.delete("/{zone}/units/{unit_id}")
def remove_unit_from_zone(zone: str, unit_id: str):
    removed = zone_service.remove_unit_from_zone_service(zone, unit_id)
    return {
        "zone": zone,
        "unit_id": unit_id,
        "removed": removed,
        "message": f"Unit '{unit_id}' removed from zone '{zone}'" if removed else f"Unit '{unit_id}' was not found in zone '{zone}'",
    }


@router.get("/{zone}/units/free", response_model=List[str])
def get_free_units_in_zone(zone: str):
    return zone_service.get_free_units_in_zone_service(zone)


@router.get("/{zone}/units", response_model=List[str])
def list_units_in_zone(zone: str):
    return zone_service.list_units_in_zone_service(zone)

