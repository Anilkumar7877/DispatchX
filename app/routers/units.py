from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query, status as http_status

try:
    from app.schemas import (
        UnitRegisterRequest,
        UnitStatusUpdateRequest,
        UnitLocationUpdateRequest,
    )
    from app.services import unit_service
except ImportError:
    from dispatchx.app.schemas import (
        UnitRegisterRequest,
        UnitStatusUpdateRequest,
        UnitLocationUpdateRequest,
    )
    from dispatchx.app.services import unit_service

router = APIRouter(prefix="/units", tags=["Units"])


@router.post("", status_code=http_status.HTTP_201_CREATED)
def register_unit(unit: UnitRegisterRequest):
    return unit_service.register_unit_service(unit)


@router.get("/nearest")
def get_nearest_units(
    lat: float = Query(..., description="Search origin latitude"),
    lon: float = Query(..., description="Search origin longitude"),
    radius_km: float = Query(default=5.0, description="Search radius in kilometers"),
    count: int = Query(default=5, description="Maximum number of units to return"),
):
    return unit_service.get_nearest_units_service(lat=lat, lon=lon, radius_km=radius_km, count=count)


@router.get("/{unit_id}")
def get_unit(unit_id: str):
    unit_data = unit_service.get_unit_by_id_service(unit_id)
    if not unit_data:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"Unit '{unit_id}' not found",
        )
    return unit_data


@router.patch("/{unit_id}/status")
def update_unit_status(unit_id: str, payload: UnitStatusUpdateRequest):
    updated = unit_service.update_unit_status_service(unit_id, payload)
    if updated is None:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"Unit '{unit_id}' not found",
        )
    return updated


@router.post("/{unit_id}/heartbeat")
def send_heartbeat(unit_id: str):
    return unit_service.record_unit_heartbeat_service(unit_id)


@router.get("/{unit_id}/uptime")
def get_unit_uptime(
    unit_id: str,
    date: str = Query(..., description="Date formatted as YYYY-MM-DD"),
):
    return unit_service.get_unit_uptime_service(unit_id, date)


@router.get("/{unit_id}/online")
def get_unit_online_status(unit_id: str):
    return unit_service.get_unit_online_status_service(unit_id)


@router.post("/{unit_id}/location")
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

    return unit_service.update_unit_location_service(unit_id, latitude, longitude)


@router.delete("/{unit_id}/location")
def delete_unit_location(unit_id: str):
    return unit_service.delete_unit_location_service(unit_id)

