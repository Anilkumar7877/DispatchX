from fastapi import APIRouter, HTTPException, Query, status as http_status

try:
    from app.schemas import ManualDispatchRequest
    from app.services import incident_service
except ImportError:
    from dispatchx.app.schemas import ManualDispatchRequest
    from dispatchx.app.services import incident_service

router = APIRouter(prefix="/dispatch", tags=["Dispatch"])


@router.post("/auto")
def auto_dispatch_incident(
    radius_km: float = Query(default=10.0, description="Search radius around the emergency in km")
):
    result, error_msg, status_code = incident_service.auto_dispatch_incident_service(radius_km=radius_km)
    if error_msg:
        raise HTTPException(status_code=status_code, detail=error_msg)
    return result


@router.post("/manual")
def manual_dispatch_incident(assignment: ManualDispatchRequest):
    result, error_msg, status_code = incident_service.manual_dispatch_incident_service(assignment)
    if error_msg:
        raise HTTPException(status_code=status_code, detail=error_msg)
    return result


@router.post("/{incident_id}/resolve")
def resolve_incident(incident_id: str):
    result = incident_service.resolve_incident_service(incident_id)
    if not result:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"Incident '{incident_id}' not found",
        )
    return result


@router.get("/active")
def get_active_dispatches():
    return incident_service.get_active_dispatches_service()

