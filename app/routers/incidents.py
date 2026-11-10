import json
from typing import Optional
from fastapi import APIRouter, HTTPException, Header, Response, status as http_status

try:
    from app.schemas import IncidentCreateRequest
    from app.services import incident_service
except ImportError:
    from dispatchx.app.schemas import IncidentCreateRequest
    from dispatchx.app.services import incident_service

router = APIRouter(prefix="/incidents", tags=["Incidents"])


@router.post("", status_code=http_status.HTTP_201_CREATED)
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

    # Rate limiting check
    is_allowed, current_count, retry_after = incident_service.check_rate_limit(x_source)
    if not is_allowed:
        return Response(
            content=json.dumps({
                "detail": "Rate limit exceeded. Maximum 5 requests per minute allowed.",
                "source": x_source,
                "current_count": current_count,
                "retry_after": retry_after,
            }),
            status_code=http_status.HTTP_429_TOO_MANY_REQUESTS,
            media_type="application/json",
            headers={"Retry-After": str(retry_after)},
        )

    return incident_service.create_incident_service(incident, x_source)


@router.get("/next")
def get_next_incident():
    incident_data = incident_service.pop_next_incident_service()
    if not incident_data:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail="Incident queue is empty",
        )
    return incident_data


@router.get("/queue")
def get_incidents_queue():
    return incident_service.get_incidents_queue_service()

