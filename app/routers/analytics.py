from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query, status as http_status

try:
    from app.services import analytics_service
except ImportError:
    from dispatchx.app.services import analytics_service

router = APIRouter(tags=["Analytics & History"])


@router.get("/stats/daily")
def get_daily_stats(date: str = Query(..., description="Date formatted as YYYY-MM-DD")):
    return analytics_service.get_daily_stats_service(date)


@router.get("/stats/range")
def get_range_stats(
    from_date: str = Query(..., alias="from", description="Start date (YYYY-MM-DD)"),
    to_date: str = Query(..., alias="to", description="End date (YYYY-MM-DD)"),
):
    try:
        return analytics_service.get_range_stats_service(from_date, to_date)
    except ValueError:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail="Dates must be in format YYYY-MM-DD",
        )


@router.get("/stats/units-overlap")
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
    return analytics_service.get_units_overlap_service(unit_ids, date)


@router.get("/stream/stats")
def get_stream_stats():
    return analytics_service.get_stream_stats_service()


@router.get("/history/search")
def search_incident_history(
    type: Optional[str] = Query(None, description="Filter by incident type (TAG field)"),
    zone: Optional[str] = Query(None, description="Filter by geographic zone (TAG field)"),
    min_severity: Optional[int] = Query(None, ge=1, le=10, description="Minimum severity rating"),
    from_time: Optional[int] = Query(None, alias="from", description="Earliest closed_at (unix epoch seconds)"),
    to_time: Optional[int] = Query(None, alias="to", description="Latest closed_at (unix epoch seconds)"),
    limit: int = Query(default=10, ge=1, le=1000, description="Maximum results to return"),
):
    try:
        return analytics_service.search_incident_history_service(
            type=type,
            zone=zone,
            min_severity=min_severity,
            from_time=from_time,
            to_time=to_time,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail=f"RediSearch query execution failed: {exc}",
        )


@router.get("/history/stats/by-zone")
def get_history_stats_by_zone():
    try:
        return analytics_service.get_history_stats_by_zone_service()
    except Exception as exc:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail=f"RediSearch aggregate execution failed: {exc}",
        )

