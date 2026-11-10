from datetime import datetime, timedelta
import json
from typing import List, Dict, Any, Optional
import uuid

from redis.commands.search.query import Query as SearchQuery
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search import reducers

try:
    from app.redis_client import redis_client
except ImportError:
    from dispatchx.app.redis_client import redis_client


def get_daily_stats_service(date: str) -> Dict[str, Any]:
    callers_key = f"callers:{date}"
    areas_key = f"areas:{date}"

    unique_callers = redis_client.pfcount(callers_key)
    unique_areas = redis_client.pfcount(areas_key)

    return {
        "date": date,
        "unique_callers": unique_callers,
        "unique_areas": unique_areas,
    }


def get_range_stats_service(from_date: str, to_date: str) -> Dict[str, Any]:
    start_dt = datetime.strptime(from_date, "%Y-%m-%d")
    end_dt = datetime.strptime(to_date, "%Y-%m-%d")

    day_keys: List[str] = []
    curr = start_dt
    while curr <= end_dt:
        day_keys.append(f"callers:{curr.strftime('%Y-%m-%d')}")
        curr += timedelta(days=1)

    existing_keys = [k for k in day_keys if redis_client.exists(k)]

    if not existing_keys:
        return {
            "from": from_date,
            "to": to_date,
            "days_checked": len(day_keys),
            "unique_callers": 0,
        }

    temp_key = f"temp:callers:range:{uuid.uuid4()}"
    try:
        redis_client.pfmerge(temp_key, *existing_keys)
        unique_callers = redis_client.pfcount(temp_key)
    finally:
        redis_client.delete(temp_key)

    return {
        "from": from_date,
        "to": to_date,
        "days_checked": len(day_keys),
        "days_found": len(existing_keys),
        "unique_callers": unique_callers,
    }


def get_units_overlap_service(unit_ids: List[str], date: str) -> Dict[str, Any]:
    bitmap_keys = [f"unit:{uid}:online:{date}" for uid in unit_ids]
    temp_key = f"temp:overlap:{uuid.uuid4()}"
    try:
        redis_client.bitop("AND", temp_key, *bitmap_keys)
        overlap_hours_count = redis_client.bitcount(temp_key)

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


def get_stream_stats_service() -> Dict[str, Any]:
    try:
        stream_length = redis_client.xlen("incidents:stream")
    except Exception:
        stream_length = 0

    try:
        pending_summary = redis_client.xpending("incidents:stream", "dispatchers")
    except Exception:
        pending_summary = {"pending": 0, "min": None, "max": None, "consumers": []}

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


def search_incident_history_service(
    type: Optional[str] = None,
    zone: Optional[str] = None,
    min_severity: Optional[int] = None,
    from_time: Optional[int] = None,
    to_time: Optional[int] = None,
    limit: int = 10,
) -> Dict[str, Any]:
    query_parts: List[str] = []

    if type:
        clean_type = type.replace(" ", "\\ ")
        query_parts.append(f"@type:{{{clean_type}}}")

    if zone:
        clean_zone = zone.replace(" ", "\\ ")
        query_parts.append(f"@zone:{{{clean_zone}}}")

    if min_severity is not None:
        query_parts.append(f"@severity:[{min_severity} +inf]")

    if from_time is not None or to_time is not None:
        start_bound = from_time if from_time is not None else "-inf"
        end_bound = to_time if to_time is not None else "+inf"
        query_parts.append(f"@closed_at:[{start_bound} {end_bound}]")

    raw_query_string = " ".join(query_parts) if query_parts else "*"

    search_query = (
        SearchQuery(raw_query_string)
        .sort_by("closed_at", asc=False)
        .paging(0, limit)
    )

    search_result = redis_client.ft("idx:incidents").search(search_query)

    formatted_matches = []
    for doc in search_result.docs:
        doc_data = json.loads(doc.json) if hasattr(doc, "json") and doc.json else {}
        doc_data["id"] = doc.id
        formatted_matches.append(doc_data)

    return {
        "query": raw_query_string,
        "total": search_result.total,
        "count": len(formatted_matches),
        "results": formatted_matches,
    }


def get_history_stats_by_zone_service() -> Dict[str, Any]:
    agg_request = (
        AggregateRequest("*")
        .group_by(
            "@zone",
            reducers.count().alias("incident_count"),
            reducers.avg("@response_secs").alias("avg_response_secs"),
            reducers.max("@response_secs").alias("max_response_secs"),
        )
    )

    agg_result = redis_client.ft("idx:incidents").aggregate(agg_request)

    zone_stats = []
    for row in agg_result.rows:
        row_dict = {}
        for i in range(0, len(row), 2):
            key = row[i]
            val = row[i + 1]
            row_dict[key] = val

        try:
            zone_stats.append({
                "zone": row_dict.get("zone", "unknown"),
                "incident_count": int(row_dict.get("incident_count", 0)),
                "avg_response_secs": round(float(row_dict.get("avg_response_secs", 0.0)), 2),
                "max_response_secs": int(float(row_dict.get("max_response_secs", 0.0))),
            })
        except Exception:
            zone_stats.append(row_dict)

    return {
        "index": "idx:incidents",
        "zones_reported": len(zone_stats),
        "stats": zone_stats,
    }

