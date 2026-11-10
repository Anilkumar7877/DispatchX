from datetime import datetime, timezone, timedelta
import asyncio
from contextlib import asynccontextmanager
import json
import os
import time
from typing import Optional, List, Dict, Any
import uuid

from fastapi import FastAPI, HTTPException, Body, Query, Header, Response, WebSocket, WebSocketDisconnect, status as http_status
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel, Field
import redis.asyncio as aioredis
from redis.commands.search.field import TagField, NumericField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query as SearchQuery
from redis.commands.search.aggregation import AggregateRequest
from redis.commands.search import reducers

try:
    from app.config import settings
    from app.redis_client import redis_client
    from app.schemas import (
        UnitRegisterRequest,
        UnitStatusUpdateRequest,
        UnitLocationUpdateRequest,
        IncidentCreateRequest,
        ManualDispatchRequest,
    )
    from app.services.lifecycle_service import (
        handle_unit_expired,
        listen_to_keyspace_expirations,
        init_redis_infrastructure,
    )
    from app.routers import units, zones, incidents, dispatch, analytics, ws
except ImportError:
    from dispatchx.app.config import settings
    from dispatchx.app.redis_client import redis_client
    from dispatchx.app.schemas import (
        UnitRegisterRequest,
        UnitStatusUpdateRequest,
        UnitLocationUpdateRequest,
        IncidentCreateRequest,
        ManualDispatchRequest,
    )
    from dispatchx.app.services.lifecycle_service import (
        handle_unit_expired,
        listen_to_keyspace_expirations,
        init_redis_infrastructure,
    )
    from dispatchx.app.routers import units, zones, incidents, dispatch, analytics, ws


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize indexes and streams
    init_redis_infrastructure()

    # Start background task to listen for heartbeat expirations
    expiry_task = asyncio.create_task(listen_to_keyspace_expirations())

    yield

    # Shutdown
    expiry_task.cancel()
    try:
        await expiry_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title=settings.PROJECT_NAME,
    description=settings.DESCRIPTION,
    version=settings.VERSION,
    lifespan=lifespan,
)

# Include modular feature routers
app.include_router(units.router)
app.include_router(zones.router)
app.include_router(incidents.router)
app.include_router(dispatch.router)
app.include_router(analytics.router)
app.include_router(ws.router)


# ---------------------------------------------------------------------------
# GET /dashboard - Serves Leaflet Dashboard HTML
# ---------------------------------------------------------------------------
@app.get("/dashboard", response_class=HTMLResponse, tags=["Dashboard"])
def get_dashboard():
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


@app.get("/", tags=["Root"])
def get_root():
    return {
        "service": settings.PROJECT_NAME,
        "description": settings.DESCRIPTION,
        "phases_implemented": [1, 2, 3, 4, 5],
        "dashboard_url": "/dashboard",
        "websocket_url": "/ws",
    }
