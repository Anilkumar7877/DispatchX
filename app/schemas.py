from typing import Optional
from pydantic import BaseModel, Field


class UnitRegisterRequest(BaseModel):
    id: str = Field(..., description="Unique identifier for the ambulance unit")
    type: str = Field(..., description="Ambulance unit type, e.g., ALS, BLS")
    status: str = Field(default="free", description="Current status of the unit (default: 'free')")
    driver: str = Field(..., description="Assigned driver name")


class UnitStatusUpdateRequest(BaseModel):
    status: str = Field(..., description="New status to set for the unit")


class UnitLocationUpdateRequest(BaseModel):
    lat: Optional[float] = Field(None, description="Latitude of the unit (-90 to 90)")
    lon: Optional[float] = Field(None, description="Longitude of the unit (-180 to 180)")


class IncidentCreateRequest(BaseModel):
    type: str = Field(..., description="Type of incident, e.g., Cardiac Arrest, Traffic Accident")
    lat: float = Field(..., description="Latitude of the incident")
    lon: float = Field(..., description="Longitude of the incident")
    severity: int = Field(..., ge=1, le=10, description="Severity rating from 1 (minor) to 10 (critical)")
    zone: Optional[str] = Field(default="central", description="Geographic zone of the incident (default: 'central')")


class ManualDispatchRequest(BaseModel):
    incident_id: str = Field(..., description="ID of the emergency incident to assign")
    unit_id: str = Field(..., description="ID of the ambulance unit to dispatch")

