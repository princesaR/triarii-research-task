import re
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from models.location import Location
from models.types import Number, StrictStr

ISO_8601 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")


class ObservationIn(BaseModel):
    """One sensor reading as submitted to POST /api/v1/observations."""

    model_config = ConfigDict(extra="forbid")

    sensor_id: StrictStr = Field(min_length=1, max_length=64)
    timestamp: datetime
    frequency_mhz: Number = Field(ge=1, le=6000)
    bandwidth_khz: Number = Field(gt=0)
    power_dbm: Number = Field(ge=-150, le=30)
    location: Location

    @field_validator("timestamp", mode="before")
    @classmethod
    def must_be_iso_string(cls, v: object) -> object:
        # Without this, pydantic would also accept Unix epochs like 1791281730.
        # A datetime object (DB row -> ObservationOut) is already parsed: let it through.
        if isinstance(v, datetime):
            return v
        if not isinstance(v, str) or not ISO_8601.match(v):
            raise ValueError("timestamp must be an ISO-8601 UTC string, e.g. 2026-10-06T10:15:30Z")
        return v

    @field_validator("timestamp")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        # Reject naive and non-UTC offsets: event time must be unambiguous across sensors.
        if v.tzinfo is None or v.utcoffset() != timedelta(0):
            raise ValueError("timestamp must be ISO-8601 UTC, e.g. 2026-10-06T10:15:30Z")
        return v


class ObservationOut(ObservationIn):
    """Built straight from a DB row: ObservationOut.model_validate(row)."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    received_at: datetime

    @model_validator(mode="before")
    @classmethod
    def nest_location(cls, data: Any) -> Any:
        # The DB row stores lat/lon flat; the API nests them under "location".
        if hasattr(data, "lat") and not isinstance(data, dict):
            fields = ("id", "sensor_id", "timestamp", "received_at",
                      "frequency_mhz", "bandwidth_khz", "power_dbm")
            return {k: getattr(data, k) for k in fields} | {
                "location": {"lat": data.lat, "lon": data.lon}}
        return data


class FieldError(BaseModel):
    field: str
    message: str


class RejectedItem(BaseModel):
    index: int  # position in the submitted batch
    errors: list[FieldError]


class BatchResult(BaseModel):
    """Batch policy: partial accept. 200 = all, 207 = some rejected, 422 = none accepted."""

    accepted: int
    rejected: list[RejectedItem]
