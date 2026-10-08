from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from models.location import Location


class SensorOut(BaseModel):
    """Built straight from a DB row: SensorOut.model_validate(row)."""

    model_config = ConfigDict(from_attributes=True)

    sensor_id: str
    last_seen: datetime
    location: Location

    @model_validator(mode="before")
    @classmethod
    def nest_location(cls, data: Any) -> Any:
        # The DB row stores lat/lon flat; the API nests them under "location".
        if hasattr(data, "lat") and not isinstance(data, dict):
            return {"sensor_id": data.sensor_id, "last_seen": data.last_seen,
                    "location": {"lat": data.lat, "lon": data.lon}}
        return data
