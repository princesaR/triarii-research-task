from pydantic import BaseModel, ConfigDict, Field

from models.types import Number


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lat: Number = Field(ge=-90, le=90)
    lon: Number = Field(ge=-180, le=180)
