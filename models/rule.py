from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from models.enums import Severity
from models.types import Number, StrictBool, StrictStr

RuleType = Literal["power_threshold"]  # multi_sensor (optional in the spec) is not implemented


class Band(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_mhz: Number = Field(ge=1, le=6000)
    max_mhz: Number = Field(ge=1, le=6000)

    @model_validator(mode="after")
    def ordered(self):
        if self.min_mhz >= self.max_mhz:
            raise ValueError("band.min_mhz must be lower than band.max_mhz")
        return self


class RuleIn(BaseModel):
    """A rule as written in the rules file or sent to POST /api/v1/rules.

    power_threshold: an observation inside `band` with power above `threshold_dbm`.
    The condition must persist for at least `min_duration_s` before an alert opens,
    so a short burst never opens one."""

    model_config = ConfigDict(extra="forbid")

    rule_id: StrictStr = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_.-]+$")
    name: StrictStr = Field(min_length=1, max_length=200)
    type: RuleType
    band: Band
    threshold_dbm: Number = Field(ge=-150, le=30)
    # > 0: with 0 a single reading (a burst) would open an alert.
    min_duration_s: Number = Field(gt=0)
    # Our addition to the spec: max silence between two matching observations that
    # still counts as "persisting". Optional, defaults to 5 s.
    max_gap_s: Number = Field(default=5, gt=0)
    resolve_after_s: Number = Field(gt=0)
    severity: Severity
    enabled: StrictBool = True


class RuleUpdate(BaseModel):
    """Partial update: send only the fields to change, e.g. {"enabled": false}.
    rule_id and type cannot be changed (extra fields are rejected)."""

    model_config = ConfigDict(extra="forbid")

    name: StrictStr | None = Field(default=None, min_length=1, max_length=200)
    band: Band | None = None
    threshold_dbm: Number | None = Field(default=None, ge=-150, le=30)
    min_duration_s: Number | None = Field(default=None, gt=0)
    max_gap_s: Number | None = Field(default=None, gt=0)
    resolve_after_s: Number | None = Field(default=None, gt=0)
    severity: Severity | None = None
    enabled: StrictBool | None = None

    @model_validator(mode="after")
    def no_explicit_nulls(self):
        # Omitting a field means "keep it"; sending null for it is a client error.
        nulls = [f for f in self.model_fields_set if getattr(self, f) is None]
        if nulls:
            raise ValueError(f"fields cannot be null: {', '.join(sorted(nulls))}")
        return self


class RuleOut(RuleIn):
    """Built straight from a DB row: RuleOut.model_validate(row)."""

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="before")
    @classmethod
    def nest_band(cls, data: Any) -> Any:
        # The DB row stores band_min_mhz/band_max_mhz flat; the API nests them under "band".
        if hasattr(data, "band_min_mhz") and not isinstance(data, dict):
            fields = ("rule_id", "name", "type", "threshold_dbm", "min_duration_s",
                      "max_gap_s", "resolve_after_s", "severity", "enabled")
            return {k: getattr(data, k) for k in fields} | {
                "band": {"min_mhz": data.band_min_mhz, "max_mhz": data.band_max_mhz}}
        return data
