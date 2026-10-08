from typing import Literal

from pydantic import BaseModel


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    checks: dict[str, bool]  # e.g. {"database": true, "kafka": false}
