from dataclasses import dataclass, field

from rfam_common.settings import BaseSettings, env


@dataclass(frozen=True)
class AlertSettings(BaseSettings):
    kafka_group_id: str = field(default_factory=lambda: env("KAFKA_GROUP_ID", "alert-service"))
    # How often the sweeper looks for alerts to auto-resolve.
    sweep_interval_s: float = field(default_factory=lambda: float(env("SWEEP_INTERVAL_S", "5")))
