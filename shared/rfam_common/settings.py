"""Configuration comes from environment variables only (12-factor). No secrets in code.

Only what both services read lives here; each service extends it with its own settings.
"""

import os
from dataclasses import dataclass, field


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class BaseSettings:
    database_url: str = field(default_factory=lambda: env("DATABASE_URL", "sqlite:///./local.db"))
    kafka_bootstrap: str = field(default_factory=lambda: env("KAFKA_BOOTSTRAP", "localhost:9092"))
    log_level: str = field(default_factory=lambda: env("LOG_LEVEL", "INFO"))
