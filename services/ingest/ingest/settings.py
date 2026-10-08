from dataclasses import dataclass, field
from pathlib import Path

from rfam_common.settings import BaseSettings, env

# services/ingest/config/rules.yaml locally, /app/config/rules.yaml in the image
DEFAULT_RULES_FILE = str(Path(__file__).resolve().parents[1] / "config" / "rules.yaml")


@dataclass(frozen=True)
class IngestSettings(BaseSettings):
    # YAML (.yaml/.yml) or JSON (.json); only seeds the DB, the API manages rules after that.
    rules_file: str = field(default_factory=lambda: env("RULES_FILE", DEFAULT_RULES_FILE))
    # Two observations whose frequencies round to the same bucket are "the same signal".
    signal_tolerance_mhz: float = field(
        default_factory=lambda: float(env("SIGNAL_TOLERANCE_MHZ", "0.1")))
    # Observations older than (newest event time seen - MAX_LATENESS_S) are stored
    # but do not drive rule evaluation.
    max_lateness_s: float = field(default_factory=lambda: float(env("MAX_LATENESS_S", "300")))
    max_batch_size: int = field(default_factory=lambda: int(env("MAX_BATCH_SIZE", "1000")))
