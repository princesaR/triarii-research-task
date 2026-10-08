from datetime import datetime

from sqlalchemy import Float, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from db.ingest.base import Base
from db.types import BigId
from rfam_common.db import UTCDateTime, utcnow


class Observation(Base):
    """One sensor reading. Append-only: never updated after insert."""

    __tablename__ = "observations"

    id: Mapped[int] = mapped_column(BigId, primary_key=True, autoincrement=True)
    sensor_id: Mapped[str] = mapped_column(String(64))
    timestamp: Mapped[datetime] = mapped_column(UTCDateTime)  # event time (from the sensor)
    received_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)  # arrival time
    frequency_mhz: Mapped[float] = mapped_column(Float)
    bandwidth_khz: Mapped[float] = mapped_column(Float)
    power_dbm: Mapped[float] = mapped_column(Float)
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        # GET /observations filters: by sensor and time, by time range, by frequency range.
        Index("ix_obs_sensor_ts", "sensor_id", "timestamp"),
        Index("ix_obs_ts", "timestamp"),
        Index("ix_obs_freq", "frequency_mhz"),
    )
