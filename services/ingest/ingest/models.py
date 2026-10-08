"""ingest_db tables. Owned only by the ingest service."""

from sqlalchemy import BigInteger, Boolean, Float, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from rfam_common.db import UTCDateTime, utcnow

# BIGINT on Postgres, INTEGER on SQLite (so autoincrement works in tests)
BigId = BigInteger().with_variant(Integer, "sqlite")


class Base(DeclarativeBase):
    pass


class Observation(Base):
    __tablename__ = "observations"

    id: Mapped[int] = mapped_column(BigId, primary_key=True, autoincrement=True)
    sensor_id: Mapped[str] = mapped_column(String(64))
    timestamp = mapped_column(UTCDateTime, nullable=False)  # event time
    received_at = mapped_column(UTCDateTime, nullable=False, default=utcnow)  # arrival time
    frequency_mhz: Mapped[float] = mapped_column(Float)
    bandwidth_khz: Mapped[float] = mapped_column(Float)
    power_dbm: Mapped[float] = mapped_column(Float)
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)

    __table_args__ = (
        Index("ix_obs_sensor_ts", "sensor_id", "timestamp"),
        Index("ix_obs_ts", "timestamp"),
        Index("ix_obs_freq", "frequency_mhz"),
    )


class Sensor(Base):
    __tablename__ = "sensors"

    sensor_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_seen = mapped_column(UTCDateTime, nullable=False)  # only moves forward
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)


class Rule(Base):
    __tablename__ = "rules"

    rule_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    type: Mapped[str] = mapped_column(String(32))
    band_min_mhz: Mapped[float] = mapped_column(Float)
    band_max_mhz: Mapped[float] = mapped_column(Float)
    threshold_dbm: Mapped[float] = mapped_column(Float)
    min_duration_s: Mapped[float] = mapped_column(Float)
    max_gap_s: Mapped[float] = mapped_column(Float)
    resolve_after_s: Mapped[float] = mapped_column(Float)
    severity: Mapped[str] = mapped_column(String(16))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
