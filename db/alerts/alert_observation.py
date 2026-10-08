from datetime import datetime

from sqlalchemy import BigInteger, Float, ForeignKey, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.alerts.base import Base
from db.types import BigId
from rfam_common.db import UTCDateTime


class AlertObservation(Base):
    """The observations that contributed to an alert.

    A copy of the matched fields from the Kafka message, not a foreign key into
    ingest_db: each service owns its own database."""

    __tablename__ = "alert_observations"

    id: Mapped[int] = mapped_column(BigId, primary_key=True, autoincrement=True)
    alert_id: Mapped[str] = mapped_column(ForeignKey("alerts.alert_id", ondelete="CASCADE"))
    observation_id: Mapped[int] = mapped_column(BigInteger)  # id in ingest_db.observations
    sensor_id: Mapped[str] = mapped_column(String(64))
    timestamp: Mapped[datetime] = mapped_column(UTCDateTime)
    frequency_mhz: Mapped[float] = mapped_column(Float)
    power_dbm: Mapped[float] = mapped_column(Float)

    alert: Mapped["Alert"] = relationship(back_populates="observations")  # noqa: F821

    __table_args__ = (
        # Kafka delivers at-least-once: a redelivered message hits this and is skipped.
        UniqueConstraint("alert_id", "observation_id", name="uq_alert_observation"),
        Index("ix_alert_obs_sensor", "sensor_id"),  # GET /alerts?sensor_id=...
    )
