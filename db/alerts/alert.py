from datetime import datetime

from sqlalchemy import JSON, CheckConstraint, Float, Index, Integer, String, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.alerts.base import Base
from rfam_common.db import UTCDateTime, utcnow

ACTIVE_STATES = ("OPEN", "ACKNOWLEDGED")
_ACTIVE = text("state IN ('OPEN', 'ACKNOWLEDGED')")


class Alert(Base):
    """One alert per (rule, signal) episode. Lifecycle: OPEN -> ACKNOWLEDGED -> RESOLVED."""

    __tablename__ = "alerts"

    alert_id: Mapped[str] = mapped_column(String(32), primary_key=True)  # alrt-xxxxxxxx
    rule_id: Mapped[str] = mapped_column(String(100))
    # Copied from the Kafka message: the alert service never reads the rules table.
    rule_name: Mapped[str] = mapped_column(String(200))
    severity: Mapped[str] = mapped_column(String(16))
    resolve_after_s: Mapped[float] = mapped_column(Float)
    signal_key: Mapped[str] = mapped_column(String(32))
    frequency_mhz: Mapped[float] = mapped_column(Float)
    state: Mapped[str] = mapped_column(String(16), default="OPEN")
    sensor_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    first_seen: Mapped[datetime] = mapped_column(UTCDateTime)
    last_seen: Mapped[datetime] = mapped_column(UTCDateTime)
    occurrences: Mapped[int] = mapped_column(Integer, default=0)
    peak_power_dbm: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime, default=utcnow)
    acknowledged_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    resolved_by: Mapped[str | None] = mapped_column(String(16))  # auto | manual

    observations: Mapped[list["AlertObservation"]] = relationship(  # noqa: F821
        back_populates="alert", cascade="all, delete-orphan",
        order_by="AlertObservation.timestamp")

    __table_args__ = (
        # At most ONE active alert per (rule, signal): the DB enforces deduplication,
        # even if two messages for the same signal are handled at the same time.
        Index("uq_alert_active_signal", "rule_id", "signal_key", unique=True,
              postgresql_where=_ACTIVE, sqlite_where=_ACTIVE),
        Index("ix_alert_state", "state"),
        Index("ix_alert_freq", "frequency_mhz"),
        CheckConstraint("state IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')", name="ck_alert_state"),
        CheckConstraint("resolved_by IS NULL OR resolved_by IN ('auto', 'manual')",
                        name="ck_alert_resolved_by"),
        CheckConstraint("severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
                        name="ck_alert_severity"),
    )
