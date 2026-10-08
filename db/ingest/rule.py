from sqlalchemy import Boolean, CheckConstraint, Float, String
from sqlalchemy.orm import Mapped, mapped_column

from db.ingest.base import Base


class Rule(Base):
    """Alert rules. Seeded from the rules file at startup, then managed via the API."""

    __tablename__ = "rules"

    rule_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    type: Mapped[str] = mapped_column(String(32))
    band_min_mhz: Mapped[float] = mapped_column(Float)
    band_max_mhz: Mapped[float] = mapped_column(Float)
    threshold_dbm: Mapped[float] = mapped_column(Float)
    min_duration_s: Mapped[float] = mapped_column(Float)
    max_gap_s: Mapped[float] = mapped_column(Float, default=5)
    resolve_after_s: Mapped[float] = mapped_column(Float)
    severity: Mapped[str] = mapped_column(String(16))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        # Last line of defense: the API validates these too, but the DB never stores nonsense.
        CheckConstraint("band_min_mhz < band_max_mhz", name="ck_rule_band_ordered"),
        CheckConstraint("min_duration_s > 0", name="ck_rule_min_duration_positive"),
        CheckConstraint("severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
                        name="ck_rule_severity"),
    )
