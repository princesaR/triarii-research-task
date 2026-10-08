from datetime import datetime

from sqlalchemy import Float, String
from sqlalchemy.orm import Mapped, mapped_column

from db.ingest.base import Base
from rfam_common.db import UTCDateTime


class Sensor(Base):
    """Sensors seen so far, upserted on every batch."""

    __tablename__ = "sensors"

    sensor_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    # Newest event time seen; only moves forward, so out-of-order data cannot rewind it.
    last_seen: Mapped[datetime] = mapped_column(UTCDateTime)
    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)
