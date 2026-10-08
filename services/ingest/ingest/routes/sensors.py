from fastapi import APIRouter
from sqlalchemy import select

from ingest.deps import SessionDep
from ingest.models import Sensor
from models import SensorOut

router = APIRouter(prefix="/api/v1/sensors", tags=["sensors"])


@router.get("")
def list_sensors(s: SessionDep) -> list[SensorOut]:
    return s.scalars(select(Sensor).order_by(Sensor.sensor_id)).all()
