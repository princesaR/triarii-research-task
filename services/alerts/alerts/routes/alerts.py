from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from alerts import lifecycle
from alerts.deps import CtxDep, SessionDep
from db.alerts import Alert, AlertObservation
from models import AlertDetail, AlertOut, AlertState, Severity
from rfam_common.db import utcnow

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


@router.get("")
def list_alerts(
    s: SessionDep,
    state: AlertState | None = None,
    severity: Severity | None = None,
    sensor_id: str | None = None,
    min_freq_mhz: float | None = Query(None, ge=1, le=6000),
    max_freq_mhz: float | None = Query(None, ge=1, le=6000),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[AlertOut]:
    q = select(Alert)
    if state:
        q = q.where(Alert.state == state)
    if severity:
        q = q.where(Alert.severity == severity)
    if sensor_id:
        # sensor_ids is a JSON list; filter through the indexed observations table instead.
        q = q.where(Alert.alert_id.in_(select(AlertObservation.alert_id)
                                       .where(AlertObservation.sensor_id == sensor_id)))
    if min_freq_mhz is not None:
        q = q.where(Alert.frequency_mhz >= min_freq_mhz)
    if max_freq_mhz is not None:
        q = q.where(Alert.frequency_mhz <= max_freq_mhz)
    return s.scalars(q.order_by(Alert.last_seen.desc()).limit(limit).offset(offset)).all()


@router.get("/{alert_id}")
def get_alert(alert_id: str, s: SessionDep) -> AlertDetail:
    """The alert with its contributing observations (oldest first)."""
    return _get_or_404(s, alert_id)


@router.post("/{alert_id}/ack")
def acknowledge(alert_id: str, ctx: CtxDep, s: SessionDep) -> AlertOut:
    """OPEN -> ACKNOWLEDGED. No-op if already acknowledged; 409 if resolved.
    An acknowledged alert keeps being updated by new matches."""
    with ctx.lock:
        alert = _get_or_404(s, alert_id)
        try:
            lifecycle.acknowledge(alert, utcnow())
        except lifecycle.InvalidTransition as e:
            raise HTTPException(409, str(e)) from None
        s.commit()
    return alert


@router.post("/{alert_id}/resolve")
def resolve(alert_id: str, ctx: CtxDep, s: SessionDep) -> AlertOut:
    """Manual resolve. 409 if already resolved. A later match opens a new alert."""
    with ctx.lock:
        alert = _get_or_404(s, alert_id)
        try:
            lifecycle.resolve_manually(alert, utcnow())
        except lifecycle.InvalidTransition as e:
            raise HTTPException(409, str(e)) from None
        s.commit()
    ctx.metrics.resolved.labels("manual").inc()
    return alert


def _get_or_404(s: Session, alert_id: str) -> Alert:
    alert = s.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(404, f"alert {alert_id} not found")
    return alert
