import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query, Response
from sqlalchemy import select

from db.ingest import Observation
from ingest import pipeline
from ingest.deps import CtxDep, SessionDep
from models import BatchResult, ObservationOut
from rfam_common.kafka import PublishError

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/observations", tags=["observations"])


@router.post("", responses={207: {"model": BatchResult}, 422: {"model": BatchResult}})
def ingest_observations(ctx: CtxDep, response: Response,
                        payload: Any = Body(...)) -> BatchResult:
    """Accept one observation or an array of them.

    Partial accept: valid items are stored, invalid ones are reported by index.
    200 = all accepted, 207 = some rejected, 422 = none accepted, 413 = batch too large,
    503 = Kafka unavailable (nothing stored; safe to retry the whole request)."""
    items = payload if isinstance(payload, list) else [payload]
    if not items:
        raise HTTPException(422, "empty batch")
    if len(items) > ctx.settings.max_batch_size:
        raise HTTPException(413, f"batch larger than {ctx.settings.max_batch_size}")

    valid, rejected = pipeline.validate_batch(items)
    ctx.metrics.observations.labels("rejected").inc(len(rejected))
    if not valid:
        response.status_code = 422
        return BatchResult(accepted=0, rejected=rejected)

    with ctx.session_factory() as s, ctx.lock:
        rows = pipeline.store(s, valid)
        events = pipeline.evaluate(ctx.rule_engine, ctx.rules, rows)
        try:
            if events:  # no matches: no Kafka round-trip, and a Kafka outage cannot block it
                ctx.publisher.publish_batch(events)
        except PublishError:
            # Nothing is committed, so the sensor can retry the batch without duplicates.
            s.rollback()
            ctx.metrics.publish_errors.inc()
            log.exception("kafka publish failed, batch rolled back")
            raise HTTPException(503, "message broker unavailable, retry later") from None
        s.commit()

    for ev in events:
        ctx.metrics.matches.labels(ev.rule_id).inc()
    ctx.metrics.episodes.set(ctx.rule_engine.episode_count)
    ctx.metrics.observations.labels("accepted").inc(len(valid))
    response.status_code = 207 if rejected else 200
    return BatchResult(accepted=len(valid), rejected=rejected)


@router.get("")
def query_observations(
    s: SessionDep,
    sensor_id: str | None = None,
    min_freq_mhz: float | None = Query(None, ge=1, le=6000),
    max_freq_mhz: float | None = Query(None, ge=1, le=6000),
    start: datetime | None = Query(None, description="ISO-8601, inclusive"),
    end: datetime | None = Query(None, description="ISO-8601, exclusive"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> list[ObservationOut]:
    q = select(Observation)
    if sensor_id:
        q = q.where(Observation.sensor_id == sensor_id)
    if min_freq_mhz is not None:
        q = q.where(Observation.frequency_mhz >= min_freq_mhz)
    if max_freq_mhz is not None:
        q = q.where(Observation.frequency_mhz <= max_freq_mhz)
    if start:
        q = q.where(Observation.timestamp >= start)
    if end:
        q = q.where(Observation.timestamp < end)
    q = q.order_by(Observation.timestamp.desc(), Observation.id.desc()).limit(limit).offset(offset)
    return s.scalars(q).all()
