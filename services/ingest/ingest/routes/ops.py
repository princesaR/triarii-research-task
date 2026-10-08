from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from ingest.deps import CtxDep
from models import Health
from rfam_common.db import db_is_healthy

router = APIRouter(tags=["ops"])


@router.get("/health", responses={503: {"model": Health}})
def health(ctx: CtxDep, response: Response) -> Health:
    checks = {"database": db_is_healthy(ctx.db_engine), "kafka": ctx.publisher.ping()}
    ok = all(checks.values())
    response.status_code = 200 if ok else 503
    return Health(status="ok" if ok else "degraded", checks=checks)


@router.get("/metrics")
def metrics(ctx: CtxDep) -> Response:
    return Response(generate_latest(ctx.metrics.registry), media_type=CONTENT_TYPE_LATEST)
