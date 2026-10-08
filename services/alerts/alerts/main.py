"""Alert service: consumes rule matches from Kafka, manages the alert lifecycle.

Two background threads next to the API:
- consumer: Kafka -> lifecycle.apply_match -> DB commit -> offset commit
- sweeper:  every SWEEP_INTERVAL_S, auto-resolves alerts with no recent match

Run: uvicorn alerts.main:app_factory --factory
"""

import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from alerts.deps import AlertsContext
from alerts.metrics import Metrics
from alerts.routes import alerts, ops
from alerts.settings import AlertSettings
from db.alerts import Base
from rfam_common.db import make_engine, make_session_factory
from rfam_common.kafka import KafkaConsumerLoop

log = logging.getLogger("alerts")


def create_app(settings: AlertSettings | None = None, run_workers: bool = True) -> FastAPI:
    """run_workers=False (unit tests): no Kafka, no threads; tests call
    ctx.handle_match() and ctx.sweep_once() directly."""
    settings = settings or AlertSettings()
    db_engine = make_engine(settings.database_url)
    ctx = AlertsContext(settings=settings, db_engine=db_engine,
                        session_factory=make_session_factory(db_engine), metrics=Metrics())
    stop = threading.Event()

    def sweeper_loop() -> None:
        while not stop.wait(settings.sweep_interval_s):
            try:
                ctx.sweep_once()
            except Exception:
                log.exception("sweeper failed, retrying next interval")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        logging.basicConfig(level=settings.log_level)
        Base.metadata.create_all(db_engine)  # no migrations tool yet (see DECISIONS.md)
        if run_workers:
            ctx.consumer = KafkaConsumerLoop(settings.kafka_bootstrap, settings.kafka_group_id,
                                             ctx.handle_match)
            for name, target in (("consumer", ctx.consumer.run), ("sweeper", sweeper_loop)):
                t = threading.Thread(target=target, name=name, daemon=True)
                t.start()
                ctx.threads.append(t)
            log.info("alert service started: consumer group %s, sweep every %ss",
                     settings.kafka_group_id, settings.sweep_interval_s)
        yield
        stop.set()
        if ctx.consumer is not None:
            ctx.consumer.stop()  # finishes the current message, closes the consumer cleanly
        for t in ctx.threads:
            t.join(timeout=10)
        db_engine.dispose()

    app = FastAPI(title="RF Alert Manager - Alerts", version="1.0", lifespan=lifespan)
    app.state.ctx = ctx
    for module in (alerts, ops):
        app.include_router(module.router)
    return app


def app_factory() -> FastAPI:
    return create_app()
