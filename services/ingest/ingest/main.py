"""Ingest service: receives observations, evaluates rules, publishes matches to Kafka.

Run: uvicorn ingest.main:app (or create_app() in tests with an in-memory publisher).
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from db.ingest import Base
from ingest import rules as rules_repo
from ingest.deps import IngestContext
from ingest.engine import RuleEngine
from ingest.metrics import Metrics
from ingest.routes import observations, ops, rules, sensors
from ingest.settings import IngestSettings
from rfam_common.db import make_engine, make_session_factory
from rfam_common.kafka import KafkaPublisher

log = logging.getLogger("ingest")


def create_app(settings: IngestSettings | None = None, publisher=None) -> FastAPI:
    settings = settings or IngestSettings()
    db_engine = make_engine(settings.database_url)
    ctx = IngestContext(
        settings=settings,
        db_engine=db_engine,
        session_factory=make_session_factory(db_engine),
        rule_engine=RuleEngine(settings.signal_tolerance_mhz, settings.max_lateness_s),
        publisher=publisher or KafkaPublisher(settings.kafka_bootstrap),
        metrics=Metrics(),
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        logging.basicConfig(level=settings.log_level)
        Base.metadata.create_all(db_engine)  # no migrations tool yet (see DECISIONS.md)
        with ctx.session_factory() as s:
            added = rules_repo.seed_rules(s, settings.rules_file)
            ctx.reload_rules(s)
        log.info("ingest started: %d rules (%d new from %s)",
                 len(ctx.rules), added, settings.rules_file)
        yield
        ctx.publisher.close()
        db_engine.dispose()

    app = FastAPI(title="RF Alert Manager - Ingest", version="1.0", lifespan=lifespan)
    app.state.ctx = ctx
    for module in (observations, sensors, rules, ops):
        app.include_router(module.router)
    return app


def app_factory() -> FastAPI:
    return create_app()
