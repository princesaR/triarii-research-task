"""Everything the routes need, built once in create_app() and handed out via Depends."""

import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from db.ingest import Rule
from ingest import rules as rules_repo
from ingest.engine import RuleEngine
from ingest.metrics import Metrics
from ingest.settings import IngestSettings


@dataclass
class IngestContext:
    settings: IngestSettings
    db_engine: Engine
    session_factory: sessionmaker[Session]
    rule_engine: RuleEngine
    publisher: object  # KafkaPublisher or InMemoryPublisher (same interface)
    metrics: Metrics
    # Sync endpoints run in a thread pool: one lock guards the engine state and the
    # rule cache, and keeps evaluate -> publish -> commit in event-time order.
    lock: threading.Lock = field(default_factory=threading.Lock)
    rules: list[Rule] = field(default_factory=list)  # cache of the rules table

    def reload_rules(self, session: Session) -> None:
        self.rules = rules_repo.load_all(session)


def get_ctx(request: Request) -> IngestContext:
    return request.app.state.ctx


def get_session(request: Request) -> Iterator[Session]:
    with request.app.state.ctx.session_factory() as session:
        yield session


# Short aliases so route signatures stay readable: def handler(ctx: CtxDep, s: SessionDep)
CtxDep = Annotated[IngestContext, Depends(get_ctx)]
SessionDep = Annotated[Session, Depends(get_session)]
