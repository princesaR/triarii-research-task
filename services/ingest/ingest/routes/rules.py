"""Rules CRUD. Each change reloads the rule cache and clears that rule's engine state,
so new thresholds never mix with persistence measured under the old ones."""

from fastapi import APIRouter, HTTPException
from sqlalchemy.orm import Session

from ingest import rules as rules_repo
from ingest.deps import CtxDep, SessionDep
from ingest.models import Rule
from models import RuleIn, RuleOut, RuleUpdate

router = APIRouter(prefix="/api/v1/rules", tags=["rules"])


@router.get("")
def list_rules(s: SessionDep) -> list[RuleOut]:
    return rules_repo.load_all(s)


@router.get("/{rule_id}")
def get_rule(rule_id: str, s: SessionDep) -> RuleOut:
    return _get_or_404(s, rule_id)


@router.post("", status_code=201)
def create_rule(rule: RuleIn, ctx: CtxDep, s: SessionDep) -> RuleOut:
    with ctx.lock:
        if s.get(Rule, rule.rule_id):
            raise HTTPException(409, f"rule {rule.rule_id} already exists")
        row = rules_repo.from_in(rule)
        s.add(row)
        s.commit()
        ctx.reload_rules(s)
    return row


@router.put("/{rule_id}")
def update_rule(rule_id: str, patch: RuleUpdate, ctx: CtxDep, s: SessionDep) -> RuleOut:
    """Partial update: only the fields sent are changed, e.g. {"enabled": false}."""
    with ctx.lock:
        row = _get_or_404(s, rule_id)
        rules_repo.apply_update(row, patch)
        s.commit()
        ctx.rule_engine.clear_rule(rule_id)
        ctx.reload_rules(s)
    return row


@router.delete("/{rule_id}", status_code=204)
def delete_rule(rule_id: str, ctx: CtxDep, s: SessionDep) -> None:
    with ctx.lock:
        s.delete(_get_or_404(s, rule_id))
        s.commit()
        ctx.rule_engine.clear_rule(rule_id)
        ctx.reload_rules(s)


def _get_or_404(s: Session, rule_id: str) -> Rule:
    row = s.get(Rule, rule_id)
    if row is None:
        raise HTTPException(404, f"rule {rule_id} not found")
    return row
