"""Rule persistence and the startup loader for the YAML/JSON rules file."""

import json
import logging
from pathlib import Path

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session

from ingest.models import Rule
from models import RuleIn, RuleUpdate

log = logging.getLogger(__name__)


def from_in(data: RuleIn) -> Rule:
    fields = data.model_dump(exclude={"band"})
    return Rule(**fields, band_min_mhz=data.band.min_mhz, band_max_mhz=data.band.max_mhz)


def apply_update(rule: Rule, patch: RuleUpdate) -> None:
    changes = patch.model_dump(exclude_unset=True)
    band = changes.pop("band", None)
    if band:
        rule.band_min_mhz, rule.band_max_mhz = band["min_mhz"], band["max_mhz"]
    for k, v in changes.items():
        setattr(rule, k, v)


def read_rules_file(path: str) -> list[RuleIn]:
    """Parse a .yaml/.yml or .json file: either {"rules": [...]} or a bare list.

    An invalid file raises (ValidationError/ValueError), so startup fails fast
    instead of running with half the rules."""
    p = Path(path)
    if not p.exists():
        log.warning("rules file %s not found, starting with the rules already in the DB", path)
        return []
    text = p.read_text(encoding="utf-8")
    if p.suffix in (".yaml", ".yml"):
        raw = yaml.safe_load(text)
    elif p.suffix == ".json":
        raw = json.loads(text)
    else:
        raise ValueError(f"unsupported rules file type {p.suffix!r}, use .yaml, .yml or .json")
    items = raw.get("rules", []) if isinstance(raw, dict) else (raw or [])
    rules = [RuleIn.model_validate(item) for item in items]
    ids = [r.rule_id for r in rules]
    if len(ids) != len(set(ids)):
        raise ValueError(f"duplicate rule_id in {path}")
    return rules


def seed_rules(session: Session, path: str) -> int:
    """Insert rules from the file that are not in the DB yet.

    The file is only the seed; after startup the API is the source of truth, so a
    rule edited or disabled through the API is not overwritten on the next restart."""
    existing = set(session.scalars(select(Rule.rule_id)))
    added = 0
    for rule in read_rules_file(path):
        if rule.rule_id not in existing:
            session.add(from_in(rule))
            added += 1
    session.commit()
    return added


def load_all(session: Session) -> list[Rule]:
    return list(session.scalars(select(Rule).order_by(Rule.rule_id)))
