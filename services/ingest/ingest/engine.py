"""power_threshold rule evaluation: pure logic, no I/O, so it is unit-testable.

Persistence (min_duration_s):
  For every (rule, signal, sensor) we track an *episode*: the event-time span over
  which matching observations arrived with gaps <= rule.max_gap_s. An observation
  only produces a match once its episode spans >= min_duration_s, so a short burst
  never opens an alert. Only first_ts/last_ts are kept, never the observations.

  The episode is per sensor on purpose: a far-away sensor seeing the same frequency
  weakly must not cancel the persistence seen by a close sensor. The alert service
  then deduplicates across sensors by (rule, signal).

Event time:
  All timing uses the observation timestamp, never arrival time.
  - Out-of-order observations inside an episode just widen it (min/max).
  - An observation older than (watermark - max_lateness_s) is stored but ignored
    here; the watermark is the newest event time seen so far.

State is in memory: it resets on restart, and ingest must run as a single replica.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


class RuleLike(Protocol):
    rule_id: str
    name: str
    type: str
    band_min_mhz: float
    band_max_mhz: float
    threshold_dbm: float
    min_duration_s: float
    max_gap_s: float
    resolve_after_s: float
    severity: str
    enabled: bool


class ObservationLike(Protocol):
    sensor_id: str
    timestamp: datetime
    frequency_mhz: float
    power_dbm: float


@dataclass
class Episode:
    first_ts: datetime
    last_ts: datetime
    confirmed: bool = False


@dataclass(frozen=True)
class Match:
    rule: RuleLike
    signal_key: str
    condition_start: datetime


def signal_key(frequency_mhz: float, tolerance_mhz: float) -> str:
    """Bucket a frequency so near-identical readings map to one signal."""
    bucket = round(frequency_mhz / tolerance_mhz) * tolerance_mhz
    return f"{bucket:.3f}"


class RuleEngine:
    PRUNE_EVERY = 1000  # evaluations

    def __init__(self, tolerance_mhz: float = 0.1, max_lateness_s: float = 300):
        self.tolerance_mhz = tolerance_mhz
        self.max_lateness = timedelta(seconds=max_lateness_s)
        self.watermark: datetime | None = None
        self._episodes: dict[tuple[str, str, str], Episode] = {}
        self._evaluations = 0

    def evaluate(self, obs: ObservationLike, rules: list[RuleLike]) -> list[Match]:
        if self.watermark is None or obs.timestamp > self.watermark:
            self.watermark = obs.timestamp
        if obs.timestamp < self.watermark - self.max_lateness:
            return []  # too late to influence alerting

        matches = []
        for rule in rules:
            if not rule.enabled or rule.type != "power_threshold":
                continue
            if not (rule.band_min_mhz <= obs.frequency_mhz <= rule.band_max_mhz):
                continue
            m = self._evaluate_power_threshold(rule, obs)
            if m:
                matches.append(m)

        self._evaluations += 1
        if self._evaluations % self.PRUNE_EVERY == 0:
            self.prune()
        return matches

    def _evaluate_power_threshold(self, rule: RuleLike, obs: ObservationLike) -> Match | None:
        key_signal = signal_key(obs.frequency_mhz, self.tolerance_mhz)
        key = (rule.rule_id, key_signal, obs.sensor_id)
        ep = self._episodes.get(key)
        gap = timedelta(seconds=rule.max_gap_s)
        ts = obs.timestamp

        if obs.power_dbm <= rule.threshold_dbm:
            # Condition broken, but only by a reading newer than the episode.
            # A late, weak reading from the middle of an episode does not cancel it.
            if ep and ts > ep.last_ts:
                del self._episodes[key]
            return None

        if ep is None or ts - ep.last_ts > gap or ep.first_ts - ts > gap:
            if ep is not None and ts < ep.first_ts:
                return None  # stale reading from before the current episode
            ep = Episode(first_ts=ts, last_ts=ts)
            self._episodes[key] = ep
        else:
            ep.first_ts = min(ep.first_ts, ts)
            ep.last_ts = max(ep.last_ts, ts)

        if (ep.last_ts - ep.first_ts).total_seconds() >= rule.min_duration_s:
            ep.confirmed = True
        if not ep.confirmed:
            return None
        return Match(rule=rule, signal_key=key_signal, condition_start=ep.first_ts)

    def clear_rule(self, rule_id: str) -> None:
        """Drop all state of a rule (called when it is updated or deleted)."""
        for key in [k for k in self._episodes if k[0] == rule_id]:
            del self._episodes[key]

    def prune(self) -> None:
        """Drop episodes nothing can extend any more, so memory stays bounded."""
        if self.watermark is None:
            return
        horizon = self.watermark - self.max_lateness - timedelta(minutes=5)
        for key in [k for k, ep in self._episodes.items() if ep.last_ts < horizon]:
            del self._episodes[key]

    @property
    def episode_count(self) -> int:
        return len(self._episodes)
