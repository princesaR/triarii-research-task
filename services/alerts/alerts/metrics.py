from prometheus_client import CollectorRegistry, Counter, Gauge


class Metrics:
    """One registry per app instance, so tests do not share counters."""

    def __init__(self):
        self.registry = CollectorRegistry()
        self.matches = Counter("rf_alert_matches_total", "Rule matches consumed, by outcome",
                               ["action"], registry=self.registry)
        self.opened = Counter("rf_alerts_opened_total", "Alerts opened",
                              ["rule_id", "severity"], registry=self.registry)
        self.resolved = Counter("rf_alerts_resolved_total", "Alerts resolved",
                                ["by"], registry=self.registry)
        self.active = Gauge("rf_alerts_active", "OPEN + ACKNOWLEDGED alerts",
                            registry=self.registry)
