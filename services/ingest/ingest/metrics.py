from prometheus_client import CollectorRegistry, Counter, Gauge


class Metrics:
    """One registry per app instance, so tests do not share counters."""

    def __init__(self):
        self.registry = CollectorRegistry()
        self.observations = Counter("rf_observations_total", "Observations received",
                                    ["result"], registry=self.registry)
        self.matches = Counter("rf_rule_matches_total", "Rule matches published",
                               ["rule_id"], registry=self.registry)
        self.publish_errors = Counter("rf_publish_errors_total", "Kafka publish failures",
                                      registry=self.registry)
        self.episodes = Gauge("rf_engine_tracked_episodes", "Signals tracked by the engine",
                              registry=self.registry)
