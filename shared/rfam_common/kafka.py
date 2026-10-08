"""Kafka publishing (confluent-kafka) plus an in-memory double with the same interface."""

import json
import logging

from confluent_kafka import KafkaException, Producer

from models import TOPIC, RuleMatchEvent

log = logging.getLogger(__name__)


class PublishError(Exception):
    """Raised when not every message of a batch was acknowledged by the broker."""


class KafkaPublisher:
    def __init__(self, bootstrap: str, topic: str = TOPIC, timeout_s: float = 10):
        self.topic = topic
        self.timeout_s = timeout_s
        self._producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",                # the broker has written it before we say "sent"
            "enable.idempotence": True,   # librdkafka retries never write a message twice
        })

    def publish_batch(self, events: list[RuleMatchEvent]) -> None:
        """Send all events and block until every one is acknowledged, or raise.

        The caller commits its DB transaction only after this returns, so a failure
        here means: roll back and let the sensor retry."""
        errors: list[str] = []

        def on_delivery(err, _msg):
            if err is not None:
                errors.append(str(err))

        try:
            for ev in events:
                self._producer.produce(
                    self.topic, key=ev.kafka_key(), value=ev.model_dump_json(),
                    on_delivery=on_delivery,
                )
                self._producer.poll(0)  # serve delivery callbacks, free the local queue
        except (BufferError, KafkaException) as e:
            raise PublishError(str(e)) from e
        remaining = self._producer.flush(self.timeout_s)
        if remaining or errors:
            raise PublishError(f"{remaining} not delivered, errors: {errors[:3]}")

    def ping(self) -> bool:
        try:
            self._producer.list_topics(timeout=2)
            return True
        except Exception:
            return False

    def close(self) -> None:
        self._producer.flush(self.timeout_s)


class InMemoryPublisher:
    """Test double: records what would have been sent; `fail = True` simulates an outage."""

    def __init__(self):
        self.messages: list[dict] = []
        self.keys: list[str] = []
        self.fail = False

    def publish_batch(self, events: list[RuleMatchEvent]) -> None:
        if self.fail:
            raise PublishError("simulated broker outage")
        for ev in events:
            self.keys.append(ev.kafka_key())
            self.messages.append(json.loads(ev.model_dump_json()))

    def ping(self) -> bool:
        return not self.fail

    def close(self) -> None:
        pass
