"""Kafka publishing and consuming (confluent-kafka), plus in-memory test doubles."""

import json
import logging
import threading
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from pydantic import ValidationError

from models import TOPIC, RuleMatchEvent

log = logging.getLogger(__name__)

Handler = Callable[[RuleMatchEvent], None]


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


class KafkaConsumerLoop:
    """Reads rule matches and hands each one to `handler`, at-least-once.

    Order per message: validate -> handler (does the DB commit) -> commit the offset.
    A crash between the DB commit and the offset commit redelivers the message, so
    the handler must be idempotent (the alert service uses a unique constraint).

    - Bad message (not JSON / wrong schema): it can never succeed, so it is logged
      (with topic/partition/offset and payload) and its offset committed. It must not
      block the partition. No dead-letter topic, to keep Kafka small: the log line is
      the only record of it.
    - Handler error (e.g. DB down): retried with backoff, offset not committed.
    """

    def __init__(self, bootstrap: str, group_id: str, handler: Handler,
                 topic: str = TOPIC, offset_reset: str = "earliest"):
        self.topic, self.handler = topic, handler
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "enable.auto.commit": False,     # we commit only after the handler succeeded
            # Where a *new* group starts: the alert service wants every message ("earliest").
            "auto.offset.reset": offset_reset,
        })
        # For ping() only: unlike the consumer, the admin client is safe to use from
        # another thread (the /health endpoint).
        self._admin = AdminClient({"bootstrap.servers": bootstrap})
        self._stop = threading.Event()
        self.last_poll: float = 0.0  # for /health: proves the loop is alive
        self.processed = 0
        self.skipped = 0

    def run(self) -> None:
        """Blocking loop; run it in a thread and call stop() to end it."""
        self._consumer.subscribe([self.topic])
        log.info("consuming %s", self.topic)
        try:
            while not self._stop.is_set():
                self.last_poll = time.monotonic()
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error: %s", msg.error())
                    continue
                self._process(msg)
        finally:
            self._consumer.close()  # leaves the group cleanly, triggers a fast rebalance

    def _process(self, msg: Message) -> None:
        try:
            event = RuleMatchEvent.model_validate_json(msg.value())
        except ValidationError as e:
            log.error("skipping bad message %s/%s/%s: %s | payload=%r", msg.topic(),
                      msg.partition(), msg.offset(), str(e).splitlines()[0],
                      (msg.value() or b"")[:500])
            self._consumer.commit(message=msg, asynchronous=False)
            self.skipped += 1
            return

        delay = 1.0
        while not self._stop.is_set():
            try:
                self.handler(event)
                break
            except Exception:
                log.exception("handler failed for offset %s, retrying in %.0fs",
                              msg.offset(), delay)
                self._stop.wait(delay)
                delay = min(delay * 2, 30)
        else:
            return  # stopping: leave the offset uncommitted, it is redelivered on restart
        self._consumer.commit(message=msg, asynchronous=False)
        self.processed += 1

    def stop(self) -> None:
        self._stop.set()

    def ping(self) -> bool:
        try:
            self._admin.list_topics(timeout=2)
            return True
        except Exception:
            return False

    def is_alive(self, max_silence_s: float = 15) -> bool:
        return time.monotonic() - self.last_poll < max_silence_s
