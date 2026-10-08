"""Print rule-match messages as they arrive on Kafka (debugging and screenshots).

    python tools/kafka_tail.py [--bootstrap localhost:9094] [--from-start]

Uses its own throwaway consumer group, so it never steals messages from the alert service.
"""

import argparse
import logging
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "shared")]

from models import RuleMatchEvent  # noqa: E402
from rfam_common.kafka import KafkaConsumerLoop  # noqa: E402


def show(ev: RuleMatchEvent) -> None:
    o = ev.observation
    print(f"{o.timestamp:%H:%M:%S} {ev.rule_id} signal={ev.signal_key} sensor={o.sensor_id} "
          f"power={o.power_dbm} dBm  since={ev.condition_start:%H:%M:%S}  "
          f"key={ev.kafka_key()}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="127.0.0.1:9094")
    ap.add_argument("--from-start", action="store_true", help="also print old messages")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)

    loop = KafkaConsumerLoop(args.bootstrap, f"kafka-tail-{uuid.uuid4().hex[:8]}", show,
                             offset_reset="earliest" if args.from_start else "latest")
    try:
        loop.run()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    main()
