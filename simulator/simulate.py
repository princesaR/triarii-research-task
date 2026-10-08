#!/usr/bin/env python3
"""RF sensor traffic simulator (standard library only, runs anywhere with Python 3.12).

Scenarios (all on by default, running together on one timeline):
  background  sensors 01-05 report weak readings across the spectrum, including weak
              readings inside the protected bands                 -> must NOT alert
  persistent  strong emitter at 2437 MHz (ISM band), seen at once by sensor-03 (~-40 dBm)
              and sensor-05 (~-47 dBm), from t=5 s                -> ONE power_threshold
              alert listing both sensors (multi_sensor rules are not implemented, so
              "seen by multiple sensors" shows up as sensor_ids on that alert)
  burst       sensor-04 sees 2462 MHz at -35 dBm for 3 s at t=20 s -> must NOT alert
  disappear   the persistent emitter stops at --emitter-off-at    -> its alert
              AUTO-RESOLVES resolve_after_s later (60 s for the default rule)

Timestamps are the real current time (UTC), so the alert service's wall-clock
auto-resolve behaves as in production.

Examples:
  python simulator/simulate.py --url http://127.0.0.1:8080 --watch --check
  python simulator/simulate.py --url http://127.0.0.1:8001 --alerts-url http://127.0.0.1:8002
  python simulator/simulate.py --scenarios background,burst --duration 40

Use 127.0.0.1 rather than localhost: on Windows, "localhost" tries IPv6 first and
can add ~2 s to every request.
"""

import argparse
import json
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime

SENSORS = {
    "sensor-01": (32.0853, 34.7818),
    "sensor-02": (32.0940, 34.7900),
    "sensor-03": (32.0800, 34.7800),
    "sensor-04": (32.0700, 34.7700),
    "sensor-05": (32.0890, 34.7750),
}
ALL_SCENARIOS = ("background", "persistent", "burst", "disappear")
EMITTER_MHZ, BURST_MHZ = 2437.0, 2462.0
EMITTER_ON_AT, BURST_AT, BURST_LEN = 5.0, 20.0, 3.0
RESOLVE_AFTER_S = 60  # resolve_after_s of rule-ism-high-power in the default rules file


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def obs(sensor: str, freq: float, power: float, bw: float = 20000) -> dict:
    lat, lon = SENSORS[sensor]
    return {"sensor_id": sensor, "timestamp": now_iso(), "frequency_mhz": round(freq, 3),
            "bandwidth_khz": bw, "power_dbm": round(power, 1),
            "location": {"lat": lat, "lon": lon}}


def build_tick(t: float, scenarios: set[str], emitter_off_at: float) -> list[dict]:
    """All observations for one tick at t seconds since start."""
    batch = []
    if "background" in scenarios:
        for sensor in SENSORS:
            # Weak noise anywhere in 100-6000 MHz, and weak in-band traffic (below -50 dBm).
            batch.append(obs(sensor, random.uniform(100, 6000), random.uniform(-110, -75),
                             bw=random.choice([200, 1000, 5000, 20000])))
            batch.append(obs(sensor, random.uniform(2400, 2483), random.uniform(-95, -65)))

    off = emitter_off_at if "disappear" in scenarios else float("inf")
    if "persistent" in scenarios and EMITTER_ON_AT <= t < off:
        # Same emitter, two sensors: the far one hears it weaker. Small frequency jitter
        # stays inside one signal bucket (SIGNAL_TOLERANCE_MHZ = 0.1).
        batch.append(obs("sensor-03", EMITTER_MHZ + random.uniform(-0.02, 0.02),
                         -40 + random.uniform(-2, 2)))
        batch.append(obs("sensor-05", EMITTER_MHZ + random.uniform(-0.02, 0.02),
                         -47 + random.uniform(-1.5, 1.5)))

    if "burst" in scenarios and BURST_AT <= t < BURST_AT + BURST_LEN:
        batch.append(obs("sensor-04", BURST_MHZ, -35 + random.uniform(-1, 1)))
    return batch


def http(method: str, url: str, body=None, timeout: float = 15):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def fetch_alerts(alerts_url: str, since: datetime) -> list[dict]:
    """Alerts touched by this run (last_seen after start), so old alerts do not interfere."""
    status, alerts = http("GET", f"{alerts_url}/api/v1/alerts?limit=100")
    if status != 200:
        raise RuntimeError(f"GET alerts -> HTTP {status}: {str(alerts)[:200]}")
    return [a for a in alerts if datetime.fromisoformat(a["last_seen"]) >= since]


def print_alerts(alerts: list[dict]) -> None:
    if not alerts:
        print("  [alerts] none")
    for a in alerts:
        resolved = f" resolved_by={a['resolved_by']}" if a["resolved_by"] else ""
        print(f"  [alerts] {a['alert_id']} {a['state']:<12} {a['rule_id']} "
              f"{a['frequency_mhz']} MHz sensors={a['sensor_ids']} occ={a['occurrences']} "
              f"peak={a['peak_power_dbm']} dBm{resolved}")


def check(alerts: list[dict], scenarios: set[str], resolve_expected: bool) -> list[str]:
    """Compare the alerts of this run with what the scenarios should have produced."""
    problems = []
    emitter = [a for a in alerts if a["frequency_mhz"] == EMITTER_MHZ]
    others = [a for a in alerts if a["frequency_mhz"] != EMITTER_MHZ]

    if "persistent" in scenarios:
        if len(emitter) != 1:
            problems.append(f"persistent: expected 1 alert at {EMITTER_MHZ} MHz, "
                            f"got {len(emitter)}")
        else:
            a = emitter[0]
            if not {"sensor-03", "sensor-05"} <= set(a["sensor_ids"]):
                problems.append("persistent: expected sensor-03 and sensor-05, "
                                f"got {a['sensor_ids']}")
            if resolve_expected and (a["state"] != "RESOLVED" or a["resolved_by"] != "auto"):
                problems.append(f"disappear: expected RESOLVED by auto, got {a['state']} "
                                f"by {a['resolved_by']}")
    elif emitter:
        problems.append(f"unexpected alert at {EMITTER_MHZ} MHz")

    if "burst" in scenarios and any(a["frequency_mhz"] == BURST_MHZ for a in others):
        problems.append(f"burst: a 3 s burst at {BURST_MHZ} MHz must not open an alert")
    stray = [a for a in others if a["frequency_mhz"] != BURST_MHZ]
    if stray:
        problems.append(f"background: unexpected alerts {[a['frequency_mhz'] for a in stray]}")
    return problems


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--url", default="http://127.0.0.1:8080",
                   help="ingest base URL (the reverse proxy once it exists)")
    p.add_argument("--alerts-url", help="alert service base URL (default: same as --url)")
    p.add_argument("--rate", type=float, default=1.0, help="batches per second (default 1)")
    p.add_argument("--duration", type=float, default=150, help="seconds to run (default 150)")
    p.add_argument("--emitter-off-at", type=float, default=60,
                   help="second at which the persistent emitter disappears (default 60)")
    p.add_argument("--scenarios", default=",".join(ALL_SCENARIOS),
                   help=f"comma separated subset of: {', '.join(ALL_SCENARIOS)}")
    p.add_argument("--watch", action="store_true", help="print alert states every 10 s")
    p.add_argument("--check", action="store_true",
                   help="at the end, verify the expected alerts; exit 1 if wrong")
    p.add_argument("--seed", type=int, help="random seed, for reproducible runs")
    args = p.parse_args()

    scenarios = {s.strip() for s in args.scenarios.split(",") if s.strip()}
    if unknown := scenarios - set(ALL_SCENARIOS):
        p.error(f"unknown scenarios: {sorted(unknown)}")
    if args.rate <= 0:
        p.error("--rate must be positive")
    random.seed(args.seed)

    base = args.url.rstrip("/")
    alerts_url = (args.alerts_url or args.url).rstrip("/")
    # Auto-resolve needs: emitter off, then resolve_after_s of silence, then a sweep (5 s).
    resolve_expected = ("disappear" in scenarios
                        and args.duration >= args.emitter_off_at + RESOLVE_AFTER_S + 10)
    if "disappear" in scenarios and not resolve_expected:
        print(f"note: --duration < emitter-off-at + {RESOLVE_AFTER_S + 10}s, "
              "the auto-resolve will not be visible in this run")

    started = datetime.now(UTC)
    start = time.monotonic()
    interval = 1.0 / args.rate
    sent = rejected = errors = 0
    next_watch = 0.0
    print(f"simulating {sorted(scenarios)} -> {base} at {args.rate}/s for {args.duration:.0f}s")

    while (t := time.monotonic() - start) < args.duration:
        batch = build_tick(t, scenarios, args.emitter_off_at)
        try:
            status, body = http("POST", f"{base}/api/v1/observations", batch)
            if status in (200, 207):
                sent += body["accepted"]
                rejected += len(body["rejected"])
            else:
                errors += 1
                print(f"  t={t:5.1f}s HTTP {status}: {str(body)[:200]}")
        except OSError as e:
            errors += 1
            print(f"  t={t:5.1f}s connection error: {e}")

        if args.watch and t >= next_watch:
            print(f"t={t:5.1f}s sent={sent} rejected={rejected} errors={errors}")
            try:
                print_alerts(fetch_alerts(alerts_url, started))
            except (OSError, RuntimeError) as e:
                print(f"  [alerts] {e}")
            next_watch += 10
        time.sleep(max(0.0, interval - ((time.monotonic() - start) - t)))

    print(f"done: sent={sent} rejected={rejected} errors={errors}")
    if not (args.watch or args.check):
        return 0 if errors == 0 else 1

    alerts = fetch_alerts(alerts_url, started)
    print_alerts(alerts)
    if not args.check:
        return 0 if errors == 0 else 1
    problems = check(alerts, scenarios, resolve_expected)
    if errors:
        problems.append(f"{errors} requests failed")
    for line in problems:
        print(f"CHECK FAILED: {line}")
    if not problems:
        print("CHECK OK: all scenarios produced the expected alerts")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
