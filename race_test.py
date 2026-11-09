"""
RedAlert Race Condition & Concurrency Test
Phase 6: Verifies race condition resilience across assignment modes:
- 'unsafe': Unprotected read-then-write (prone to double-assignments)
- 'lock':   SET NX PX 5000 distributed mutex
- 'watch':  Optimistic concurrency with WATCH / MULTI / EXEC
- 'lua':    Atomic Lua script execution on Redis thread

Creates 5 free units and 20 incidents, runs 10 parallel worker threads,
and counts double-assignments (a unit assigned to >1 incident).
"""

import argparse
import concurrent.futures
import json
import os
import random
import sys
import time
from typing import Dict, List, Set, Tuple, Any

try:
    from app.redis_client import redis_client
except ImportError:
    try:
        from dispatchx.app.redis_client import redis_client
    except ImportError:
        import redis
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Import process_incident_message and assignment functions
try:
    from worker import (
        ensure_consumer_group,
        process_incident_message,
        STREAM_NAME,
        GROUP_NAME,
    )
except ImportError:
    from dispatchx.worker import (
        ensure_consumer_group,
        process_incident_message,
        STREAM_NAME,
        GROUP_NAME,
    )

NUM_UNITS = 5
NUM_INCIDENTS = 20
NUM_WORKERS = 10

# Fixed coordinates around Bengaluru center
BASE_LAT = 12.97
BASE_LON = 77.59


def setup_race_environment() -> List[str]:
    """
    Sets up a clean testing environment in Redis:
    - Purges test streams, sets, and incident keys.
    - Creates 5 free units clustered around (12.97, 77.59).
    - Seeds 20 incidents directly into incidents:stream.
    """
    # 1. Clean previous race test keys
    redis_client.delete(STREAM_NAME, "units:free", "units:busy", "units:geo", "incidents:queue")

    # Clean old test hashes
    for i in range(1, NUM_UNITS + 10):
        redis_client.delete(f"unit:race-{i:02d}", f"lock:unit:race-{i:02d}")
    for i in range(1, NUM_INCIDENTS + 20):
        redis_client.delete(f"incident:race-{i:02d}")

    # Re-create consumer group on clean stream
    ensure_consumer_group()

    # 2. Register 5 free units
    unit_ids = []
    pipe = redis_client.pipeline(transaction=False)
    for i in range(1, NUM_UNITS + 1):
        uid = f"race-{i:02d}"
        member = f"unit:{uid}"
        lat = BASE_LAT + (i * 0.002)
        lon = BASE_LON + (i * 0.002)

        pipe.hset(member, mapping={"type": "ALS", "status": "free", "driver": f"RaceDriver-{i}"})
        pipe.sadd("units:free", uid)
        pipe.set(f"hb:unit:{uid}", 1, ex=120)  # Long heartbeat for test stability
        pipe.geoadd("units:geo", (lon, lat, member))
        unit_ids.append(uid)

    pipe.execute()

    # 3. Inject 20 incidents into incidents:stream
    pipe = redis_client.pipeline(transaction=False)
    for i in range(1, NUM_INCIDENTS + 1):
        inc_id = f"race-{i:02d}"
        lat = BASE_LAT + random.uniform(-0.01, 0.01)
        lon = BASE_LON + random.uniform(-0.01, 0.01)

        # Initialize incident hash
        pipe.hset(f"incident:{inc_id}", mapping={
            "id": inc_id,
            "type": "Race Emergency",
            "lat": str(lat),
            "lon": str(lon),
            "severity": str(random.randint(5, 10)),
            "status": "pending",
        })

        # Add to stream
        pipe.xadd(STREAM_NAME, {
            "incident_id": inc_id,
            "type": "Race Emergency",
            "lat": str(lat),
            "lon": str(lon),
            "severity": str(random.randint(5, 10)),
        })

    pipe.execute()
    return unit_ids


def worker_thread_loop(worker_id: int, mode: str, stop_signal: Dict[str, bool]) -> int:
    """
    Worker thread that repeatedly calls XREADGROUP and attempts assignment in the specified mode.
    """
    worker_name = f"race-worker-{worker_id}"
    assigned_count = 0

    while not stop_signal["stop"]:
        try:
            # Redis Command: XREADGROUP GROUP dispatchers <name> COUNT 1 BLOCK 300 STREAMS incidents:stream >
            entries = redis_client.xreadgroup(
                GROUP_NAME,
                worker_name,
                {STREAM_NAME: ">"},
                count=1,
                block=300,
            )

            if not entries:
                # No new messages currently available
                time.sleep(0.01)
                continue

            for stream_name, messages in entries:
                for message_id, fields in messages:
                    success = process_incident_message(worker_name, message_id, fields, mode=mode)
                    if success:
                        assigned_count += 1
        except Exception:
            pass

    return assigned_count


def evaluate_double_assignments() -> Tuple[int, Dict[str, List[str]]]:
    """
    Inspects all race incident hashes and tallies assignments.
    Detects if any unit was assigned to more than 1 incident concurrently.
    """
    unit_to_incidents: Dict[str, List[str]] = {}

    for i in range(1, NUM_INCIDENTS + 1):
        inc_id = f"race-{i:02d}"
        assigned_unit = redis_client.hget(f"incident:{inc_id}", "assigned_unit")
        if assigned_unit:
            unit_to_incidents.setdefault(assigned_unit, []).append(inc_id)

    double_assignments = 0
    conflicts: Dict[str, List[str]] = {}

    for uid, incidents in unit_to_incidents.items():
        if len(incidents) > 1:
            double_assignments += (len(incidents) - 1)
            conflicts[uid] = incidents

    return double_assignments, conflicts


def run_benchmark_for_mode(mode: str) -> Dict[str, Any]:
    """
    Runs the full race benchmark for a single mode:
    1. Sets up 5 units and 20 incidents.
    2. Spawns 10 parallel worker threads.
    3. Measures execution time and double-assignments.
    """
    print(f"\n" + "=" * 60)
    print(f"  🏁 Running Race Test Mode: [{mode.upper()}]")
    print(f"  Units: {NUM_UNITS} | Incidents: {NUM_INCIDENTS} | Threads: {NUM_WORKERS}")
    print("=" * 60)

    setup_race_environment()

    stop_signal = {"stop": False}
    start_time = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [
            executor.submit(worker_thread_loop, i, mode, stop_signal)
            for i in range(1, NUM_WORKERS + 1)
        ]

        # Allow workers to consume the 20 incidents
        # Wait until either all units are busy or stream is processed
        timeout_seconds = 10.0
        poll_interval = 0.2
        elapsed = 0.0

        while elapsed < timeout_seconds:
            time.sleep(poll_interval)
            elapsed += poll_interval
            # Check PEL + unread entries
            pending_info = redis_client.xpending(STREAM_NAME, GROUP_NAME)
            free_count = redis_client.scard("units:free")
            # If no free units remain, remaining incidents cannot be dispatched anyway
            if free_count == 0:
                time.sleep(0.5)  # Grace period for final writes
                break

        stop_signal["stop"] = True
        concurrent.futures.wait(futures, timeout=2.0)

    total_time = time.perf_counter() - start_time
    double_assignments, conflicts = evaluate_double_assignments()
    busy_units = redis_client.smembers("units:busy")

    print(f"[{mode.upper()}] Benchmark Results:")
    print(f"  - Total Elapsed Time:   {total_time * 1000.0:.2f} ms")
    print(f"  - Double Assignments:   {double_assignments}")
    print(f"  - Units Marked Busy:    {len(busy_units)}/{NUM_UNITS}")
    if conflicts:
        print(f"  - Conflicts Detected:   {conflicts}")
    else:
        print(f"  - Conflicts Detected:   None (Clean atomic assignment)")

    return {
        "mode": mode,
        "elapsed_ms": round(total_time * 1000.0, 2),
        "double_assignments": double_assignments,
        "conflicts": conflicts,
    }


def main():
    parser = argparse.ArgumentParser(description="RedAlert Phase 6 Race Benchmark")
    parser.add_argument(
        "--mode",
        choices=["unsafe", "lock", "watch", "lua", "all"],
        default="all",
        help="Mode to benchmark: 'unsafe', 'lock', 'watch', 'lua', or 'all' (default: all)"
    )
    args = parser.parse_args()

    modes_to_test = ["unsafe", "lock", "watch", "lua"] if args.mode == "all" else [args.mode]
    results = []

    for mode in modes_to_test:
        res = run_benchmark_for_mode(mode)
        results.append(res)
        time.sleep(0.5)

    print("\n" + "=" * 65)
    print("  📊 SUMMARY OF PHASE 6 CONCURRENCY BENCHMARK")
    print("=" * 65)
    print(f"{'Mode':<10} | {'Safety':<14} | {'Double Assigns':<16} | {'Time (ms)':<10}")
    print("-" * 65)
    for r in results:
        safety = "❌ UNSAFE" if r["double_assignments"] > 0 else "✅ SAFE"
        print(f"{r['mode']:<10} | {safety:<14} | {r['double_assignments']:<16} | {r['elapsed_ms']:<10.2f}")
    print("=" * 65)


if __name__ == "__main__":
    main()
