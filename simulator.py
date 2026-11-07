"""
RedAlert Ambulance Fleet Simulator
Phases 2 & 4: Real-time geospatial location simulator for 50 ambulance units around Bengaluru,
with heartbeat renewal (hb:unit:{id} EX 10) and live Pub/Sub streaming (unit:updates).
"""

import argparse
import json
import random
import sys
import time
from typing import List, Dict, Any

try:
    from app.redis_client import redis_client
except ImportError:
    try:
        from dispatchx.app.redis_client import redis_client
    except ImportError:
        import redis
        # Shared Redis connection fallback
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Bengaluru center coordinates
BENGALURU_LAT = 12.97
BENGALURU_LON = 77.59
NUM_UNITS = 50


def setup_units(num_units: int = NUM_UNITS) -> List[Dict[str, Any]]:
    """
    Registers fake ambulance units in Redis as hashes and assigns initial locations.
    """
    units = []
    print(f"Registering {num_units} ambulance units in Redis around Bengaluru ({BENGALURU_LAT}, {BENGALURU_LON})...")

    # Use pipeline for initial registration of 50 units
    pipe = redis_client.pipeline(transaction=False)

    for i in range(1, num_units + 1):
        unit_id = f"sim-{i:02d}"
        member = f"unit:{unit_id}"
        unit_type = "ALS" if i % 2 == 0 else "BLS"
        driver_name = f"Driver-{i:02d}"

        # Spread initial coordinates around Bengaluru (~2-3 km radius)
        initial_lat = BENGALURU_LAT + random.uniform(-0.025, 0.025)
        initial_lon = BENGALURU_LON + random.uniform(-0.025, 0.025)

        unit = {
            "id": unit_id,
            "member": member,
            "lat": initial_lat,
            "lon": initial_lon,
        }
        units.append(unit)

        # Redis Command: HSET unit:{id} mapping
        # Why: Registers unit attributes in Redis with status "free"
        pipe.hset(
            member,
            mapping={
                "type": unit_type,
                "status": "free",
                "driver": driver_name,
            },
        )
        # Redis Command: SADD units:free {id}
        # Why: Adds initial registered unit to units:free availability set
        pipe.sadd("units:free", unit_id)

        # Redis Command: SET hb:unit:{id} 1 EX 10
        # Why: Set initial 10s heartbeat TTL so unit is online
        pipe.set(f"hb:unit:{unit_id}", 1, ex=10)

        # Initial GEOADD
        # Redis Command: GEOADD units:geo lon lat unit:{id}
        # Why: Seed initial geospatial position in the sorted set
        pipe.geoadd("units:geo", (initial_lon, initial_lat, member))

    pipe.execute()
    print(f"Successfully registered {num_units} units with status='free'.\n")
    return units


def run_simulation(mode: str, ticks: int = 0) -> None:
    """
    Simulates real-time ambulance movements every second.

    Why Pipelining is Faster:
    -------------------------
    - In 'naive' mode, the client issues 50 separate GEOADD + SET + PUBLISH network requests per second.
      Each individual command incurs a complete network round-trip time (RTT: client -> network
      -> Redis server -> network -> client).
    - In 'pipeline' mode with pipeline(transaction=False), all commands are buffered
      in client memory and transmitted across the network in a single TCP socket write,
      and Redis returns all responses in a single TCP reply.
    - This reduces 150 network round-trips to exactly 1 round-trip per tick, yielding a 20x to 50x
      reduction in elapsed execution time.
    """
    units = setup_units(NUM_UNITS)
    print(f"Starting simulation in '{mode.upper()}' mode...")
    print("Press Ctrl+C to stop simulation.\n")
    print(f"{'Tick':<10} | {'Mode':<10} | {'Execution Time':<18} | {'Units Updated'}")
    print("-" * 55)

    tick = 0
    try:
        while True:
            tick += 1

            # Update coordinates with small random steps (~50-100m jitter)
            for unit in units:
                unit["lat"] += random.uniform(-0.0008, 0.0008)
                unit["lon"] += random.uniform(-0.0008, 0.0008)

            t0 = time.perf_counter()

            if mode == "naive":
                # Naive mode: individual round-trip network calls
                for unit in units:
                    # Redis Command: GEOADD units:geo lon lat unit:{id}
                    redis_client.geoadd(
                        "units:geo",
                        (unit["lon"], unit["lat"], unit["member"]),
                    )
                    # Redis Command: SET hb:unit:{id} 1 EX 10
                    # Why: Refresh heartbeat TTL so unit remains marked online
                    redis_client.set(f"hb:unit:{unit['id']}", 1, ex=10)

                    # Redis Command: PUBLISH unit:updates <json>
                    # Why: Stream real-time telemetry to connected WebSocket clients
                    redis_client.publish("unit:updates", json.dumps({
                        "unit_id": unit["id"],
                        "lat": unit["lat"],
                        "lon": unit["lon"],
                        "status": "free",
                        "timestamp": time.time(),
                    }))
            else:
                # Pipeline mode: all commands batched in 1 single round trip
                # Redis Command: pipeline(transaction=False)
                pipe = redis_client.pipeline(transaction=False)
                for unit in units:
                    pipe.geoadd(
                        "units:geo",
                        (unit["lon"], unit["lat"], unit["member"]),
                    )
                    # Redis Command: SET hb:unit:{id} 1 EX 10
                    pipe.set(f"hb:unit:{unit['id']}", 1, ex=10)

                    # Redis Command: PUBLISH unit:updates <json>
                    pipe.publish("unit:updates", json.dumps({
                        "unit_id": unit["id"],
                        "lat": unit["lat"],
                        "lon": unit["lon"],
                        "status": "free",
                        "timestamp": time.time(),
                    }))
                pipe.execute()

            t1 = time.perf_counter()
            elapsed_ms = (t1 - t0) * 1000.0

            print(f"{tick:<10} | {mode:<10} | {elapsed_ms:>13.2f} ms | {len(units)} units")

            if ticks > 0 and tick >= ticks:
                print(f"\nReached target of {ticks} ticks. Simulation completed.")
                break

            # Sleep remaining time to maintain a 1-second tick interval
            time_spent = t1 - t0
            sleep_duration = max(0.0, 1.0 - time_spent)
            time.sleep(sleep_duration)

    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")


def main():
    parser = argparse.ArgumentParser(
        description="RedAlert Ambulance Fleet Real-time Geospatial Simulator"
    )
    parser.add_argument(
        "--mode",
        choices=["naive", "pipeline"],
        default="pipeline",
        help="Update mode: 'naive' (individual calls) or 'pipeline' (batched calls)",
    )
    parser.add_argument(
        "--ticks",
        type=int,
        default=0,
        help="Number of ticks to run (default: 0 for infinite loop until Ctrl+C)",
    )
    args = parser.parse_args()

    run_simulation(mode=args.mode, ticks=args.ticks)


if __name__ == "__main__":
    main()
