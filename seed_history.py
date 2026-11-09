"""
RedAlert Incident History Seed Script
Phase 8A: Inserts 5,000 realistic closed incident records into Redis using JSON.SET
at closed:incident:{id} for testing RediSearch (idx:incidents) queries and FT.AGGREGATE analytics.
"""

import argparse
import random
import time

try:
    from app.redis_client import redis_client
except ImportError:
    try:
        from dispatchx.app.redis_client import redis_client
    except ImportError:
        import redis
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

from redis.commands.search.field import TagField, NumericField
from redis.commands.search.index_definition import IndexDefinition, IndexType

INCIDENT_TYPES = [
    "Cardiac Arrest",
    "Severe Stroke",
    "Major Highway Collision",
    "Acute Respiratory Distress",
    "Severe Burn Injury",
    "Anaphylactic Shock",
    "Pedestrian Trauma",
    "Fall from Height",
    "Diabetic Emergency",
    "Active Seizure",
]

ZONES = ["north", "south", "east", "west", "central"]


def ensure_search_index():
    """
    Ensures RediSearch index idx:incidents exists on closed:incident: JSON keys.
    Redis Command: FT.CREATE idx:incidents ON JSON PREFIX 1 closed:incident: SCHEMA ...
    """
    schema = (
        TagField("$.type", as_name="type"),
        TagField("$.zone", as_name="zone"),
        NumericField("$.severity", as_name="severity", sortable=True),
        NumericField("$.closed_at", as_name="closed_at", sortable=True),
        NumericField("$.response_secs", as_name="response_secs", sortable=True),
    )
    try:
        redis_client.ft("idx:incidents").create_index(
            schema,
            definition=IndexDefinition(prefix=["closed:incident:"], index_type=IndexType.JSON),
        )
        print("✅ RediSearch index 'idx:incidents' created successfully.")
    except Exception as exc:
        if "already exists" in str(exc).lower() or "busy" in str(exc).lower():
            print("ℹ️ RediSearch index 'idx:incidents' already exists.")
        else:
            raise exc


def seed_history(count: int = 5000, batch_size: int = 500):
    print("=" * 65)
    print(f"  🌱 RedAlert Incident History Seeder")
    print(f"  Target: {count:,} closed incident records into closed:incident:*")
    print("=" * 65)

    ensure_search_index()

    now_ts = int(time.time())
    # Spread closed timestamps over the past 30 days
    thirty_days_sec = 30 * 24 * 3600
    start_ts = now_ts - thirty_days_sec

    print(f"Generating and writing {count:,} JSON records in batches of {batch_size:,}...")
    t0 = time.perf_counter()
    inserted = 0

    for batch_start in range(1, count + 1, batch_size):
        pipe = redis_client.pipeline(transaction=False)
        batch_end = min(batch_start + batch_size, count + 1)

        for i in range(batch_start, batch_end):
            incident_id = f"seed-{i:05d}"
            key = f"closed:incident:{incident_id}"

            record = {
                "type": random.choice(INCIDENT_TYPES),
                "zone": random.choice(ZONES),
                "severity": random.randint(1, 10),
                "closed_at": random.randint(start_ts, now_ts),
                # Typical emergency response time: 2 to 15 minutes (120 to 900 seconds)
                "response_secs": random.randint(120, 900),
                "assigned_unit": f"sim-{random.randint(1, 50):02d}",
            }

            # Redis Command: JSON.SET closed:incident:{id} $ <data>
            # Why JSON: Encodes structured documents directly supported by RediSearch queries.
            pipe.json().set(key, "$", record)

        pipe.execute()
        inserted += (batch_end - batch_start)
        print(f"  -> Written {inserted:,}/{count:,} records ({(inserted / count) * 100:.1f}%)")

    duration = time.perf_counter() - t0
    rate = count / duration if duration > 0 else 0
    print("-" * 65)
    print(f"🎉 Successfully seeded {count:,} closed incidents in {duration:.2f}s ({rate:.0f} docs/sec)!")
    print("You can now query:")
    print("  curl.exe \"http://localhost:8000/history/search?zone=central&min_severity=8&limit=5\"")
    print("  curl.exe \"http://localhost:8000/history/stats/by-zone\"")
    print("=" * 65)


def main():
    parser = argparse.ArgumentParser(description="Seed 5,000 closed incidents into Redis for RediSearch")
    parser.add_argument("--count", type=int, default=5000, help="Number of records to insert (default: 5000)")
    parser.add_argument("--batch-size", type=int, default=500, help="Pipeline batch size (default: 500)")
    args = parser.parse_args()

    seed_history(count=args.count, batch_size=args.batch_size)


if __name__ == "__main__":
    main()

