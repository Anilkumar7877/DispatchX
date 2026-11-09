"""
RedAlert Memory Usage Comparison: Set vs HyperLogLog
Phase 7: Compares exact cardinality storage (Redis Set) with probabilistic cardinality
estimation (Redis HyperLogLog) across 100,000 unique caller IDs.

Analyzes:
1. Exact unique count vs HLL estimated count (calculates relative error %).
2. MEMORY USAGE for Set vs HyperLogLog.
3. Memory savings multiplier.
"""

import sys
import time

try:
    from app.redis_client import redis_client
except ImportError:
    try:
        from dispatchx.app.redis_client import redis_client
    except ImportError:
        import redis
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

TOTAL_ITEMS = 100_000
BATCH_SIZE = 5_000

SET_KEY = "benchmark:callers:set"
HLL_KEY = "benchmark:callers:hll"


def run_memory_comparison():
    print("=" * 65)
    print(f"  🧠 RedAlert Memory Comparison: Set vs HyperLogLog")
    print(f"  Target cardinality: {TOTAL_ITEMS:,} unique caller IDs")
    print("=" * 65)

    # 1. Clean previous benchmark keys
    redis_client.delete(SET_KEY, HLL_KEY)

    print(f"Generating and inserting {TOTAL_ITEMS:,} items in batches of {BATCH_SIZE:,}...")
    t0 = time.perf_counter()

    for start_idx in range(0, TOTAL_ITEMS, BATCH_SIZE):
        batch = [f"caller-{i:07d}" for i in range(start_idx, start_idx + BATCH_SIZE)]

        pipe = redis_client.pipeline(transaction=False)
        # Redis Command: SADD benchmark:callers:set <members...>
        # Why Set: Stores exact string values in an in-memory hash table or intset.
        pipe.sadd(SET_KEY, *batch)

        # Redis Command: PFADD benchmark:callers:hll <elements...>
        # Why HyperLogLog: Hashes items and records the maximum number of leading zeros
        # across 16,384 6-bit registers (Dense representation capped at ~12,288 bytes).
        pipe.pfadd(HLL_KEY, *batch)
        pipe.execute()

    insert_duration = time.perf_counter() - t0
    print(f"Insertion completed in {insert_duration:.2f} seconds.\n")

    # 2. Cardinality inspection
    # Redis Command: SCARD benchmark:callers:set
    exact_count = redis_client.scard(SET_KEY)

    # Redis Command: PFCOUNT benchmark:callers:hll
    estimated_count = redis_client.pfcount(HLL_KEY)

    # Calculate HLL error
    absolute_error = abs(estimated_count - exact_count)
    relative_error_pct = (absolute_error / exact_count) * 100.0 if exact_count > 0 else 0.0

    # 3. Memory inspection
    # Redis Command: MEMORY USAGE <key>
    # Why: Reports total memory allocated for the key in bytes (data + object overhead).
    set_memory_bytes = redis_client.memory_usage(SET_KEY) or 0
    hll_memory_bytes = redis_client.memory_usage(HLL_KEY) or 0

    set_kb = set_memory_bytes / 1024.0
    hll_kb = hll_memory_bytes / 1024.0
    savings_factor = set_memory_bytes / hll_memory_bytes if hll_memory_bytes > 0 else 0.0

    # 4. Display Results
    print("-----------------------------------------------------------------")
    print(f"{'Metric':<30} | {'Redis Set':<16} | {'HyperLogLog':<16}")
    print("-----------------------------------------------------------------")
    print(f"{'Reported Count':<30} | {exact_count:<16,} | {estimated_count:<16,}")
    print(f"{'Counting Accuracy':<30} | {'100% (Exact)':<16} | {f'{100.0 - relative_error_pct:.2f}% (Approx)':<16}")
    print(f"{'Absolute Error':<30} | {'0 items':<16} | {f'{absolute_error:,} items':<16}")
    print(f"{'Relative Error %':<30} | {'0.00%':<16} | {f'{relative_error_pct:.2f}%':<16}")
    print(f"{'Memory Allocated (Bytes)':<30} | {f'{set_memory_bytes:,} B':<16} | {f'{hll_memory_bytes:,} B':<16}")
    print(f"{'Memory Allocated (KB)':<30} | {f'{set_kb:.2f} KB':<16} | {f'{hll_kb:.2f} KB':<16}")
    print("-----------------------------------------------------------------")
    print(f"🎉 Memory Savings: HyperLogLog uses {savings_factor:.1f}x LESS memory than a Redis Set!")
    print(f"Standard HLL theoretical error bound is ~0.81%; observed error: {relative_error_pct:.2f}%.")
    print("=" * 65)

    # Clean up benchmark keys
    redis_client.delete(SET_KEY, HLL_KEY)


if __name__ == "__main__":
    run_memory_comparison()

