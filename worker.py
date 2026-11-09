"""
RedAlert Standalone Dispatch Worker
Phase 5 & 6: Consumer group worker processing incidents from 'incidents:stream' via XREADGROUP.
Phase 6 adds safe unit assignment strategies to prevent race conditions when competing workers
process incidents concurrently.

Assignment modes supported via ASSIGN_MODE (env var or CLI flag):
1. 'unsafe': (Baseline) Checks SISMEMBER units:free, then updates status without atomic locking.
2. 'lock':   Distributed lock per unit with SET lock:unit:{id} <token> NX PX 5000 and Lua unlock.
3. 'watch':  Optimistic locking using WATCH unit:{id} units:free, then MULTI/EXEC SMOVE + HSET.
4. 'lua':    Atomic Lua script choosing first free candidate, SMOVEing, and updating hashes in 1 shot.
"""

import argparse
import json
import os
import random
import sys
import threading
import time
import uuid
from typing import Dict, Any, Optional, Tuple, List

try:
    from app.redis_client import redis_client
except ImportError:
    try:
        from dispatchx.app.redis_client import redis_client
    except ImportError:
        import redis
        redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

STREAM_NAME = "incidents:stream"
GROUP_NAME = "dispatchers"
SEARCH_RADIUS_KM = 25.0
AUTOCLAIM_INTERVAL_SEC = 10
MIN_IDLE_TIME_MS = 15000

# Global assignment mode setting (can be overridden via CLI --mode or env ASSIGN_MODE)
ASSIGN_MODE = os.getenv("ASSIGN_MODE", "unsafe").lower()

stop_event = threading.Event()

# ---------------------------------------------------------------------------
# LUA SCRIPTS
# ---------------------------------------------------------------------------

# Lua script to release lock only if the token matches (prevents releasing someone else's lock)
# KEYS[1]: lock:unit:{id}
# ARGV[1]: lock_token
LUA_RELEASE_LOCK = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""
release_lock_script = redis_client.register_script(LUA_RELEASE_LOCK)

# Lua script for atomic candidate assignment
# KEYS: none required (or pass dummy), candidate unit IDs passed in ARGV
# ARGV[1]: incident_id
# ARGV[2]: dispatched_timestamp
# ARGV[3..N]: candidate unit_ids in order of proximity (e.g. "unit-1", "unit-2")
# Returns: chosen unit_id (string) or nil if none available
LUA_ASSIGN_CANDIDATE = """
local incident_id = ARGV[1]
local dispatched_at = ARGV[2]
local incident_key = "incident:" .. incident_id

-- Idempotency check: if incident already assigned, return nil (or already handled)
local existing_unit = redis.call("hget", incident_key, "assigned_unit")
if existing_unit and existing_unit ~= "" then
    return existing_unit
end

-- Iterate through candidates ordered by distance
for i = 3, #ARGV do
    local unit_id = ARGV[i]
    -- Check if unit is currently in units:free
    if redis.call("sismember", "units:free", unit_id) == 1 then
        -- Atomically move unit from units:free to units:busy
        redis.call("smove", "units:free", "units:busy", unit_id)
        
        -- Update unit hash
        local unit_key = "unit:" .. unit_id
        redis.call("hset", unit_key, "status", "busy", "current_incident", incident_key)
        
        -- Update incident hash
        redis.call("hset", incident_key, "assigned_unit", unit_id, "status", "dispatched", "dispatched_at", dispatched_at)
        
        return unit_id
    end
end

return nil
"""
assign_candidate_script = redis_client.register_script(LUA_ASSIGN_CANDIDATE)


def ensure_consumer_group():
    """
    Ensures the consumer group exists on the stream.
    Redis Command: XGROUP CREATE incidents:stream dispatchers $ MKSTREAM
    """
    try:
        redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="$", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            pass  # Consumer group already exists
        else:
            raise exc


# ---------------------------------------------------------------------------
# ASSIGNMENT STRATEGIES
# ---------------------------------------------------------------------------

def assign_unsafe(incident_id: str, candidates: List[Tuple[str, float]]) -> Optional[Tuple[str, float]]:
    """
    Mode 'unsafe': Checks membership and updates separately.
    Why Unsafe:
    There is a race window between checking if a unit is in units:free and marking it busy.
    Two competing workers can both see unit A as free, both believe they own it,
    and both dispatch it to separate incidents, resulting in double-assignment.
    """
    incident_key = f"incident:{incident_id}"
    dispatched_timestamp = str(time.time())

    for member, distance in candidates:
        raw_unit_id = member.split("unit:", 1)[1] if member.startswith("unit:") else member

        # Non-atomic check: another worker could also pass this check at the same time
        if redis_client.sismember("units:free", raw_unit_id):
            # Interleaving delay in unsafe mode allows concurrent workers to race
            time.sleep(0.005)

            # Move between sets and update unit & incident hashes non-atomically
            redis_client.srem("units:free", raw_unit_id)
            redis_client.sadd("units:busy", raw_unit_id)
            redis_client.hset(f"unit:{raw_unit_id}", mapping={"status": "busy", "current_incident": incident_key})
            redis_client.hset(incident_key, mapping={"assigned_unit": raw_unit_id, "status": "dispatched", "dispatched_at": dispatched_timestamp})
            return raw_unit_id, float(distance)

    return None


def assign_lock(incident_id: str, candidates: List[Tuple[str, float]]) -> Optional[Tuple[str, float]]:
    """
    Mode 'lock': Distributed lock pattern (SET lock:unit:{id} token NX PX 5000).
    Why Safe:
    SET with NX (not exists) is atomic: exactly one worker obtains the mutex lock for a given unit.
    The losing workers fail to acquire the lock and seamlessly move to the next nearest candidate.
    Locks are released via Lua script matching the unique token to prevent unlocking another worker's lock.
    """
    incident_key = f"incident:{incident_id}"
    dispatched_timestamp = str(time.time())

    for member, distance in candidates:
        raw_unit_id = member.split("unit:", 1)[1] if member.startswith("unit:") else member
        lock_key = f"lock:unit:{raw_unit_id}"
        lock_token = str(uuid.uuid4())

        # Redis Command: SET lock:unit:{id} <token> NX PX 5000
        # Why: Atomically acquires exclusive lock with 5-second TTL auto-expiry if worker crashes
        acquired = redis_client.set(lock_key, lock_token, nx=True, px=5000)
        if not acquired:
            # Another worker is currently evaluating or claiming this unit; try next candidate
            continue

        try:
            # Under lock: verify the unit is still in units:free
            if redis_client.sismember("units:free", raw_unit_id):
                redis_client.smove("units:free", "units:busy", raw_unit_id)
                redis_client.hset(f"unit:{raw_unit_id}", mapping={"status": "busy", "current_incident": incident_key})
                redis_client.hset(incident_key, mapping={"assigned_unit": raw_unit_id, "status": "dispatched", "dispatched_at": dispatched_timestamp})
                return raw_unit_id, float(distance)
        finally:
            # Release lock via Lua script: delete only if token matches
            try:
                release_lock_script(keys=[lock_key], args=[lock_token])
            except Exception:
                pass

    return None


def assign_watch(incident_id: str, candidates: List[Tuple[str, float]]) -> Optional[Tuple[str, float]]:
    """
    Mode 'watch': Optimistic concurrency with WATCH / MULTI / EXEC.
    Why Safe:
    WATCH monitors unit:{id} and units:free for changes. If another worker modifies either key
    before this worker's MULTI/EXEC commits, Redis aborts the transaction with WatchError.
    The worker catches WatchError and retries cleanly with the next nearest candidate.
    """
    incident_key = f"incident:{incident_id}"
    dispatched_timestamp = str(time.time())

    for member, distance in candidates:
        raw_unit_id = member.split("unit:", 1)[1] if member.startswith("unit:") else member
        unit_key = f"unit:{raw_unit_id}"

        pipe = redis_client.pipeline()
        try:
            # Redis Command: WATCH unit:{id} units:free
            # Why: Aborts EXEC if any other client alters these keys before execution
            pipe.watch(unit_key, "units:free")

            # Check if unit is still in units:free
            is_free = redis_client.sismember("units:free", raw_unit_id)
            if not is_free:
                pipe.unwatch()
                continue

            # Queue state changes in transaction
            pipe.multi()
            pipe.smove("units:free", "units:busy", raw_unit_id)
            pipe.hset(unit_key, mapping={"status": "busy", "current_incident": incident_key})
            pipe.hset(incident_key, mapping={"assigned_unit": raw_unit_id, "status": "dispatched", "dispatched_at": dispatched_timestamp})
            
            # Redis Command: EXEC
            pipe.execute()
            return raw_unit_id, float(distance)
        except redis.WatchError:
            # Concurrently claimed by another worker; move to next candidate
            continue
        finally:
            pipe.reset()

    return None


def assign_lua(incident_id: str, candidates: List[Tuple[str, float]]) -> Optional[Tuple[str, float]]:
    """
    Mode 'lua': Atomic execution directly on the Redis server thread.
    Why Safe:
    Redis executes Lua scripts single-threaded and run-to-completion without interruption.
    Checking availability, SMOVEing, and updating hashes all occur within 1 atomic operation.
    No other command or worker can interleave, eliminating race conditions entirely with zero lock overhead.
    """
    dispatched_timestamp = str(time.time())
    raw_candidates = [
        m.split("unit:", 1)[1] if m.startswith("unit:") else m
        for m, _ in candidates
    ]

    if not raw_candidates:
        return None

    # Call registered Lua script
    # ARGV: [incident_id, timestamp, candidate_1, candidate_2, ...]
    args = [incident_id, dispatched_timestamp] + raw_candidates
    chosen_unit = assign_candidate_script(keys=[], args=args)

    if chosen_unit:
        # Find matching distance for telemetry reporting
        distance = next(
            (float(d) for m, d in candidates if (m == chosen_unit or m == f"unit:{chosen_unit}")),
            0.0
        )
        return chosen_unit, distance

    return None


def process_incident_message(worker_name: str, message_id: str, fields: Dict[str, Any], mode: Optional[str] = None) -> bool:
    """
    Processes an incident message from the stream:
    1. Checks idempotency: if incident:{id} already has assigned_unit, XACK and skip.
    2. Uses GEOSEARCH to find nearest units.
    3. Executes chosen assignment strategy ('unsafe', 'lock', 'watch', 'lua').
    4. Broadcasts dispatch telemetry to Pub/Sub channels.
    5. Acknowledges message with XACK.
    """
    active_mode = (mode or ASSIGN_MODE).lower()
    incident_id = fields.get("incident_id")
    incident_type = fields.get("type", "Emergency")
    lat_str = fields.get("lat")
    lon_str = fields.get("lon")
    severity = fields.get("severity", "5")

    if not incident_id:
        # Redis Command: XACK incidents:stream dispatchers <id>
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return False

    incident_key = f"incident:{incident_id}"

    # -----------------------------------------------------------------------
    # Requirement 5: Idempotency Protection
    # -----------------------------------------------------------------------
    # Redis Command: HGET incident:{id} assigned_unit
    # Why: If this message was redelivered or claimed by XAUTOCLAIM after recovery,
    # check whether it was already assigned to avoid double dispatching.
    existing_assigned_unit = redis_client.hget(incident_key, "assigned_unit")
    if existing_assigned_unit:
        print(f"[{worker_name}] ⏭️ Incident #{incident_id} already assigned to Unit '{existing_assigned_unit}'. Skipping (idempotent XACK).")
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return True

    if not lat_str or not lon_str:
        print(f"[{worker_name}] ❌ Corrupt message {message_id}: missing coordinates.")
        redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
        return False

    lat = float(lat_str)
    lon = float(lon_str)

    print(f"[{worker_name}] 📨 [{active_mode.upper()}] Incident #{incident_id} [{incident_type} | Sev {severity}] at ({lat:.4f}, {lon:.4f})")

    # Redis Command: GEOSEARCH units:geo FROMLONLAT <lon> <lat> BYRADIUS <radius> km ASC WITHDIST
    # Why: Retrieves nearby units sorted by proximity in ascending order.
    candidates = redis_client.geosearch(
        "units:geo",
        longitude=lon,
        latitude=lat,
        radius=SEARCH_RADIUS_KM,
        unit="km",
        sort="ASC",
        withdist=True,
    )

    assignment: Optional[Tuple[str, float]] = None

    # Dispatch to the selected concurrency control strategy
    if active_mode == "lock":
        assignment = assign_lock(incident_id, candidates)
    elif active_mode == "watch":
        assignment = assign_watch(incident_id, candidates)
    elif active_mode == "lua":
        assignment = assign_lua(incident_id, candidates)
    else:
        # Default: unsafe baseline
        assignment = assign_unsafe(incident_id, candidates)

    if not assignment:
        # No free unit available: DO NOT ACK so message stays pending in PEL for recovery
        print(f"[{worker_name}] ⚠️ No available free units within {SEARCH_RADIUS_KM}km for Incident #{incident_id}. Message left unacked in PEL.")
        return False

    assigned_unit, assigned_distance = assignment
    dispatched_timestamp = time.time()

    # Broadcast update across WebSockets
    # Redis Command: PUBLISH incident:updates <json>
    redis_client.publish(
        "incident:updates",
        json.dumps({
            "event": "dispatched",
            "id": str(incident_id),
            "incident_id": str(incident_id),
            "assigned_unit": assigned_unit,
            "distance_km": round(assigned_distance, 3),
            "worker": worker_name,
            "mode": active_mode,
            "timestamp": dispatched_timestamp,
        }),
    )

    # Broadcast unit status change to unit:updates (marker turns red on dashboard)
    # Redis Command: PUBLISH unit:updates <json>
    pos = redis_client.geopos("units:geo", f"unit:{assigned_unit}")
    u_lon, u_lat = pos[0] if (pos and pos[0] is not None) else (None, None)
    redis_client.publish(
        "unit:updates",
        json.dumps({
            "unit_id": assigned_unit,
            "lat": u_lat,
            "lon": u_lon,
            "status": "busy",
            "current_incident": incident_key,
            "timestamp": dispatched_timestamp,
        }),
    )

    # Redis Command: XACK incidents:stream dispatchers <message_id>
    # Why: Acknowledges completion, purging the entry from PEL.
    redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)

    print(
        f"[{worker_name}] 🚑 Assigned Unit '{assigned_unit}' ({assigned_distance:.2f}km away) "
        f"to Incident #{incident_id} [{active_mode}] | XACK {message_id}"
    )
    return True


def recovery_task(worker_name: str, mode: Optional[str] = None):
    """
    Background recovery thread running every 10 seconds.
    Uses XAUTOCLAIM to take over messages stuck with dead or stalled workers for >15000ms.
    """
    while not stop_event.is_set():
        for _ in range(AUTOCLAIM_INTERVAL_SEC):
            if stop_event.is_set():
                return
            time.sleep(1)

        try:
            # Redis Command: XAUTOCLAIM incidents:stream dispatchers <worker_name> 15000 0-0 COUNT 10
            autoclaim_result = redis_client.xautoclaim(
                STREAM_NAME,
                GROUP_NAME,
                worker_name,
                min_idle_time=MIN_IDLE_TIME_MS,
                start_id="0-0",
                count=10,
            )

            if autoclaim_result and len(autoclaim_result) > 1:
                claimed_messages = autoclaim_result[1]
                if claimed_messages:
                    print(f"[{worker_name}] 🔄 XAUTOCLAIM recovered {len(claimed_messages)} orphaned messages from PEL!")
                    for message_id, fields in claimed_messages:
                        if stop_event.is_set():
                            break
                        process_incident_message(worker_name, message_id, fields, mode=mode)
        except Exception:
            pass


def main():
    global ASSIGN_MODE
    parser = argparse.ArgumentParser(description="RedAlert Stream Dispatcher Worker - Phase 6 Concurrency Safe")
    parser.add_argument("name", nargs="?", default=None, help="Worker identifier (e.g. worker-1)")
    parser.add_argument("--name", dest="name_flag", default=None, help="Worker identifier")
    parser.add_argument(
        "--mode",
        dest="assign_mode",
        choices=["unsafe", "lock", "watch", "lua"],
        default=None,
        help="Assignment concurrency mode: 'unsafe', 'lock', 'watch', or 'lua' (default: env ASSIGN_MODE or 'unsafe')"
    )
    args = parser.parse_args()

    worker_name = args.name_flag or args.name or f"worker-{os.getpid()}"
    if args.assign_mode:
        ASSIGN_MODE = args.assign_mode.lower()

    print(f"======================================================")
    print(f"  🚨 RedAlert Dispatch Worker: {worker_name}")
    print(f"  Stream: {STREAM_NAME} | Group: {GROUP_NAME}")
    print(f"  Assignment Concurrency Mode: {ASSIGN_MODE.upper()}")
    print(f"======================================================")

    ensure_consumer_group()

    recovery_thread = threading.Thread(target=recovery_task, args=(worker_name, ASSIGN_MODE), daemon=True)
    recovery_thread.start()

    print(f"[{worker_name}] 🎧 Listening for stream entries via XREADGROUP (block=5000ms)...")

    try:
        while not stop_event.is_set():
            # Redis Command: XREADGROUP GROUP dispatchers <worker_name> COUNT 1 BLOCK 5000 STREAMS incidents:stream >
            entries = redis_client.xreadgroup(
                GROUP_NAME,
                worker_name,
                {STREAM_NAME: ">"},
                count=1,
                block=5000,
            )

            if entries:
                for stream_name, messages in entries:
                    for message_id, fields in messages:
                        process_incident_message(worker_name, message_id, fields, mode=ASSIGN_MODE)

    except KeyboardInterrupt:
        print(f"\n[{worker_name}] Shutting down worker...")
    finally:
        stop_event.set()
        recovery_thread.join(timeout=2.0)
        print(f"[{worker_name}] Worker stopped cleanly.")


if __name__ == "__main__":
    main()
