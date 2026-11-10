import asyncio
import json
import time
import redis.asyncio as aioredis
from redis.commands.search.field import TagField, NumericField
from redis.commands.search.index_definition import IndexDefinition, IndexType

try:
    from app.redis_client import redis_client
    from app.config import settings
except ImportError:
    from dispatchx.app.redis_client import redis_client
    from dispatchx.app.config import settings


def handle_unit_expired(unit_id: str) -> None:
    """
    Handles expiration of a unit's heartbeat key (hb:unit:{id}).
    Marks the unit offline across all data structures and broadcasts the offline event.
    """
    unit_key = f"unit:{unit_id}"
    geo_member = f"unit:{unit_id}"

    # Redis Command: ZREM units:geo unit:{id}
    # Why: Remove the offline ambulance from geospatial indexing so it is omitted from proximity queries.
    redis_client.zrem("units:geo", geo_member)

    # Redis Command: SREM units:free {id} & SREM units:busy {id}
    # Why: An offline ambulance is unavailable; purge it from availability tracking sets.
    redis_client.srem("units:free", unit_id)
    redis_client.srem("units:busy", unit_id)

    # Redis Command: HSET unit:{id} status offline
    # Why: Update the unit hash record to reflect its offline status.
    redis_client.hset(unit_key, "status", "offline")

    # Redis Command: PUBLISH unit:updates <json>
    # Why: Broadcast the offline event so connected WebSockets/dashboards remove the marker in real time.
    offline_msg = {
        "unit_id": unit_id,
        "lat": None,
        "lon": None,
        "status": "offline",
        "timestamp": time.time(),
    }
    redis_client.publish("unit:updates", json.dumps(offline_msg))


async def listen_to_keyspace_expirations() -> None:
    """
    Background worker that listens for Redis keyspace expiry events on __keyevent@0__:expired.
    """
    async_redis = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    pubsub = async_redis.pubsub()

    # Redis Command: SUBSCRIBE __keyevent@0__:expired
    # Why: Redis keyspace notifications emit an event to this channel whenever any key expires.
    await pubsub.subscribe("__keyevent@0__:expired")

    try:
        async for message in pubsub.listen():
            if message and message.get("type") == "message":
                expired_key = message.get("data", "")
                if expired_key.startswith("hb:unit:"):
                    unit_id = expired_key.split("hb:unit:", 1)[1]
                    handle_unit_expired(unit_id)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            await pubsub.unsubscribe("__keyevent@0__:expired")
            await pubsub.aclose()
            await async_redis.aclose()
        except Exception:
            pass


def init_redis_infrastructure() -> None:
    """
    Configures keyspace notifications, consumer groups, and RediSearch schemas on startup.
    """
    # 1. Redis Command: CONFIG SET notify-keyspace-events Ex
    # Why: Enables keyspace notifications specifically for expired events ('x') on keyspace ('E').
    try:
        redis_client.config_set("notify-keyspace-events", "Ex")
    except Exception as exc:
        print(f"Warning setting notify-keyspace-events: {exc}")

    # 2. Redis Command: XGROUP CREATE incidents:stream dispatchers $ MKSTREAM
    # Why: Creates a named consumer group ('dispatchers') on 'incidents:stream' for worker distribution.
    try:
        redis_client.xgroup_create("incidents:stream", "dispatchers", id="$", mkstream=True)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            pass  # Consumer group already exists, ignore
        else:
            raise exc

    # 3. RediSearch Index Creation
    # Redis Command: FT.CREATE idx:incidents ON JSON PREFIX 1 closed:incident: SCHEMA ...
    incidents_schema = (
        TagField("$.type", as_name="type"),
        TagField("$.zone", as_name="zone"),
        NumericField("$.severity", as_name="severity", sortable=True),
        NumericField("$.closed_at", as_name="closed_at", sortable=True),
        NumericField("$.response_secs", as_name="response_secs", sortable=True),
    )
    try:
        redis_client.ft("idx:incidents").create_index(
            incidents_schema,
            definition=IndexDefinition(prefix=["closed:incident:"], index_type=IndexType.JSON),
        )
    except Exception as exc:
        if "already exists" in str(exc).lower() or "busy" in str(exc).lower():
            pass
        else:
            print(f"Warning creating RediSearch index idx:incidents: {exc}")

