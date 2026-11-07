import os
import redis

# Redis Configuration with environment variable fallbacks
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Redis Client: Single shared connection to Redis Stack.
# decode_responses=True ensures that Redis returns string values rather than raw bytes.
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
)
