# Re-export shared redis_client for convenience
try:
    from app.redis_client import redis_client
except ImportError:
    from redis_client import redis_client

