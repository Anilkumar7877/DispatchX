import asyncio
import json
import redis.asyncio as aioredis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

try:
    from app.config import settings
except ImportError:
    from dispatchx.app.config import settings

router = APIRouter(tags=["WebSocket"])


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    async_redis = aioredis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD,
        decode_responses=True,
    )
    pubsub = async_redis.pubsub()

    # Redis Command: SUBSCRIBE unit:updates incident:updates
    await pubsub.subscribe("unit:updates", "incident:updates")

    async def forward_pubsub_messages():
        try:
            async for message in pubsub.listen():
                if message and message.get("type") == "message":
                    raw_data = message.get("data")
                    channel = message.get("channel")
                    try:
                        payload = json.loads(raw_data)
                    except Exception:
                        payload = {"data": raw_data}

                    if isinstance(payload, dict) and "channel" not in payload:
                        payload["channel"] = channel

                    await websocket.send_json(payload)
        except Exception:
            pass

    forward_task = asyncio.create_task(forward_pubsub_messages())

    try:
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        forward_task.cancel()
        try:
            await forward_task
        except asyncio.CancelledError:
            pass
        try:
            await pubsub.unsubscribe("unit:updates", "incident:updates")
            await pubsub.aclose()
            await async_redis.aclose()
        except Exception:
            pass

