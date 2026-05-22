"""
Temporal activities for Snowplow event processing
"""
import logging
import os
from temporalio import activity
from datetime import datetime
import redis.asyncio as redis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

#### Environment variables
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
REDIS_NAMESPACE = os.getenv('REDIS_NAMESPACE', 'snowplow')

#### Redis client singleton
_redis_client = None


async def get_redis_client():
    """Get or create Redis client connection"""
    global _redis_client
    if _redis_client is None:
        _redis_client = await redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
    return _redis_client


@activity.defn
async def store_snowplow_event(event_data: dict):
    """
    Store Snowplow event data to Redis hash by clid

    Expected event_data format:
    {
        "clid": "test-clid-123",
        "last_event": "page_view",
        "os": "Mac OS",
        "country": "",
        "last_seen": "2026-05-22 10:54:14"
    }

    Redis key format: {REDIS_NAMESPACE}:{clid}
    """
    activity.logger.info("=" * 80)
    activity.logger.info("🎿 SNOWPLOW EVENT RECEIVED")
    activity.logger.info("=" * 80)

    clid = event_data.get('clid')

    if not clid:
        error_msg = "Missing required field: clid"
        activity.logger.error(f"❌ {error_msg}")
        return {
            "status": "error",
            "error": error_msg
        }

    activity.logger.info(f"📋 CLID: {clid}")
    activity.logger.info(f"📋 Event: {event_data.get('last_event')}")
    activity.logger.info(f"📋 OS: {event_data.get('os')}")
    activity.logger.info(f"📋 Country: {event_data.get('country')}")
    activity.logger.info(f"📋 Last Seen: {event_data.get('last_seen')}")

    try:
        redis_client = await get_redis_client()
        redis_key = f"{REDIS_NAMESPACE}:{clid}"

        await redis_client.hset(
            redis_key,
            mapping={
                'last_event': event_data.get('last_event', ''),
                'os':         event_data.get('os', ''),
                'country':    event_data.get('country', ''),
                'last_seen':  event_data.get('last_seen', '')
            }
        )

        updated_hash = await redis_client.hgetall(redis_key)

        activity.logger.info(f"✅ Redis updated for key: {redis_key}")
        activity.logger.info(f"📦 Current data: {updated_hash}")
        activity.logger.info("=" * 80)

        return {
            "status": "success",
            "clid": clid,
            "redis_key": redis_key,
            "data": updated_hash,
            "processed_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        error_msg = f"Redis operation failed: {str(e)}"
        activity.logger.error(f"❌ {error_msg}", exc_info=True)

        return {
            "status": "error",
            "clid": clid,
            "error": error_msg,
            "processed_at": datetime.utcnow().isoformat()
        }
