"""
Temporal worker with embedded Kafka consumer
Processes enriched Snowplow events from Kafka
and stores data to Redis by clid
"""
import asyncio
import logging
import json
import os
import re
from aiokafka import AIOKafkaConsumer
from temporalio.client import Client
from temporalio.worker import Worker
from datetime import timedelta
import workflows
import activities

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

#### Environment variables
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka1:9092,kafka2:9095')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'snowplow_enriched_good')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'temporal-snowplow-consumer')
TEMPORAL_ADDRESS = os.getenv('TEMPORAL_ADDRESS', 'temporal:7233')
TEMPORAL_NAMESPACE = os.getenv('TEMPORAL_NAMESPACE', 'default')
TASK_QUEUE = os.getenv('TASK_QUEUE', 'snowplow-events')


def parse_snowplow_event(raw_string: str) -> dict | None:
    """
    Parse enriched Snowplow TSV event and extract required fields

    TSV field positions (1-based in SQL, 0-based in Python):
    [5]  → event (SQL [6])
    [18] → country_code (SQL [19])
    [2]  → etl_tstamp timestamp (SQL [3])
    [52] → contexts JSON (SQL [53]) - contains clid
    [122]→ derived_contexts JSON (SQL [123]) - contains os_name
    """
    try:
        fields = raw_string.split('\t')

        #### Extract basic fields
        event      = fields[5]  if len(fields) > 5   else ''
        timestamp  = fields[2]  if len(fields) > 2   else ''
        country    = fields[18] if len(fields) > 18  else ''

        #### Extract clid from contexts JSON (field 52, 0-based)
        clid = ''
        if len(fields) > 52:
            contexts_raw = fields[52]
            try:
                contexts = json.loads(contexts_raw)
                for ctx in contexts.get('data', []):
                    if 'user_context' in ctx.get('schema', ''):
                        clid = ctx.get('data', {}).get('clid', '')
                        break
            except (json.JSONDecodeError, AttributeError):
                pass

        #### Skip events without clid
        if not clid:
            return None

        #### Extract os_name from derived_contexts (field 122, 0-based)
        os_name = ''
        if len(fields) > 122:
            derived_raw = fields[122]
            try:
                #### Extract operatingSystemName using regex for performance
                match = re.search(r'"operatingSystemName"\s*:\s*"([^"]*)"', derived_raw)
                if match:
                    os_name = match.group(1)
            except Exception:
                pass

        return {
            'clid':       clid,
            'last_event': event,
            'os':         os_name,
            'country':    country,
            'last_seen':  timestamp
        }

    except Exception as e:
        logger.error(f"❌ Failed to parse Snowplow event: {e}")
        return None


async def consume_and_trigger_workflows(temporal_client: Client):
    """
    Consume Kafka messages and start Temporal workflows
    """
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=KAFKA_CONSUMER_GROUP,
        auto_offset_reset='latest',
        session_timeout_ms=45000,
        heartbeat_interval_ms=15000,
        max_poll_interval_ms=900000,
        enable_auto_commit=False,
        max_poll_records=50
    )

    await consumer.start()
    logger.info("=" * 80)
    logger.info("📥 Kafka Consumer Started")
    logger.info("=" * 80)
    logger.info(f"📌 Kafka Brokers: {KAFKA_BROKERS}")
    logger.info(f"📌 Topic: {KAFKA_TOPIC}")
    logger.info(f"📌 Consumer Group: {KAFKA_CONSUMER_GROUP}")
    logger.info(f"📌 Task Queue: {TASK_QUEUE}")
    logger.info("=" * 80)

    try:
        async for message in consumer:
            try:
                raw_string = message.value.decode('utf-8')

                logger.info(f"📨 Received message (offset: {message.offset})")

                event_data = parse_snowplow_event(raw_string)

                if event_data is None:
                    logger.debug("⏭️ Skipping event - no clid found")
                    await consumer.commit()
                    continue

                logger.info(f"🎯 Event with clid detected: {event_data['clid']}")

                workflow_id = f"snowplow-{event_data['clid']}-{message.partition}-{message.offset}"

                await temporal_client.start_workflow(
                    workflows.ProcessSnowplowEventWorkflow.run,
                    args=[event_data],
                    id=workflow_id,
                    task_queue=TASK_QUEUE,
                    execution_timeout=timedelta(hours=1)
                )

                await consumer.commit()

                logger.info(f"✅ Workflow started: {workflow_id}")
                logger.info("-" * 80)

            except Exception as e:
                logger.error(f"❌ Failed to process message: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"❌ Error in Kafka consumer: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logger.info("🛑 Kafka consumer stopped")


async def run_temporal_worker(temporal_client: Client):
    """
    Run Temporal worker to execute workflows
    """
    logger.info("🏭 Starting Temporal Worker...")

    worker = Worker(
        temporal_client,
        task_queue=TASK_QUEUE,
        workflows=[
            workflows.ProcessSnowplowEventWorkflow,
        ],
        activities=[
            activities.store_snowplow_event,
        ]
    )

    logger.info(f"✅ Worker ready on task queue: {TASK_QUEUE}")
    logger.info("⏳ Waiting for workflows to execute...")

    await worker.run()


async def main():
    """
    Main entry point - runs both consumer and worker concurrently
    """
    logger.info("=" * 80)
    logger.info("🚀 Starting Snowplow Temporal Worker")
    logger.info("=" * 80)
    logger.info(f"📌 Temporal Address: {TEMPORAL_ADDRESS}")
    logger.info(f"📌 Namespace: {TEMPORAL_NAMESPACE}")
    logger.info(f"📌 Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"📌 Task Queue: {TASK_QUEUE}")
    logger.info("=" * 80)

    try:
        temporal_client = await Client.connect(
            TEMPORAL_ADDRESS,
            namespace=TEMPORAL_NAMESPACE
        )

        logger.info("✅ Connected to Temporal server")

        await asyncio.gather(
            consume_and_trigger_workflows(temporal_client),
            run_temporal_worker(temporal_client)
        )

    except KeyboardInterrupt:
        logger.info("⛔ Shutting down gracefully...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
