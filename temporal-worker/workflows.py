"""
Temporal workflow definitions for Snowplow events
"""
from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
import activities


@workflow.defn
class ProcessSnowplowEventWorkflow:
    """
    Workflow to process enriched Snowplow events from Kafka
    and store data to Redis by clid
    """

    @workflow.run
    async def run(self, event_data: dict) -> dict:
        """
        Main workflow execution

        Args:
            event_data: Parsed Snowplow event data
                {
                    "clid": "test-clid-123",
                    "last_event": "page_view",
                    "os": "Mac OS",
                    "country": "",
                    "last_seen": "2026-05-22 10:54:14"
                }

        Returns:
            dict: Workflow execution result
        """
        clid = event_data.get('clid', 'unknown')
        workflow.logger.info(f"🚀 Starting workflow for clid: {clid}")

        result = await workflow.execute_activity(
            activities.store_snowplow_event,
            args=[event_data],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                backoff_coefficient=2.0
            )
        )

        workflow.logger.info(f"✅ Workflow completed for clid: {clid}")

        return result
