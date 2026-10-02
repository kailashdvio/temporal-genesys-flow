from datetime import timedelta
from enum import IntEnum
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from app.config.constants import SOURCE_TYPES
from app.utils.date_utils import get_past_n_days_timestamps

with workflow.unsafe.imports_passed_through():
    from app.temporal.genesys.activities import (
        get_analytics_conversations,
        load_analytics_conversations_to_snowflake,
        get_outbound_campaigns,
        load_outbound_campaigns_to_snowflake,
        get_outbound_campaign_stats,
        load_outbound_campaign_stats_to_snowflake,
    )
    from app.temporal.shared_activities import (
        capture_exception_activity,
        check_connection_status_activity,
        create_semantic_view_activity,
        is_auth_error_message,
        update_sync_status_activity,
    )


_SEMANTIC_VIEW_RETRY = RetryPolicy(maximum_attempts=1)
_ACTIVITY_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=60),
    maximum_attempts=5,
)


class SyncDuration(IntEnum):
    DEFAULT_HISTORIC_SYNC_DURATION = 29
    ROLLING_LOOPBACK_WINDOW_DURATION = 4


async def _run_genesys_sync(params, team_id, now):
    source_name = SOURCE_TYPES.get("GENESYS", "genesys")
    try:
        # 1. Sync Analytics Conversations (which contain talkTime, handleTime, latency, dials, wrap-up codes)
        conversations = await workflow.execute_activity(
            get_analytics_conversations,
            params,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=_ACTIVITY_RETRY,
        )
        if conversations:
            await workflow.execute_activity(
                load_analytics_conversations_to_snowflake,
                args=[team_id, conversations],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=_ACTIVITY_RETRY,
            )

        # 2. Sync Outbound Campaigns (contains status/names of dialer campaigns)
        campaigns = await workflow.execute_activity(
            get_outbound_campaigns,
            params,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=_ACTIVITY_RETRY,
        )
        if campaigns:
            await workflow.execute_activity(
                load_outbound_campaigns_to_snowflake,
                args=[team_id, campaigns],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=_ACTIVITY_RETRY,
            )

            # Extract Campaign IDs to fetch stats
            campaign_ids = [c["id"] for c in campaigns if "id" in c]
            if campaign_ids:
                # 3. Sync Campaign Stats
                stats = await workflow.execute_activity(
                    get_outbound_campaign_stats,
                    args=[params, campaign_ids],
                    start_to_close_timeout=timedelta(minutes=10),
                    retry_policy=_ACTIVITY_RETRY,
                )
                if stats:
                    await workflow.execute_activity(
                        load_outbound_campaign_stats_to_snowflake,
                        args=[team_id, stats],
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=_ACTIVITY_RETRY,
                    )

        await workflow.execute_activity(
            update_sync_status_activity,
            {
                "team_id": team_id,
                "status": "synced",
                "last_synced_on": now,
                "source_name": source_name,
            },
            start_to_close_timeout=timedelta(seconds=30),
        )

    except Exception as e:
        is_auth_error = is_auth_error_message(str(e))
        await workflow.execute_activity(
            update_sync_status_activity,
            {
                "team_id": team_id,
                "status": "failed",
                "last_synced_on": now,
                "source_name": source_name,
                "is_auth_error": is_auth_error,
            },
            start_to_close_timeout=timedelta(seconds=30),
        )

        await workflow.execute_activity(
            capture_exception_activity,
            str(e),
            start_to_close_timeout=timedelta(seconds=30),
        )
        raise e


@workflow.defn
class GenesysHistoricalWorkflow:
    @workflow.run
    async def run(self, params: dict[str, Any]) -> None:
        team_id = params["team_id"]

        now = workflow.now()

        if "duration" in params:
            until_date, since_date = get_past_n_days_timestamps(now, params["duration"])
        else:
            until_date, since_date = get_past_n_days_timestamps(
                now, SyncDuration.DEFAULT_HISTORIC_SYNC_DURATION
            )

        params["until_date"] = until_date
        params["since_date"] = since_date

        await _run_genesys_sync(params, team_id, now)

        await workflow.execute_activity(
            create_semantic_view_activity,
            args=[{"team_id": team_id}],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=_SEMANTIC_VIEW_RETRY,
        )

        return


@workflow.defn
class GenesysScheduleWorkflow:
    @workflow.run
    async def run(self, params: dict[str, Any]) -> None:
        team_id = params["team_id"]
        source_name = SOURCE_TYPES.get("GENESYS", "genesys")

        now = workflow.now()
        until_date, since_date = get_past_n_days_timestamps(
            now, SyncDuration.ROLLING_LOOPBACK_WINDOW_DURATION
        )

        params["until_date"] = until_date
        params["since_date"] = since_date

        is_synced = await workflow.execute_activity(
            check_connection_status_activity,
            {"team_id": team_id, "source_name": source_name},
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        if not is_synced:
            msg = f"Skipping scheduled run: Genesys connection status is not synced for team {team_id}."
            workflow.logger.info(msg)
            raise ApplicationError(msg)

        await _run_genesys_sync(params, team_id, now)

        return
