"""
Celery tasks for GHL integration.
"""
import logging

logger = logging.getLogger(__name__)


def get_shared_task():
    """Lazy import to avoid requiring Celery at import time."""
    try:
        from celery import shared_task
        return shared_task
    except ImportError:
        return None


def refresh_ghl_tokens_task():
    """
    Periodic task to refresh OAuth tokens for all active GHL locations.
    Run via Celery Beat (e.g., every hour).
    """
    shared_task = get_shared_task()
    if not shared_task:
        logger.warning("Celery not installed; token refresh task skipped")
        return {'refreshed': 0, 'failed': 0, 'total_checked': 0}

    from .models import GHLLocation
    from .services import GHLClient

    try:
        from django.utils import timezone
    except ImportError:
        return {'error': 'Django not available'}

    active_locations = GHLLocation.objects.filter(
        status='active',
        refresh_token__isnull=False
    ).exclude(refresh_token='')

    refreshed_count = 0
    failed_count = 0

    for location in active_locations:
        try:
            if location.needs_token_refresh():
                logger.info("Refreshing token for location %s", location.location_id)
                client = GHLClient(location_id=location.location_id)
                client._get_location()
                refreshed_count += 1
                logger.info("Successfully refreshed token for location %s", location.location_id)
            else:
                logger.debug("Token for location %s is still valid, skipping refresh", location.location_id)
        except Exception as exc:
            logger.error(
                "Failed to refresh token for location %s: %s",
                location.location_id, exc, exc_info=True
            )
            failed_count += 1

    logger.info(
        "Token refresh completed: %d refreshed, %d failed, %d skipped",
        refreshed_count, failed_count, active_locations.count() - refreshed_count - failed_count
    )

    return {
        'refreshed': refreshed_count,
        'failed': failed_count,
        'total_checked': active_locations.count()
    }


# Register as Celery task if Celery is available
try:
    from celery import shared_task

    @shared_task
    def refresh_ghl_tokens():
        """Celery task wrapper for refresh_ghl_tokens_task."""
        return refresh_ghl_tokens_task()

    @shared_task
    def process_opportunity_webhook_task(event_type: str, location_id: str, opportunity_id: str):
        """
        Process opportunity webhook in background (avoids SQLite DB locks on burst).
        """
        from .webhook_handlers import process_opportunity_webhook
        try:
            process_opportunity_webhook(event_type, location_id, opportunity_id)
        except Exception as exc:
            logger.error(
                "Failed to process opportunity webhook %s for %s: %s",
                event_type, opportunity_id, exc, exc_info=True
            )
            raise

    @shared_task
    def process_contact_webhook_task(event_type: str, location_id: str, contact_id: str):
        """
        Process contact webhook in background (avoids DB locks on burst).
        """
        from .webhook_handlers import process_contact_webhook
        try:
            process_contact_webhook(event_type, location_id, contact_id)
        except Exception as exc:
            logger.error(
                "Failed to process contact webhook %s for %s: %s",
                event_type, contact_id, exc, exc_info=True
            )
            raise

except ImportError:
    refresh_ghl_tokens = None
    process_opportunity_webhook_task = None
    process_contact_webhook_task = None

