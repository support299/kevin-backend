"""
Webhook handlers for GHL opportunity sync (GHL → our DB only).
Only syncs opportunities from the HMG pipeline (see GHL_HMG_PIPELINE_NAME).
"""
import logging
import time
from typing import Optional

from django.conf import settings
from django.db import OperationalError

from .models import GHLLocation, GHLOpportunity
from .services import GHLClient

logger = logging.getLogger(__name__)


def _is_hmg_pipeline_opportunity(location_id: str, pipeline_id: Optional[str]) -> bool:
    """Return True if pipeline_id matches HMG pipeline, or if HMG filter is disabled."""
    if not pipeline_id:
        return False
    name = getattr(settings, 'GHL_HMG_PIPELINE_NAME', 'HMG') or ''
    if not name:
        return True  # No filter = sync all
    client = GHLClient(location_id=location_id)
    hmg_id = client.get_hmg_pipeline_id()
    return hmg_id is not None and hmg_id == pipeline_id


def _get_pipeline_id_from_opportunity(opp_data: dict) -> Optional[str]:
    """Extract pipelineId from opportunity (handles nested opportunity wrapper)."""
    opp_obj = opp_data.get('opportunity') if isinstance(opp_data.get('opportunity'), dict) else opp_data
    return opp_obj.get('pipelineId') if isinstance(opp_obj, dict) else None


def process_opportunity_webhook(event_type: str, location_id: str, opportunity_id: str):
    """Process opportunity webhook: fetch from GHL then create/update or delete in DB (HMG pipeline only)."""
    if event_type == 'OpportunityDelete':
        _handle_opportunity_delete(location_id, opportunity_id)
    elif event_type in ('OpportunityUpdate', 'OpportunityCreate', 'OpportunityAdded'):
        _fetch_and_store_opportunity(location_id, opportunity_id)
    # Unrecognized event_type: no-op


def _handle_opportunity_delete(location_id: str, opportunity_id: str):
    """
    On delete: try to fetch opportunity from GHL. If 404, delete from our DB only if it was HMG pipeline.
    For delete we check our local record's pipeline (GHL may already have removed the opp).
    """
    try:
        location = GHLLocation.objects.get(location_id=location_id, status='active')
    except GHLLocation.DoesNotExist:
        logger.warning("Location %s not found or inactive, skipping delete sync", location_id)
        return

    # Check if we have this opportunity locally and if it belongs to HMG pipeline
    try:
        local_opp = GHLOpportunity.objects.get(opportunity_id=opportunity_id)
        pipeline_id = _get_pipeline_id_from_opportunity(local_opp.raw_data or {})
        if not _is_hmg_pipeline_opportunity(location_id, pipeline_id):
            logger.info("Opportunity %s not in HMG pipeline, skipping delete", opportunity_id)
            return
    except GHLOpportunity.DoesNotExist:
        # Not in our DB (e.g. never stored because not HMG) – nothing to delete
        return

    client = GHLClient(location_id=location_id)
    fetched = client.get_opportunity_or_none(opportunity_id)

    if fetched is None:
        _db_delete_opportunity(opportunity_id)
        logger.info("Opportunity %s not found in GHL (deleted), removed from local DB", opportunity_id)


def _fetch_and_store_opportunity(location_id: str, opportunity_id: str):
    """
    Fetch full opportunity from GHL API and upsert to GHLOpportunity (only if HMG pipeline).
    """
    try:
        location = GHLLocation.objects.get(location_id=location_id, status='active')
    except GHLLocation.DoesNotExist:
        logger.warning("Location %s not found or inactive, skipping opportunity fetch", location_id)
        return

    client = GHLClient(location_id=location_id)
    try:
        full_opportunity = client.get_opportunity(opportunity_id)
    except Exception as exc:
        logger.error("GHL API error fetching opportunity %s: %s", opportunity_id, exc)
        raise

    pipeline_id = _get_pipeline_id_from_opportunity(full_opportunity)
    if not _is_hmg_pipeline_opportunity(location_id, pipeline_id):
        logger.info("Opportunity %s not in HMG pipeline (pipeline_id=%s), skipping store", opportunity_id, pipeline_id)
        return

    _db_update_or_create_opportunity(opportunity_id, location, full_opportunity)
    logger.info("Stored opportunity %s for location %s (HMG pipeline)", opportunity_id, location_id)


def _db_delete_opportunity(opportunity_id: str, max_retries: int = 3):
    """Delete opportunity from DB with retry for SQLite 'database is locked'."""
    for attempt in range(max_retries):
        try:
            GHLOpportunity.objects.filter(opportunity_id=opportunity_id).delete()
            return
        except OperationalError as exc:
            if 'locked' in str(exc).lower() and attempt < max_retries - 1:
                time.sleep(0.3 * (attempt + 1))
            else:
                raise


def _db_update_or_create_opportunity(opportunity_id: str, location: GHLLocation, raw_data: dict, max_retries: int = 3):
    """Update or create opportunity in DB with retry for SQLite 'database is locked'."""
    data = raw_data if isinstance(raw_data, dict) else {}
    for attempt in range(max_retries):
        try:
            GHLOpportunity.objects.update_or_create(
                opportunity_id=opportunity_id,
                defaults={'location': location, 'raw_data': data},
            )
            return
        except OperationalError as exc:
            if 'locked' in str(exc).lower() and attempt < max_retries - 1:
                time.sleep(0.3 * (attempt + 1))
            else:
                raise
