"""
Webhook handlers for GHL opportunity sync (GHL → our DB only).
Only syncs opportunities from the HMG pipeline (see GHL_HMG_PIPELINE_NAME).
"""
import logging
import time
from typing import Optional

from django.conf import settings
from django.db import OperationalError, connection
from django.utils import timezone

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


def _parse_dt(val):
    """Parse datetime from string, return None if invalid."""
    if not val:
        return None
    try:
        from django.utils.dateparse import parse_datetime
        return parse_datetime(val) if isinstance(val, str) else val
    except (TypeError, ValueError):
        return None


def _upsert_opportunity_report(opportunity_id: str, location: GHLLocation, raw_data: dict):
    """
    Upsert into existing opportunity_report table (RDS) without altering it.
    Maps GHL opportunity data to matching columns.
    """
    data = raw_data if isinstance(raw_data, dict) else {}
    opp_obj = data.get('opportunity') if isinstance(data.get('opportunity'), dict) else data
    contact = (opp_obj.get('contact') or {}) if isinstance(opp_obj.get('contact'), dict) else {}
    contact_name = (
        contact.get('name')
        or (f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip() or '')
        or opp_obj.get('contactName', '')
        or opp_obj.get('contactId', '')
    ) or ''
    emails = contact.get('emails') or []
    phones = contact.get('phones') or []
    email = contact.get('email') or (emails[0].get('email') if emails and isinstance(emails[0], dict) else '') or ''
    phone_val = contact.get('phone') or (phones[0].get('phone') if phones and isinstance(phones[0], dict) else '') or ''
    company_name = contact.get('companyName') or contact.get('company') or ''
    created_at = _parse_dt(opp_obj.get('dateAdded'))
    updated_at = _parse_dt(opp_obj.get('dateUpdated')) or timezone.now()
    last_status_change = _parse_dt(opp_obj.get('lastStatusChangeAt'))
    last_stage_change = _parse_dt(opp_obj.get('lastStageChangeAt'))
    assigned_to = opp_obj.get('assignedTo') or ''
    lost_reason_id = opp_obj.get('lostReasonId') or ''
    monetary = opp_obj.get('monetaryValue')
    if monetary is not None:
        try:
            monetary = int(monetary)  # opportunity_report.monetary_value is BIGINT
        except (TypeError, ValueError):
            monetary = None
    created_date_val = created_at or updated_at

    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO opportunity_report (
                opportunity_id, pipeline_id, pipeline_stage_id, assigned_to,
                contact_id, location_id, lost_reason_id, opportunity_name,
                monetary_value, status, source, last_status_change_at,
                last_stage_change_at, created_at, updated_at, created_date,
                contact_name, email, phone, company_name
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (opportunity_id) DO UPDATE SET
                pipeline_id = EXCLUDED.pipeline_id,
                pipeline_stage_id = EXCLUDED.pipeline_stage_id,
                assigned_to = EXCLUDED.assigned_to,
                contact_id = EXCLUDED.contact_id,
                location_id = EXCLUDED.location_id,
                lost_reason_id = EXCLUDED.lost_reason_id,
                opportunity_name = EXCLUDED.opportunity_name,
                monetary_value = EXCLUDED.monetary_value,
                status = EXCLUDED.status,
                source = EXCLUDED.source,
                last_status_change_at = EXCLUDED.last_status_change_at,
                last_stage_change_at = EXCLUDED.last_stage_change_at,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                created_date = EXCLUDED.created_date,
                contact_name = EXCLUDED.contact_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                company_name = EXCLUDED.company_name
        """, [
            opportunity_id,
            opp_obj.get('pipelineId') or '',
            opp_obj.get('pipelineStageId') or '',
            assigned_to,
            opp_obj.get('contactId') or '',
            location.location_id,
            lost_reason_id,
            opp_obj.get('name') or '',
            monetary,
            opp_obj.get('status') or '',
            opp_obj.get('source') or '',
            last_status_change,
            last_stage_change,
            created_at,
            updated_at,
            created_date_val,
            contact_name,
            email,
            phone_val,
            company_name,
        ])


def _db_delete_opportunity(opportunity_id: str, max_retries: int = 3):
    """Delete opportunity from GHLOpportunity and opportunity_report (RDS)."""
    for attempt in range(max_retries):
        try:
            GHLOpportunity.objects.filter(opportunity_id=opportunity_id).delete()
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM opportunity_report WHERE opportunity_id = %s", [opportunity_id])
            return
        except OperationalError as exc:
            if 'locked' in str(exc).lower() and attempt < max_retries - 1:
                time.sleep(0.3 * (attempt + 1))
            else:
                raise


def _db_update_or_create_opportunity(opportunity_id: str, location: GHLLocation, raw_data: dict, max_retries: int = 3):
    """Update or create in GHLOpportunity and upsert into opportunity_report (RDS)."""
    data = raw_data if isinstance(raw_data, dict) else {}
    for attempt in range(max_retries):
        try:
            GHLOpportunity.objects.update_or_create(
                opportunity_id=opportunity_id,
                defaults={'location': location, 'raw_data': data},
            )
            _upsert_opportunity_report(opportunity_id, location, data)
            return
        except OperationalError as exc:
            if 'locked' in str(exc).lower() and attempt < max_retries - 1:
                time.sleep(0.3 * (attempt + 1))
            else:
                raise
