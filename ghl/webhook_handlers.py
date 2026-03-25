"""
Webhook handlers for GHL opportunity sync (GHL → our DB only).
Syncs opportunities from allowed pipelines only (v2026 is blocked).
"""

# Pipeline IDs that should NOT be synced or stored in our DB.
# Add any pipeline ID here to block it from future syncing.
BLOCKED_PIPELINE_IDS = {
    'VEA9ftw4r48zYwhq4ltL',  # v2026 pipeline
    '2Dw2997c3HrCqtKGvd28',  # blocked pipeline
}
import logging
import time
from datetime import datetime

from django.db import OperationalError, connection
from django.utils import timezone
from django.utils.timezone import make_aware

# Global sync start date (records created before this will be ignored)
SYNC_START_DATE = make_aware(datetime(2025, 12, 1))

from .models import GHLLocation, GHLOpportunity
from .services import GHLClient
from .custom_fields_utils import sync_and_get_custom_field_values

logger = logging.getLogger(__name__)


def process_opportunity_webhook(event_type: str, location_id: str, opportunity_id: str):
    """Process opportunity webhook: fetch from GHL then create/update or delete in DB (all pipelines)."""
    if event_type == 'OpportunityDelete':
        _handle_opportunity_delete(location_id, opportunity_id)
    elif event_type in (
        'OpportunityUpdate', 'OpportunityCreate', 'OpportunityAdded',
        'OpportunityStageUpdate', 'OpportunityStatusUpdate',
    ):
        # For stage/status updates we do a full re-fetch — the GHL API always
        # returns the latest data, so one fetch covers stage, status, and fields.
        _fetch_and_store_opportunity(location_id, opportunity_id)
    # Unrecognized event_type: no-op


def _handle_opportunity_delete(location_id: str, opportunity_id: str):
    """
    On delete: try to fetch opportunity from GHL. If 404, delete from our DB.
    """
    try:
        GHLLocation.objects.get(location_id=location_id, status='active')
    except GHLLocation.DoesNotExist:
        logger.warning("Location %s not found or inactive, skipping delete sync", location_id)
        return

    client = GHLClient(location_id=location_id)
    fetched = client.get_opportunity_or_none(opportunity_id)

    if fetched is None:
        _db_delete_opportunity(opportunity_id)
        logger.info("Opportunity %s not found in GHL (deleted), removed from local DB", opportunity_id)


def _fetch_and_store_opportunity(location_id: str, opportunity_id: str):
    """
    Fetch full opportunity from GHL API and upsert to GHLOpportunity (allowed pipelines only).
    Skips any opportunity belonging to a pipeline listed in BLOCKED_PIPELINE_IDS.
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

    # Extract common fields
    data = full_opportunity if isinstance(full_opportunity, dict) else {}
    opp_obj = data.get('opportunity') if isinstance(data.get('opportunity'), dict) else data
    
    # 1. Date Filter: Ignore legacy opportunities created before Dec 1, 2025
    date_added_str = opp_obj.get('dateAdded')
    date_added = _parse_dt(date_added_str)
    if date_added and date_added < SYNC_START_DATE:
        logger.info(
            "Skipping legacy opportunity %s - created on %s (before %s)",
            opportunity_id, date_added_str, SYNC_START_DATE
        )
        return

    # 2. Pipeline Filter: Skip blocked pipelines
    pipeline_id = opp_obj.get('pipelineId') or ''
    if pipeline_id in BLOCKED_PIPELINE_IDS:
        logger.info(
            "Skipping opportunity %s - pipeline %s is blocked from syncing",
            opportunity_id, pipeline_id
        )
        return

    _db_update_or_create_opportunity(opportunity_id, location, full_opportunity)
    logger.info("Stored opportunity %s for location %s", opportunity_id, location_id)


def _parse_dt(val):
    """Parse datetime from string, return None if invalid."""
    if not val:
        return None
    try:
        from django.utils.dateparse import parse_datetime
        return parse_datetime(val) if isinstance(val, str) else val
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# In-memory cache for GHL pipeline/stage names.
# Key: location_id  →  Value: { 'pipeline_names': {pipe_id: name},
#                               'stage_names': {stage_id: name},
#                               'fetched_at': datetime }
# Refreshed every PIPELINE_CACHE_TTL_SECONDS seconds.
# ---------------------------------------------------------------------------
import threading as _threading
_pipeline_cache: dict = {}
_pipeline_cache_lock = _threading.Lock()
PIPELINE_CACHE_TTL_SECONDS = 1800  # 30 minutes


def _get_pipeline_maps(location_id: str):
    """
    Return (pipeline_names, stage_names) dicts for the given location.
    Served from in-memory cache; refreshes from GHL API when stale (>30 min).
    pipeline_names : { pipeline_id -> pipeline_name }
    stage_names    : { stage_id    -> stage_name    }
    """
    import datetime
    with _pipeline_cache_lock:
        cached = _pipeline_cache.get(location_id)
        if cached:
            age = (datetime.datetime.utcnow() - cached['fetched_at']).total_seconds()
            if age < PIPELINE_CACHE_TTL_SECONDS:
                return cached['pipeline_names'], cached['stage_names']

    # Cache miss or expired → fetch from GHL API
    pipeline_names = {}
    stage_names = {}
    try:
        client = GHLClient(location_id=location_id)
        pipelines = client.get_pipelines()
        for p in pipelines:
            if not isinstance(p, dict) or not p.get('id'):
                continue
            pipe_id = p['id']
            pipeline_names[pipe_id] = p.get('name') or p.get('pipelineName') or pipe_id
            for stage in (p.get('stages') or []):
                if isinstance(stage, dict) and stage.get('id'):
                    sid = stage['id']
                    stage_names[sid] = stage.get('name') or stage.get('stageName') or sid
        logger.info("Pipeline cache refreshed for location %s: %d pipelines, %d stages",
                    location_id, len(pipeline_names), len(stage_names))
    except Exception as e:
        logger.warning("GHL pipelines API fetch failed for location %s: %s — will use DB fallback", location_id, e)

    import datetime
    with _pipeline_cache_lock:
        _pipeline_cache[location_id] = {
            'pipeline_names': pipeline_names,
            'stage_names': stage_names,
            'fetched_at': datetime.datetime.utcnow(),
        }
    return pipeline_names, stage_names


def _resolve_pipeline_stage_names(location_id: str, pipeline_id: str, pipeline_stage_id: str):
    """
    Resolve human-readable pipeline_name and pipeline_stage_name.

    Strategy:
      1. GHL pipelines API (via 30-min in-memory cache) — always current.
      2. If GHL unreachable, fall back to existing DB rows.

    Returns: (pipeline_name, pipeline_stage_name) as strings.
    """
    pipeline_name = ''
    pipeline_stage_name = ''

    if not pipeline_id:
        return pipeline_name, pipeline_stage_name

    # --- Step 1: GHL API (cached, always current) ---
    try:
        pipeline_names, stage_names = _get_pipeline_maps(location_id)
        pipeline_name = pipeline_names.get(pipeline_id, '')
        pipeline_stage_name = stage_names.get(pipeline_stage_id, '') if pipeline_stage_id else ''
    except Exception as e:
        logger.warning("Pipeline cache lookup failed: %s", e)

    # Both resolved from GHL cache — done
    if pipeline_name and (pipeline_stage_name or not pipeline_stage_id):
        return pipeline_name, pipeline_stage_name

    # --- Step 2: DB fallback (GHL was unreachable) ---
    try:
        with connection.cursor() as cursor:
            if not pipeline_name:
                cursor.execute("""
                    SELECT pipeline_name FROM opportunity_report
                    WHERE pipeline_id = %s
                      AND pipeline_name IS NOT NULL AND pipeline_name <> ''
                      AND pipeline_name <> pipeline_id
                    LIMIT 1
                """, [pipeline_id])
                row = cursor.fetchone()
                if row:
                    pipeline_name = row[0]

            if pipeline_stage_id and not pipeline_stage_name:
                cursor.execute("""
                    SELECT pipeline_stage_name FROM opportunity_report
                    WHERE pipeline_stage_id = %s
                      AND pipeline_stage_name IS NOT NULL AND pipeline_stage_name <> ''
                      AND pipeline_stage_name <> pipeline_stage_id
                    LIMIT 1
                """, [pipeline_stage_id])
                row = cursor.fetchone()
                if row:
                    pipeline_stage_name = row[0]
    except Exception as e:
        logger.warning("DB name fallback failed for pipeline %s: %s", pipeline_id, e)

    return pipeline_name, pipeline_stage_name


def _upsert_opportunity_report(opportunity_id: str, location: GHLLocation, raw_data: dict):
    """
    Upsert into existing opportunity_report table (RDS) without altering it.
    Maps GHL opportunity data to matching columns, including pipeline_name and pipeline_stage_name.
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
            monetary = int(monetary)
        except (TypeError, ValueError):
            monetary = None
    created_date_val = created_at or updated_at

    pipe_id = opp_obj.get('pipelineId') or ''
    stage_id = opp_obj.get('pipelineStageId') or ''

    # Resolve human-readable names (DB lookup first, GHL API as fallback)
    pipeline_name, pipeline_stage_name = _resolve_pipeline_stage_names(
        location.location_id, pipe_id, stage_id
    )

    # --- Dynamic custom fields for opportunities ---
    raw_cf = opp_obj.get('customFields') or []
    client = GHLClient(location_id=location.location_id)
    cf_col_values = {}
    try:
        cf_col_values = sync_and_get_custom_field_values(
            location_id=location.location_id,
            model='opportunity',
            raw_custom_fields=raw_cf,
            client=client,
            table_name='opportunity_report',
        )
    except Exception as cf_exc:
        logger.warning("Custom field sync failed for opportunity %s: %s", opportunity_id, cf_exc)

    # Build base columns and values
    base_cols = [
        'opportunity_id', 'pipeline_id', 'pipeline_stage_id', 'pipeline_name', 'pipeline_stage_name',
        'assigned_to', 'contact_id', 'location_id', 'lost_reason_id', 'opportunity_name',
        'monetary_value', 'status', 'source', 'last_status_change_at',
        'last_stage_change_at', 'created_at', 'updated_at', 'created_date',
        'contact_name', 'email', 'phone', 'company_name',
    ]
    base_vals = [
        opportunity_id,
        pipe_id,
        stage_id,
        pipeline_name,
        pipeline_stage_name,
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
    ]

    # Append dynamic custom field columns
    cf_cols = list(cf_col_values.keys())
    cf_vals = [cf_col_values[c] for c in cf_cols]
    all_cols = base_cols + cf_cols
    all_vals = base_vals + cf_vals

    col_identifiers = ', '.join(f'"{c}"' for c in all_cols)
    placeholders = ', '.join(['%s'] * len(all_cols))
    # All columns after opportunity_id are updatable on conflict
    update_set = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in all_cols if c != 'opportunity_id')

    sql = f"""
        INSERT INTO opportunity_report ({col_identifiers})
        VALUES ({placeholders})
        ON CONFLICT (opportunity_id) DO UPDATE SET
            {update_set}
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, all_vals)


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


# ---------------------------------------------------------------------------
# Contact webhook handlers
# ---------------------------------------------------------------------------

def process_contact_webhook(event_type: str, location_id: str, contact_id: str):
    """
    Process contact webhook: fetch from GHL then upsert or delete in contact_report.
    Supported event types: ContactCreate, ContactUpdate, ContactDelete, ContactTagUpdate, ContactDndUpdate
    """
    if event_type == 'ContactDelete':
        _handle_contact_delete(location_id, contact_id)
    elif event_type in (
        'ContactCreate', 'ContactUpdate', 'ContactAdded',
        'ContactTagUpdate', 'ContactDndUpdate'
    ):
        # ContactTagUpdate: webhook payload only has the contact id.
        # We do a full re-fetch so that the latest tags list from GHL API
        # is stored in both the contact_report.tags column and any cf_ columns.
        _fetch_and_store_contact(location_id, contact_id)
    else:
        logger.debug("Unrecognized contact event_type=%s for contact %s", event_type, contact_id)


def _handle_contact_delete(location_id: str, contact_id: str):
    """On delete: remove contact from contact_report if it exists."""
    try:
        GHLLocation.objects.get(location_id=location_id, status='active')
    except GHLLocation.DoesNotExist:
        logger.warning("Location %s not found or inactive, skipping contact delete", location_id)
        return

    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM contact_report WHERE id = %s", [contact_id])
    logger.info("Deleted contact %s from contact_report", contact_id)


def _fetch_and_store_contact(location_id: str, contact_id: str):
    """Fetch full contact from GHL API and upsert into contact_report."""
    try:
        location = GHLLocation.objects.get(location_id=location_id, status='active')
    except GHLLocation.DoesNotExist:
        logger.warning("Location %s not found or inactive, skipping contact fetch", location_id)
        return

    client = GHLClient(location_id=location_id)
    try:
        full_contact = client.get_contact(contact_id)
    except Exception as exc:
        logger.error("GHL API error fetching contact %s: %s", contact_id, exc)
        raise

    # GHL wraps the contact under "contact" key
    data = full_contact if isinstance(full_contact, dict) else {}
    c = data.get('contact') if isinstance(data.get('contact'), dict) else data
    
    # Date Filter: Ignore legacy contacts created before Dec 1, 2025
    date_added_str = c.get('dateAdded')
    date_added_dt = _parse_dt(date_added_str)
    if date_added_dt and date_added_dt < SYNC_START_DATE:
        logger.info(
            "Skipping legacy contact %s - created on %s (before %s)",
            contact_id, date_added_str, SYNC_START_DATE
        )
        return

    _upsert_contact_report(contact_id, location, full_contact)
    logger.info("Stored contact %s for location %s", contact_id, location_id)


def _parse_date(val):
    """Parse a date string (various formats) into a date object, or return None."""
    if not val:
        return None
    try:
        from django.utils.dateparse import parse_date, parse_datetime
        if isinstance(val, str):
            # Try date first, then datetime
            d = parse_date(val[:10])  # take first 10 chars covers YYYY-MM-DD
            if d:
                return d
            dt = parse_datetime(val)
            return dt.date() if dt else None
        return val
    except (TypeError, ValueError):
        return None


def _upsert_contact_report(contact_id: str, location: GHLLocation, raw_data: dict):
    """
    Upsert into contact_report table using raw GHL API contact response.
    GHL GET /contacts/{id} returns { "contact": { ... } }.
    """
    data = raw_data if isinstance(raw_data, dict) else {}
    # GHL wraps the contact under "contact" key
    c = data.get('contact') if isinstance(data.get('contact'), dict) else data

    # Basic fields
    first_name = (c.get('firstName') or '')[:500]
    last_name  = (c.get('lastName') or '')[:500]
    contact_name = (c.get('contactName') or c.get('name') or f"{first_name} {last_name}".strip())[:500]
    email      = (c.get('email') or '')[:500]
    phone      = (c.get('phone') or '')[:100]

    # Phone label from additionalPhones[0].label if available
    phones_raw = c.get('phones') or c.get('additionalPhones') or []
    phone_label = ''
    if phones_raw and isinstance(phones_raw[0], dict):
        phone_label = (phones_raw[0].get('label') or '')[:100]

    company_name  = (c.get('companyName') or '')[:500]
    business_id   = (c.get('businessId') or '')[:255]
    business_name = (c.get('businessName') or '')[:500]
    address       = (c.get('address1') or '')[:1000]
    city          = (c.get('city') or '')[:255]
    state         = (c.get('state') or '')[:255]
    country       = (c.get('country') or '')[:255]
    postal_code   = (c.get('postalCode') or '')[:50]
    website       = (c.get('website') or '')[:500]
    tz            = (c.get('timezone') or '')[:100]
    source        = (c.get('source') or '')[:500]
    contact_type  = (c.get('type') or '')[:100]
    valid_email   = c.get('validEmail')
    dnd           = c.get('dnd')
    assigned_to   = (c.get('assignedTo') or '')[:255]

    date_added   = _parse_date(c.get('dateAdded'))
    date_updated = _parse_date(c.get('dateUpdated'))
    date_of_birth = _parse_date(c.get('dateOfBirth'))

    import json as json_lib
    def _as_json(val):
        """Convert value to JSON string for JSONB columns, or None if empty."""
        if val is None:
            return None
        if isinstance(val, (dict, list)):
            return json_lib.dumps(val)
        return str(val)

    additional_emails       = _as_json(c.get('additionalEmails') or c.get('emails'))
    additional_phones       = _as_json(phones_raw)
    tags                    = _as_json(c.get('tags'))
    custom_fields           = _as_json(c.get('customFields'))
    dnd_settings            = _as_json(c.get('dndSettings'))
    inbound_dnd_settings    = _as_json(c.get('inboundDndSettings'))
    followers               = _as_json(c.get('followers'))
    opportunities           = _as_json(c.get('opportunities'))
    attribution_source      = _as_json(c.get('attributionSource'))
    last_attribution_source = _as_json(c.get('lastAttributionSource'))

    # --- Dynamic custom fields for contacts ---
    raw_cf_list = c.get('customFields') or []
    _client = GHLClient(location_id=location.location_id)
    cf_col_values = {}
    try:
        cf_col_values = sync_and_get_custom_field_values(
            location_id=location.location_id,
            model='contact',
            raw_custom_fields=raw_cf_list,
            client=_client,
            table_name='contact_report',
        )
    except Exception as cf_exc:
        logger.warning("Custom field sync failed for contact %s: %s", contact_id, cf_exc)

    # Base columns (existing schema preserved)
    base_cols = [
        'id', 'location_id', 'first_name', 'last_name', 'contact_name',
        'email', 'phone', 'phone_label', 'company_name', 'business_id', 'business_name',
        'address', 'city', 'state', 'country', 'postal_code', 'website', 'timezone',
        'date_added', 'date_updated', 'date_of_birth', 'source', 'type',
        'valid_email', 'dnd', 'assigned_to',
        'additional_emails', 'additional_phones', 'tags', 'custom_fields',
        'dnd_settings', 'inbound_dnd_settings', 'followers', 'opportunities',
        'attribution_source', 'last_attribution_source',
    ]
    base_vals = [
        contact_id, location.location_id, first_name, last_name, contact_name,
        email, phone, phone_label, company_name, business_id, business_name,
        address, city, state, country, postal_code, website, tz,
        date_added, date_updated, date_of_birth, source, contact_type,
        valid_email, dnd, assigned_to,
        additional_emails, additional_phones, tags, custom_fields,
        dnd_settings, inbound_dnd_settings, followers, opportunities,
        attribution_source, last_attribution_source,
    ]

    # Append dynamic custom field columns
    cf_cols = list(cf_col_values.keys())
    cf_vals = [cf_col_values[k] for k in cf_cols]
    all_cols = base_cols + cf_cols
    all_vals = base_vals + cf_vals

    col_identifiers = ', '.join(f'"{col}"' for col in all_cols)
    placeholders = ', '.join(['%s'] * len(all_cols))
    # All columns except primary key 'id' are updatable on conflict
    update_set = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in all_cols if col != 'id')

    sql = f"""
        INSERT INTO contact_report ({col_identifiers})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
            {update_set}
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, all_vals)

