"""
GHL vs DB Reconciliation Script
================================
Finds records that exist in our DB but no longer exist in GHL (orphaned records).

Strategy:
- Fetch ALL opportunity IDs from GHL (paginated via POST /opportunities/search)
- Fetch ALL contact IDs from GHL (paginated via GET /contacts)
- Compare with DB IDs
- Output orphaned records (in DB but not in GHL) to JSON files

Run on server: python reconcile_ghl_db.py
Note: This will make many API calls. Be patient — may take 30–60 minutes.
"""

import os
import sys
import json
import time
import logging
import datetime
import django

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from django.db import connection
from ghl.models import GHLLocation
from ghl.services import GHLClient

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
# Primary location to check against (the main HMG location)
PRIMARY_LOCATION_ID = 'Gr7A9M5HBop3hB1v2owg'

# Pipeline IDs to check for opportunities
PIPELINE_IDS = [
    'XhU5rh8b2jJfHIIGr4NW',  # HMG
    'kGnCNaOtRRiPXmUH4xLV',  # Z - Duplications Only
]

# Date range: only compare DB records created within this window
# (matches the scope GHL was syncing from)
DB_DATE_FROM = '2025-12-01'  # December 1, 2025
DB_DATE_TO   = '2026-03-26'  # March 25, 2026 (exclusive upper bound)

# Rate limit: pause between API calls (seconds)
API_PAUSE = 0.1   # 10 calls/sec max (but we have retries for 429)

# Max retries for 429 (Too Many Requests) errors
MAX_RETRIES = 5
RETRY_DELAY_BASE = 2  # wait 2, 4, 8, 16... sec on 429

# Page size for GHL API calls
PAGE_SIZE = 100
# ──────────────────────────────────────────────────────────────────────────────


def handle_ghl_request_with_retries(client, method, path, **kwargs):
    """Make GHL request with automatic retry on 429 (Too Many Requests) errors."""
    retries = 0
    while retries < MAX_RETRIES:
        try:
            return client._request(method, path, **kwargs)
        except Exception as exc:
            # Check for 429 (Too Many Requests)
            # Both requests and urllib exceptions will be handled
            is_429 = False
            status_code = getattr(getattr(exc, 'response', None), 'status_code', None)
            if status_code == 429:
                is_429 = True
            elif hasattr(exc, 'code') and exc.code == 429: # urllib error
                is_429 = True
            
            if is_429:
                delay = RETRY_DELAY_BASE * (2 ** retries)
                logger.warning("GHL Rate Limit Hit (429)! Retrying in %ds... (attempt %d/%d)", delay, retries + 1, MAX_RETRIES)
                time.sleep(delay)
                retries += 1
                continue
            
            # Non-429 error, re-raise
            raise exc
    
    raise Exception(f"Max retries exceeded for {path} after {MAX_RETRIES} attempts.")


def fetch_all_ghl_opportunity_ids(client, pipeline_id):
    """Fetch ALL opportunity IDs from GHL for a given pipeline using GET pagination."""
    ids = set()
    page = 1
    start_after_id = None

    while True:
        # GHL V2 Opportunity Search requires snake_case for location_id and pipeline_id
        params = f"location_id={client.location_id}&pipeline_id={pipeline_id}&limit={PAGE_SIZE}"
        if start_after_id:
            params += f"&startAfterId={start_after_id}"
        
        path = f"/opportunities/search?{params}"

        try:
            # Using GET for search to avoid 422 Unprocessable Entity
            resp = handle_ghl_request_with_retries(client, 'GET', path)
        except Exception as exc:
            logger.error("GHL API error fetching opportunities (pipeline=%s, page=%d): %s", pipeline_id, page, exc)
            break

        # Response format is usually { "opportunities": [...] }
        opps = resp.get('opportunities', [])
        if not opps:
            break

        new_ids_count = 0
        for opp in opps:
            oid = opp.get('id')
            if oid and oid not in ids:
                ids.add(oid)
                new_ids_count += 1

        meta = resp.get('meta', {})
        total = meta.get('total', 'unknown')
        logger.info("Pipeline %s page %d: found %d new IDs, total so far=%d (GHL total estimate=%s)", 
                    pipeline_id, page, new_ids_count, len(ids), total)

        if len(opps) < PAGE_SIZE:
            break

        start_after_id = opps[-1].get('id')
        if not start_after_id:
            break
        
        page += 1
        time.sleep(API_PAUSE)

    return ids


def fetch_all_ghl_contact_ids(client):
    """Fetch ALL contact IDs from GHL using cursor-based pagination."""
    ids = set()
    page = 1
    start_after_id = None

    while True:
        params = f"locationId={client.location_id}&limit={PAGE_SIZE}"
        if start_after_id:
            params += f"&startAfterId={start_after_id}"
        path = f"/contacts/?{params}"

        try:
            resp = handle_ghl_request_with_retries(client, 'GET', path)
        except Exception as exc:
            logger.error("GHL API error fetching contacts (page=%d): %s", page, exc)
            break

        contacts = (
            resp.get('contacts')
            or resp.get('data')
            or []
        )
        if not contacts:
            break

        for c in contacts:
            cid = c.get('id')
            if cid and cid not in ids:
                ids.add(cid)

        meta = resp.get('meta', {})
        total = meta.get('total', 'unknown')
        logger.info("Contacts page %d: unique IDs so far=%d (GHL total estimate=%s)", page, len(ids), total)

        if len(contacts) < PAGE_SIZE:
            break

        last_id = contacts[-1].get('id')
        if not last_id:
            logger.warning("Last contact on page %d has no 'id' — keys: %s", page, list(contacts[-1].keys()))
            break

        if last_id == start_after_id:
            logger.error("Pagination cursor didn't advance (same id: %s). Breaking loop.", last_id)
            break

        start_after_id = last_id
        page += 1
        time.sleep(API_PAUSE)

    return ids


def fetch_all_db_opportunity_ids(pipeline_ids):
    """Fetch opportunity IDs from our DB for the given pipelines within the date window."""
    placeholders = ', '.join(['%s'] * len(pipeline_ids))
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT opportunity_id FROM opportunity_report "
            f"WHERE pipeline_id IN ({placeholders}) "
            f"AND created_at >= %s AND created_at < %s",
            pipeline_ids + [DB_DATE_FROM, DB_DATE_TO]
        )
        return {row[0] for row in cursor.fetchall()}


def fetch_all_db_contact_ids():
    """Fetch contact IDs from contact_report within the date window."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT id FROM contact_report "
            "WHERE location_id = %s AND date_added >= %s AND date_added < %s",
            [PRIMARY_LOCATION_ID, DB_DATE_FROM, DB_DATE_TO]
        )
        return {row[0] for row in cursor.fetchall()}


def main():
    logger.info("=== GHL vs DB Reconciliation ===")
    logger.info("Date window: %s to %s", DB_DATE_FROM, DB_DATE_TO)
    logger.info("Pipelines: %s", PIPELINE_IDS)
    start = datetime.datetime.utcnow()

    client = GHLClient(location_id=PRIMARY_LOCATION_ID)

    # ── OPPORTUNITIES ─────────────────────────────────────────────────────────
    logger.info("Fetching all opportunity IDs from GHL...")
    ghl_opp_ids = set()
    for pid in PIPELINE_IDS:
        logger.info("  Fetching pipeline: %s", pid)
        ids = fetch_all_ghl_opportunity_ids(client, pid)
        logger.info("  Pipeline %s: %d opportunities in GHL", pid, len(ids))
        ghl_opp_ids.update(ids)

    logger.info("Total GHL opportunity IDs: %d", len(ghl_opp_ids))

    logger.info("Fetching all opportunity IDs from DB...")
    db_opp_ids = fetch_all_db_opportunity_ids(PIPELINE_IDS)
    logger.info("Total DB opportunity IDs: %d", len(db_opp_ids))

    orphaned_opps = db_opp_ids - ghl_opp_ids
    logger.info("Orphaned opportunities (in DB but not GHL): %d", len(orphaned_opps))

    # ── CONTACTS ──────────────────────────────────────────────────────────────
    logger.info("Fetching all contact IDs from GHL...")
    ghl_contact_ids = fetch_all_ghl_contact_ids(client)
    logger.info("Total GHL contact IDs: %d", len(ghl_contact_ids))

    logger.info("Fetching all contact IDs from DB...")
    db_contact_ids = fetch_all_db_contact_ids()
    logger.info("Total DB contact IDs: %d", len(db_contact_ids))

    orphaned_contacts = db_contact_ids - ghl_contact_ids
    logger.info("Orphaned contacts (in DB but not GHL): %d", len(orphaned_contacts))

    # ── SAVE RESULTS ──────────────────────────────────────────────────────────
    end = datetime.datetime.utcnow()
    result = {
        'generated_at': end.isoformat(),
        'duration_seconds': (end - start).total_seconds(),
        'config': {
            'location_id': PRIMARY_LOCATION_ID,
            'pipelines': PIPELINE_IDS,
            'db_date_from': DB_DATE_FROM,
            'db_date_to': DB_DATE_TO,
            'note': 'GHL fetches ALL records (no date filter). DB filtered to date window only.',
        },
        'summary': {
            'ghl_opportunities': len(ghl_opp_ids),
            'db_opportunities': len(db_opp_ids),
            'orphaned_opportunities': len(orphaned_opps),
            'ghl_contacts': len(ghl_contact_ids),
            'db_contacts': len(db_contact_ids),
            'orphaned_contacts': len(orphaned_contacts),
        },
        'orphaned_opportunity_ids': sorted(orphaned_opps),
        'orphaned_contact_ids': sorted(orphaned_contacts),
    }

    with open('reconciliation_results.json', 'w') as f:
        json.dump(result, f, indent=2)

    logger.info("=== RESULTS SAVED to reconciliation_results.json ===")
    logger.info("Summary:")
    for k, v in result['summary'].items():
        logger.info("  %s: %s", k, v)

    logger.info("Done in %.1f seconds.", result['duration_seconds'])


if __name__ == '__main__':
    main()
