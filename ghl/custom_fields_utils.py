"""
Utilities for dynamic custom field column management in opportunity_report and contact_report.

Strategy:
  - Each GHL custom field is stored as a dedicated column in the target table.
  - Column names are derived from the field name: lowercased, spaces → underscores,
    non-alphanumeric chars stripped, prefixed with 'cf_' to avoid conflicts.
  - If a column doesn't exist in the table, it is added via ALTER TABLE (TEXT type).
  - Values are always updated when a record is upserted.
  - Custom field definitions are cached per location (30 min TTL) to avoid repeated API calls.
"""

import re
import logging
import threading
import datetime

from django.db import connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory cache for custom field definitions per location.
# Key: (location_id, model)  →  { 'fields': [...], 'fetched_at': datetime }
# ---------------------------------------------------------------------------
_cf_cache: dict = {}
_cf_cache_lock = threading.Lock()
CF_CACHE_TTL_SECONDS = 1800  # 30 minutes

# ---------------------------------------------------------------------------
# In-memory cache for existing columns per table (refreshed on ALTER).
# ---------------------------------------------------------------------------
_table_columns_cache: dict = {}
_table_columns_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Column name helpers
# ---------------------------------------------------------------------------

def field_name_to_column(name: str) -> str:
    """
    Convert a GHL custom field name to a safe PostgreSQL column name.
    Prefix with 'cf_' to avoid clashing with reserved/existing columns.
    E.g. 'Job Title Applied For' → 'cf_job_title_applied_for'
    """
    safe = name.strip().lower()
    safe = re.sub(r'[^a-z0-9]+', '_', safe)   # replace non-alphanumeric with _
    safe = re.sub(r'_+', '_', safe).strip('_')  # collapse multiple underscores
    col = f"cf_{safe}"
    # PostgreSQL identifiers max 63 chars
    return col[:63]


def field_id_to_column_map(fields: list) -> dict:
    """
    Build a dict: { field_id → column_name } from a list of GHL custom field dicts.
    """
    result = {}
    seen_cols = {}
    for f in fields:
        if not isinstance(f, dict):
            continue
        fid = f.get('id') or f.get('fieldId')
        name = f.get('name') or f.get('fieldName') or ''
        if not fid or not name:
            continue
        col = field_name_to_column(name)
        # handle duplicates by appending suffix
        if col in seen_cols:
            col = f"{col}_{seen_cols[col]}"
            seen_cols[col] = seen_cols.get(col, 1) + 1
        else:
            seen_cols[col] = 1
        result[fid] = col
    return result


# ---------------------------------------------------------------------------
# Column management
# ---------------------------------------------------------------------------

def _get_table_columns(table_name: str) -> set:
    """Return set of existing column names for the given table (cached)."""
    with _table_columns_lock:
        if table_name in _table_columns_cache:
            return _table_columns_cache[table_name]

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, [table_name])
        cols = {row[0] for row in cursor.fetchall()}

    with _table_columns_lock:
        _table_columns_cache[table_name] = cols

    return cols


def _invalidate_table_columns_cache(table_name: str):
    """Invalidate cached column list for a table (called after ALTER TABLE)."""
    with _table_columns_lock:
        _table_columns_cache.pop(table_name, None)


def ensure_custom_field_columns(table_name: str, id_to_col: dict):
    """
    For each column in id_to_col.values(), add it to the table if it doesn't exist.
    Uses ALTER TABLE ... ADD COLUMN IF NOT EXISTS (PostgreSQL 9.6+).
    """
    if not id_to_col:
        return

    existing = _get_table_columns(table_name)
    new_cols = set(id_to_col.values()) - existing

    if not new_cols:
        return

    logger.info("Adding %d new custom field column(s) to %s: %s", len(new_cols), table_name, sorted(new_cols))
    with connection.cursor() as cursor:
        for col in sorted(new_cols):
            try:
                # TEXT is flexible enough for all GHL custom field types
                cursor.execute(
                    f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{col}" TEXT'
                )
                logger.info("Added column '%s' to table '%s'", col, table_name)
            except Exception as exc:
                logger.warning("Could not add column '%s' to '%s': %s", col, table_name, exc)

    # Invalidate the column cache so next call re-reads from DB
    _invalidate_table_columns_cache(table_name)


# ---------------------------------------------------------------------------
# Custom field definition cache (per location + model)
# ---------------------------------------------------------------------------

def get_cached_custom_fields(location_id: str, model: str, client) -> list:
    """
    Return custom field definitions for a location, served from cache.
    Falls back to empty list if API call fails.
    model: 'contact' | 'opportunity'
    """
    cache_key = (location_id, model)
    with _cf_cache_lock:
        cached = _cf_cache.get(cache_key)
        if cached:
            age = (datetime.datetime.utcnow() - cached['fetched_at']).total_seconds()
            if age < CF_CACHE_TTL_SECONDS:
                return cached['fields']

    # Cache miss or stale → fetch from API
    fields = []
    try:
        fields = client.get_custom_fields(model=model)
        logger.info("Fetched %d custom fields (model=%s) for location %s", len(fields), model, location_id)
    except Exception as exc:
        logger.warning(
            "Failed to fetch custom fields (model=%s) for location %s: %s",
            model, location_id, exc
        )

    with _cf_cache_lock:
        _cf_cache[cache_key] = {'fields': fields, 'fetched_at': datetime.datetime.utcnow()}

    return fields


def invalidate_cf_cache(location_id: str, model: str = None):
    """Invalidate custom field cache for a location (optionally a specific model)."""
    with _cf_cache_lock:
        if model:
            _cf_cache.pop((location_id, model), None)
        else:
            for m in ['contact', 'opportunity']:
                _cf_cache.pop((location_id, m), None)


# ---------------------------------------------------------------------------
# Value extraction
# ---------------------------------------------------------------------------

def extract_custom_field_values(raw_custom_fields, id_to_col: dict) -> dict:
    """
    Given the raw custom fields array from GHL (list of {id, value}),
    return a dict: { column_name → str_value }.
    Values are coerced to strings (TEXT column).
    """
    col_values = {}
    if not raw_custom_fields or not id_to_col:
        return col_values

    import json as _json

    for item in raw_custom_fields:
        if not isinstance(item, dict):
            continue
        fid = item.get('id') or item.get('fieldId')
        val = item.get('value')
        col = id_to_col.get(fid)
        if col is None:
            continue  # field not in our definitions (maybe newly added, next sync will pick it up)
        # Coerce value to string
        if val is None:
            str_val = None
        elif isinstance(val, (dict, list)):
            str_val = _json.dumps(val)
        else:
            str_val = str(val)
        col_values[col] = str_val

    return col_values


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------

def sync_and_get_custom_field_values(
    location_id: str,
    model: str,
    raw_custom_fields,
    client,
    table_name: str,
) -> dict:
    """
    Full pipeline:
      1. Fetch custom field definitions (cached).
      2. Ensure all columns exist in the target table.
      3. Extract values from raw_custom_fields payload.
      4. Return dict: { column_name → value_str } for use in upsert.

    model: 'contact' | 'opportunity'
    raw_custom_fields: list of {id, value} from GHL API response
    client: GHLClient instance
    table_name: 'contact_report' | 'opportunity_report'
    """
    fields = get_cached_custom_fields(location_id, model, client)
    id_to_col = field_id_to_column_map(fields)
    ensure_custom_field_columns(table_name, id_to_col)
    return extract_custom_field_values(raw_custom_fields, id_to_col)
