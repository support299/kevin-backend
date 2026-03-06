#!/usr/bin/env python
"""
Check if a GHLOpportunity record is from the HMG pipeline.
Usage: python manage.py shell < check_hmg_opportunity.py
   OR:  python check_hmg_opportunity.py  (run from Backend dir after: python -c "import django; django.setup()" ...)
   OR:  python manage.py shell -c "exec(open('check_hmg_opportunity.py').read())"

Simplest: python manage.py shell
Then paste the code below, or: exec(open('check_hmg_opportunity.py').read())
"""
import os
import sys
import django

# Setup Django if run as standalone script
if __name__ == '__main__':
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
    django.setup()

from django.conf import settings
from ghl.models import GHLOpportunity
from ghl.services import GHLClient


def get_pipeline_id_from_opportunity(raw_data):
    """Extract pipelineId from opportunity raw_data."""
    if not raw_data or not isinstance(raw_data, dict):
        return None
    opp_obj = raw_data.get('opportunity') if isinstance(raw_data.get('opportunity'), dict) else raw_data
    return opp_obj.get('pipelineId') if isinstance(opp_obj, dict) else None


def check_hmg(db_id: int):
    """
    Check if GHLOpportunity with Django id=db_id is from HMG pipeline.
    """
    try:
        opp = GHLOpportunity.objects.get(id=db_id)
    except GHLOpportunity.DoesNotExist:
        print(f"GHLOpportunity with id={db_id} not found.")
        return

    pipeline_id = get_pipeline_id_from_opportunity(opp.raw_data or {})
    location_id = opp.location.location_id

    print(f"Opportunity: {opp.opportunity_id} (DB id={opp.id})")
    print(f"Location: {location_id}")
    print(f"Pipeline ID from raw_data: {pipeline_id or '(none)'}")

    if not pipeline_id:
        print("Result: NOT HMG (no pipelineId in raw_data)")
        return

    hmg_name = getattr(settings, 'GHL_HMG_PIPELINE_NAME', 'HMG') or ''
    if not hmg_name:
        print("Result: GHL_HMG_PIPELINE_NAME is empty - all pipelines treated as syncable")
        return

    client = GHLClient(location_id=location_id)
    hmg_pipeline_id = client.get_hmg_pipeline_id()

    if hmg_pipeline_id is None:
        print(f"Result: Cannot determine HMG pipeline (name '{hmg_name}' not found in GHL)")
        return

    if pipeline_id == hmg_pipeline_id:
        print("Result: YES - This opportunity is from the HMG pipeline")
    else:
        print(f"Result: NO - This opportunity is NOT from HMG pipeline (HMG pipeline_id={hmg_pipeline_id})")


if __name__ == '__main__':
    check_hmg(694)
