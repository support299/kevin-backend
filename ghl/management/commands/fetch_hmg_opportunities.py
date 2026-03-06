"""
Fetch opportunities from HMG pipeline and store in DB.
Usage: python manage.py fetch_hmg_opportunities [--limit 10]
"""
import logging

from django.core.management.base import BaseCommand

from ghl.models import GHLLocation, GHLOpportunity
from ghl.services import GHLClient
from ghl.webhook_handlers import _db_update_or_create_opportunity

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Fetch opportunities from HMG pipeline and store in DB'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=10,
            help='Max number of opportunities to fetch (default: 10)',
        )
        parser.add_argument(
            '--location',
            type=str,
            default=None,
            help='Location ID (optional; uses first active location if not set)',
        )

    def handle(self, *args, **options):
        limit = options['limit']
        location_id = options['location']

        if location_id:
            try:
                location = GHLLocation.objects.get(location_id=location_id, status='active')
            except GHLLocation.DoesNotExist:
                self.stderr.write(self.style.ERROR(f'Location {location_id} not found or inactive.'))
                return
        else:
            location = GHLLocation.objects.filter(status='active').first()
            if not location:
                self.stderr.write(self.style.ERROR('No active GHL location found. Onboard first.'))
                return
            location_id = location.location_id

        self.stdout.write(f'Using location: {location_id}')

        client = GHLClient(location_id=location_id)
        hmg_pipeline_id = client.get_hmg_pipeline_id()
        if not hmg_pipeline_id:
            self.stderr.write(self.style.ERROR('HMG pipeline not found for this location.'))
            return

        self.stdout.write(f'HMG pipeline ID: {hmg_pipeline_id}')

        try:
            opportunities = client.search_opportunities(pipeline_id=hmg_pipeline_id, limit=limit)
        except Exception as e:
            self.stderr.write(self.style.ERROR(f'Search failed: {e}'))
            return

        if not opportunities:
            self.stdout.write(self.style.WARNING('No opportunities returned from search.'))
            return

        stored = 0
        for opp in opportunities:
            opp_id = opp.get('id') if isinstance(opp, dict) else None
            if not opp_id:
                continue
            try:
                full_opportunity = client.get_opportunity(opp_id)
                _db_update_or_create_opportunity(opp_id, location, full_opportunity)
                stored += 1
                self.stdout.write(f'  Stored: {opp_id}')
            except Exception as e:
                self.stderr.write(self.style.WARNING(f'  Failed to fetch/store {opp_id}: {e}'))

        self.stdout.write(self.style.SUCCESS(f'Stored {stored} opportunities from HMG pipeline.'))
