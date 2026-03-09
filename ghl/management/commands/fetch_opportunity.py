"""
Fetch a single opportunity from GHL by ID.
Usage: python manage.py fetch_opportunity <opportunity_id> [--location LOCATION_ID] [--save]
"""
import json

from django.core.management.base import BaseCommand

from ghl.models import GHLLocation
from ghl.services import GHLClient
from ghl.webhook_handlers import _db_update_or_create_opportunity


class Command(BaseCommand):
    help = 'Fetch a single opportunity from GHL by ID'

    def add_arguments(self, parser):
        parser.add_argument(
            'opportunity_id',
            type=str,
            help='GHL opportunity ID to fetch',
        )
        parser.add_argument(
            '--location',
            type=str,
            default=None,
            help='GHL location ID (optional; uses first active location if not set)',
        )
        parser.add_argument(
            '--save',
            action='store_true',
            help='Save or update the opportunity in the local DB after fetching',
        )

    def handle(self, *args, **options):
        opportunity_id = (options['opportunity_id'] or '').strip()
        location_id = (options.get('location') or '').strip() or None
        save = options.get('save', False)

        if not opportunity_id:
            self.stderr.write(self.style.ERROR('opportunity_id is required.'))
            return

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

        self.stdout.write(f'Fetching opportunity {opportunity_id} from location {location_id} ...')

        try:
            client = GHLClient(location_id=location_id)
            data = client.get_opportunity(opportunity_id)
        except Exception as e:
            self.stderr.write(self.style.ERROR(f'GHL API error: {e}'))
            return

        if save:
            try:
                _db_update_or_create_opportunity(opportunity_id, location, data)
                self.stdout.write(self.style.SUCCESS(f'Saved opportunity {opportunity_id} to DB.'))
            except Exception as e:
                self.stderr.write(self.style.ERROR(f'Failed to save: {e}'))

        self.stdout.write(json.dumps(data, indent=2, default=str))
