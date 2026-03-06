"""
GHL API client with OAuth token management.
"""
import json
import logging
from typing import Optional
from datetime import timedelta
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib import error as urllib_error

from django.conf import settings
from django.utils import timezone

try:
    import requests
except ImportError:
    requests = None

from .models import GHLLocation

logger = logging.getLogger(__name__)


class GHLClient:
    """Client for GHL API with automatic token refresh."""

    def __init__(self, location_id: str):
        self.location_id = location_id
        self.base_url = getattr(settings, 'GHL_BASE_URL', 'https://services.leadconnectorhq.com')
        self._location = None

    def _get_location(self):
        """Get or fetch the GHL location with valid token."""
        if self._location and self._location.is_token_valid():
            return self._location

        if not self.location_id:
            raise ValueError('location_id is required for OAuth authentication')

        location = GHLLocation.objects.get(location_id=self.location_id)
        self._location = location

        if not location.access_token:
            raise ValueError(f'GHL location {self.location_id} has no access token. Please re-onboard.')

        if not location.is_token_valid():
            logger.warning("Token for location %s is expired. Attempting refresh...", self.location_id)
            if location.needs_token_refresh() or not location.is_token_valid():
                self._refresh_access_token(location)
        elif location.needs_token_refresh():
            logger.info("Token for location %s expires soon. Refreshing proactively...", self.location_id)
            self._refresh_access_token(location)

        return self._location

    def _refresh_access_token(self, location: GHLLocation):
        """Refresh the OAuth access token using refresh token."""
        if not location.refresh_token:
            raise ValueError(
                f'No refresh token available for location {location.location_id}. Please re-authenticate.'
            )

        client_id = getattr(settings, 'GHL_CLIENT_ID', '')
        client_secret = getattr(settings, 'GHL_CLIENT_SECRET', '')

        if not client_id or not client_secret:
            raise ValueError('GHL_CLIENT_ID and GHL_CLIENT_SECRET must be configured')

        token_url = f"{self.base_url}/oauth/token"
        payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': location.refresh_token,
        }

        try:
            if requests:
                response = requests.post(token_url, data=payload, timeout=30)
                response.raise_for_status()
                data = response.json()
            else:
                data_str = urllib_parse.urlencode(payload).encode('utf-8')
                req = urllib_request.Request(token_url, data=data_str, method='POST')
                req.add_header('Content-Type', 'application/x-www-form-urlencoded')
                with urllib_request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode('utf-8')
                    data = json.loads(body or "{}")

            location.access_token = data.get('access_token', '')
            location.refresh_token = data.get('refresh_token', location.refresh_token)
            expires_in = data.get('expires_in', 3600)
            location.token_expires_at = timezone.now() + timedelta(seconds=expires_in)
            location.save(update_fields=['access_token', 'refresh_token', 'token_expires_at'])

            logger.info("Refreshed access token for location %s", location.location_id)
            self._location = location
        except (getattr(requests, 'RequestException', Exception), urllib_error.URLError, urllib_error.HTTPError) as exc:
            logger.error("Failed to refresh token for location %s: %s", location.location_id, exc, exc_info=True)
            raise

    def _request(self, method: str, path: str, **kwargs):
        """Make an authenticated request to GHL API."""
        location = self._get_location()
        url = f"{self.base_url}{path}"
        headers = kwargs.pop('headers', {})
        headers.setdefault('Authorization', f'Bearer {location.access_token}')
        headers.setdefault('Version', getattr(settings, 'GHL_API_VERSION', '2021-07-28'))
        headers.setdefault('Content-Type', 'application/json')
        kwargs['headers'] = headers
        kwargs.setdefault('timeout', 30)

        if requests:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json() if response.content else {}
        else:
            req = urllib_request.Request(url, headers=headers, method=method.upper())
            with urllib_request.urlopen(req, timeout=kwargs.get('timeout', 30)) as resp:
                body = resp.read().decode('utf-8')
                return json.loads(body) if body else {}

    def get_opportunity(self, opportunity_id: str) -> dict:
        """
        Fetch full opportunity from GHL API.
        GET /opportunities/{id}
        """
        path = f"/opportunities/{opportunity_id}"
        return self._request('GET', path)

    def get_opportunity_or_none(self, opportunity_id: str):
        """
        Fetch opportunity from GHL API. Returns None if not found (404).
        Raises for other errors (auth, network, etc.).
        """
        try:
            return self.get_opportunity(opportunity_id)
        except Exception as exc:
            if getattr(exc, 'response', None) and getattr(exc.response, 'status_code', None) == 404:
                return None
            if getattr(exc, 'code', None) == 404:  # urllib HTTPError
                return None
            raise

    def get_pipelines(self) -> list:
        """
        Fetch pipelines for this location from GHL API.
        GET /opportunities/pipelines?locationId={location_id}
        Returns list of pipeline dicts with 'id' and 'name'.
        """
        path = f"/opportunities/pipelines?locationId={self.location_id}"
        resp = self._request('GET', path)
        # GHL may return { "pipelines": [...] } or { "opportunities": { "pipelines": [...] } }
        pipelines = resp.get('pipelines') or resp.get('opportunities', {}).get('pipelines') or []
        return pipelines if isinstance(pipelines, list) else []

    def get_hmg_pipeline_id(self) -> Optional[str]:
        """
        Return the pipeline ID for the HMG pipeline (by name from settings).
        Returns None if GHL_HMG_PIPELINE_NAME is empty (sync all) or pipeline not found.
        """
        name = getattr(settings, 'GHL_HMG_PIPELINE_NAME', 'HMG') or ''
        if not name:
            return None  # Empty = sync all pipelines
        name_lower = name.strip().lower()
        for p in self.get_pipelines():
            if isinstance(p, dict) and (p.get('name') or '').strip().lower() == name_lower:
                return p.get('id')
        return None

    def search_opportunities(self, pipeline_id: str, limit: int = 10) -> list:
        """
        Search opportunities by pipeline.
        POST /opportunities/search
        Returns list of opportunity objects (may be summary; use get_opportunity for full details).
        """
        path = '/opportunities/search'
        body = {
            'locationId': self.location_id,
            'pipelineId': pipeline_id,
            'limit': limit,
        }
        # GHL API may expect different structure; try query wrapper if needed
        resp = self._request('POST', path, json=body)
        opportunities = resp.get('opportunities') or resp.get('opportunitiesList') or resp.get('data') or resp
        if isinstance(opportunities, list):
            return opportunities
        if isinstance(opportunities, dict) and 'opportunities' in opportunities:
            return opportunities.get('opportunities', [])
        return []
