"""
GHL onboarding and webhook views.
"""
import base64
import json as json_lib
import logging
from datetime import timedelta
from urllib import request as urllib_request
from urllib import error as urllib_error
from urllib import parse as urllib_parse

from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

try:
    import requests
except ImportError:
    requests = None
from django.shortcuts import redirect
from django.utils import timezone
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import GHLLocation, GHLOpportunity


def _serialize_opportunity(opp):
    """Serialize one GHLOpportunity to API dict."""
    raw = opp.raw_data or {}
    opp_obj = raw.get('opportunity') if isinstance(raw.get('opportunity'), dict) else raw
    contact = (opp_obj.get('contact') or {}) if isinstance(opp_obj.get('contact'), dict) else {}
    contact_name = (
        contact.get('name')
        or (f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip() or None)
        or opp_obj.get('contactName')
        or opp_obj.get('contactId')
    )
    contact_name = contact_name or '-'
    name = opp_obj.get('name') or '-'
    status = opp_obj.get('status') or '-'
    emails = contact.get('emails') or []
    phones = contact.get('phones') or []
    email = contact.get('email') or (emails[0].get('email') if emails and isinstance(emails[0], dict) else None)
    phone_val = contact.get('phone') or (phones[0].get('phone') if phones and isinstance(phones[0], dict) else None)
    email = email or '-'
    phone_val = phone_val or '-'
    return {
        'id': opp.opportunity_id,
        'location_id': opp.location.location_id,
        'location_name': opp.location.company_name or opp.location.location_id,
        'contact_name': contact_name,
        'contact_email': email,
        'contact_phone': phone_val,
        'name': name,
        'status': status,
        'monetary_value': opp_obj.get('monetaryValue'),
        'contact_id': opp_obj.get('contactId'),
        'pipeline_id': opp_obj.get('pipelineId'),
        'pipeline_stage_id': opp_obj.get('pipelineStageId'),
        'assigned_to': opp_obj.get('assignedTo'),
        'source': opp_obj.get('source'),
        'date_added': opp_obj.get('dateAdded'),
        'updated_at': opp.updated_at.isoformat() if opp.updated_at else None,
        'raw_data': raw,
    }


def _matches_search(item, q):
    """Return True if opportunity matches search query (case-insensitive)."""
    import re
    if not q or not q.strip():
        return True
    q_lower = q.strip().lower()
    q_digits = re.sub(r'\D', '', q)
    id_val = (item.get('id') or '-')
    contact_name = (item.get('contact_name') or '-')
    email = (item.get('contact_email') or '-')
    phone = (item.get('contact_phone') or '-')
    id_match = id_val != '-' and id_val.lower() == q_lower
    if id_match:
        return True
    if contact_name != '-' and q_lower in contact_name.lower():
        return True
    if email != '-' and q_lower in email.lower():
        return True
    if q_digits and phone != '-':
        phone_digits = re.sub(r'\D', '', phone)
        if phone_digits and q_digits in phone_digits:
            return True
    return id_val != '-' and q_lower in id_val.lower()


class OpportunityListView(APIView):
    """
    List opportunities with backend pagination and search.
    GET /api/ghlpage/opportunities/?page=1&page_size=10&search=...
    """
    permission_classes = [AllowAny]

    def get(self, request):
        page = max(1, int(request.query_params.get('page', 1)))
        page_size = max(1, min(100, int(request.query_params.get('page_size', 10))))
        search = (request.query_params.get('search') or '').strip()

        qs = GHLOpportunity.objects.select_related('location').order_by('-updated_at')

        if search:
            data = []
            for opp in qs:
                item = _serialize_opportunity(opp)
                if _matches_search(item, search):
                    data.append(item)
            total_count = len(data)
        else:
            from django.core.paginator import Paginator
            paginator = Paginator(qs, page_size)
            total_count = paginator.count
            page_obj = paginator.get_page(page)
            data = [_serialize_opportunity(opp) for opp in page_obj]

        total_pages = max(1, (total_count + page_size - 1) // page_size)
        if search:
            page = 1
            total_pages = 1

        return Response({
            'results': data,
            'count': total_count,
            'page': page,
            'page_size': page_size,
            'total_pages': total_pages,
        })
from .services import GHLClient

logger = logging.getLogger(__name__)


class GHLOnboardView(APIView):
    """
    Simple GET endpoint that redirects to GHL OAuth authorization page.
    User will select their location in GHL's interface.
    GET /api/onboard/
    """
    permission_classes = [AllowAny]

    def get(self, request):
        client_id = getattr(settings, 'GHL_CLIENT_ID', '')
        redirect_uri = getattr(settings, 'GHL_REDIRECTED_URI', '')
        scope = getattr(settings, 'GHL_SCOPE', '')
        auth_url = getattr(settings, 'GHL_AUTH_URL', '')

        if not all([client_id, redirect_uri, scope, auth_url]):
            return Response(
                {"error": "GHL OAuth configuration incomplete"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        version_id = request.query_params.get('version_id', '69aaf371cdd0ee5dc9618261')
        auth_redirect_url = (
            f"{auth_url}?"
            f"response_type=code&"
            f"redirect_uri={redirect_uri}&"
            f"client_id={client_id}&"
            f"scope={scope}&"
            f"version_id={version_id}"
        )

        return redirect(auth_redirect_url)


class GHLOAuthAuthorizeView(APIView):
    """
    Optional – starts OAuth with a known location_id.
    GET /api/oauth/authorize/?location_id=<id>
    """
    permission_classes = [AllowAny]

    def get(self, request):
        location_id = request.query_params.get('location_id')
        if not location_id:
            return Response(
                {"error": "location_id is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        client_id = getattr(settings, 'GHL_CLIENT_ID', '')
        redirect_uri = getattr(settings, 'GHL_REDIRECTED_URI', '')
        scope = getattr(settings, 'GHL_SCOPE', '')
        auth_url = getattr(settings, 'GHL_AUTH_URL', '')

        if not all([client_id, redirect_uri, scope, auth_url]):
            return Response(
                {"error": "GHL OAuth configuration incomplete"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        version_id = request.query_params.get('version_id', '69aaf371cdd0ee5dc9618261')
        auth_redirect_url = (
            f"{auth_url}?"
            f"response_type=code&"
            f"redirect_uri={redirect_uri}&"
            f"client_id={client_id}&"
            f"scope={scope}&"
            f"location_id={location_id}&"
            f"version_id={version_id}"
        )

        return redirect(auth_redirect_url)


class GHLOAuthCallbackView(APIView):
    """
    Handle OAuth callback from GHL.
    Exchanges authorization code for tokens and saves them.
    GET /api/oauth/callback/?code=<code>&locationId=<location_id>
    """
    permission_classes = [AllowAny]

    def get(self, request):
        code = request.query_params.get('code')
        location_id = request.query_params.get('locationId')

        if not code:
            return Response(
                {"error": "Authorization code is required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        client_id = getattr(settings, 'GHL_CLIENT_ID', '')
        client_secret = getattr(settings, 'GHL_CLIENT_SECRET', '')
        redirect_uri = getattr(settings, 'GHL_REDIRECTED_URI', '')
        base_url = getattr(settings, 'GHL_BASE_URL', 'https://services.leadconnectorhq.com')
        token_url = f"{base_url}/oauth/token"

        if not all([client_id, client_secret, redirect_uri]):
            return Response(
                {"error": "GHL OAuth configuration incomplete"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        payload = {
            'grant_type': 'authorization_code',
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'code': code,
        }

        try:
            if requests:
                response = requests.post(token_url, data=payload, timeout=30)
                response.raise_for_status()
                token_data = response.json()
            else:
                data_str = urllib_parse.urlencode(payload).encode('utf-8')
                req = urllib_request.Request(token_url, data=data_str, method='POST')
                req.add_header('Content-Type', 'application/x-www-form-urlencoded')
                with urllib_request.urlopen(req, timeout=30) as resp:
                    body = resp.read().decode('utf-8')
                    token_data = json_lib.loads(body or "{}")
        except urllib_error.HTTPError as exc:
            try:
                error_text = exc.read().decode('utf-8')
            except Exception:
                error_text = str(exc)
            logger.error("Failed to exchange OAuth code for tokens: %s - %s", exc, error_text, exc_info=True)
            return Response(
                {"error": "Failed to complete OAuth flow", "details": error_text},
                status=status.HTTP_400_BAD_REQUEST
            )
        except Exception as exc:
            error_text = str(exc)
            if requests and hasattr(exc, 'response') and exc.response is not None:
                error_text = getattr(exc.response, 'text', error_text)
            logger.error("Failed to exchange OAuth code for tokens: %s - %s", exc, error_text, exc_info=True)
            return Response(
                {"error": "Failed to complete OAuth flow", "details": error_text},
                status=status.HTTP_502_BAD_GATEWAY
            )

        if not location_id:
            location_id = token_data.get('locationId')

        if not location_id:
            return Response(
                {"error": "locationId not found in token response"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Decode token to verify location (optional debug)
        access_token = token_data.get('access_token')
        if access_token:
            try:
                token_parts = access_token.split('.')
                if len(token_parts) >= 2:
                    payload_part = token_parts[1]
                    payload_part += '=' * (4 - len(payload_part) % 4)
                    decoded = base64.urlsafe_b64decode(payload_part)
                    token_payload = json_lib.loads(decoded)
                    actual_location = token_payload.get('authClassId') or token_payload.get('primaryAuthClassId')
                    logger.debug("Token location: %s, saving to: %s", actual_location, location_id)
            except Exception as e:
                logger.debug("Error decoding token: %s", e)

        # Fetch location name from GHL API
        location_name = None
        try:
            if access_token:
                location_info_url = f"{base_url}/locations/{location_id}"
                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Version': getattr(settings, 'GHL_API_VERSION', '2021-07-28'),
                }
                if requests:
                    location_response = requests.get(location_info_url, headers=headers, timeout=30)
                    if location_response.status_code == 200:
                        location_info = location_response.json()
                        location_name = location_info.get('name') or location_info.get('companyName')
                else:
                    req = urllib_request.Request(location_info_url, headers=headers, method='GET')
                    with urllib_request.urlopen(req, timeout=30) as resp:
                        if resp.status == 200:
                            location_info = json_lib.loads(resp.read().decode('utf-8') or "{}")
                            location_name = location_info.get('name') or location_info.get('companyName')
        except Exception as exc:
            logger.warning("Failed to fetch location name for %s: %s", location_id, exc)

        location, created = GHLLocation.objects.update_or_create(
            location_id=location_id,
            defaults={
                'access_token': token_data.get('access_token', ''),
                'refresh_token': token_data.get('refresh_token', ''),
                'token_expires_at': timezone.now() + timedelta(seconds=token_data.get('expires_in', 3600)),
                'status': 'active',
                'company_name': location_name or '',
                'onboarded_at': timezone.now(),
                'metadata': {
                    **token_data,
                    'scope': token_data.get('scope'),
                    'user_type': token_data.get('userType'),
                    'company_id': token_data.get('companyId'),
                    'user_id': token_data.get('userId'),
                },
            }
        )

        logger.info("OAuth tokens saved for location %s (created: %s)", location_id, created)

        return Response(
            {
                "message": "Authentication successful",
                "location_id": location_id,
                "location_name": location_name or location.company_name,
                "token_stored": True,
                "note": f"Use this location_id ({location_id}) in your login URL: ?location={location_id}",
            },
            status=status.HTTP_200_OK
        )


@method_decorator(csrf_exempt, name='dispatch')
class GHLWebhookView(APIView):
    """
    Webhook endpoint for GHL opportunity events (OpportunityUpdate, OpportunityDelete, OpportunityCreate).
    Sync flow: GHL → our DB only (one-way).
    - Create/Update: fetch full opportunity from GHL, then upsert to DB.
    - Delete: try to fetch from GHL; if we can't fetch (404), delete from our DB.
    POST /api/ghlpage/webhooks/opportunity/
    """
    permission_classes = [AllowAny]

    def post(self, request):
        data = request.data if hasattr(request, 'data') else {}
        if not data and request.body:
            try:
                import json
                data = json.loads(request.body)
            except Exception:
                return Response({"error": "Invalid JSON"}, status=status.HTTP_400_BAD_REQUEST)

        print("Webhook payload:", data)

        event_type = data.get('type', '')
        location_id = data.get('locationId')
        opportunity_id = data.get('id')

        if not opportunity_id or not location_id:
            logger.warning("Webhook missing opportunity id or locationId: %s", data)
            return Response({"received": True}, status=status.HTTP_200_OK)

        # Offload to Celery if available (avoids SQLite DB locks on burst webhooks)
        try:
            from .tasks import process_opportunity_webhook_task
            if process_opportunity_webhook_task:
                process_opportunity_webhook_task.delay(
                    event_type=event_type,
                    location_id=location_id,
                    opportunity_id=opportunity_id,
                )
            else:
                raise ImportError("Task not available")
        except Exception:
            # Celery not available or Redis down: process synchronously
            from .webhook_handlers import process_opportunity_webhook
            process_opportunity_webhook(event_type, location_id, opportunity_id)

        return Response({"received": True}, status=status.HTTP_200_OK)
