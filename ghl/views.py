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

from django.db import connection

from .models import GHLLocation, GHLOpportunity
from .services import GHLClient


def _row_to_opportunity_dict(row, columns):
    """Convert a raw SQL row to API dict format (id, contact_name, etc.)."""
    d = dict(zip(columns, row))
    return {
        'id': d.get('opportunity_id') or '',
        'location_id': d.get('location_id') or '',
        'location_name': d.get('location_id') or '',  # opportunity_report has no location_name
        'contact_name': d.get('contact_name') or '-',
        'contact_email': d.get('email') or '-',
        'contact_phone': d.get('phone') or '-',
        'company_name': d.get('company_name') or '',
        'name': d.get('opportunity_name') or '-',
        'status': d.get('status') or '-',
        'monetary_value': d.get('monetary_value'),
        'contact_id': d.get('contact_id') or '',
        'pipeline_id': d.get('pipeline_id') or '',
        'pipeline_stage_id': d.get('pipeline_stage_id') or '',
        'pipeline_name': '',  # filled by _enrich_opportunities_with_pipeline_stage_names
        'pipeline_stage_name': '',
        'assigned_to': d.get('assigned_to') or '',
        'source': d.get('source') or '',
        'date_added': d.get('created_at').isoformat() if d.get('created_at') else None,
        'updated_at': d.get('updated_at').isoformat() if d.get('updated_at') else None,
        'raw_data': {},  # opportunity_report has no raw JSON
    }


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
        'pipeline_name': '',  # filled by _enrich_opportunities_with_pipeline_stage_names
        'pipeline_stage_name': '',
        'assigned_to': opp_obj.get('assignedTo'),
        'source': opp_obj.get('source'),
        'date_added': opp_obj.get('dateAdded'),
        'updated_at': opp.updated_at.isoformat() if opp.updated_at else None,
        'raw_data': raw,
    }


def _enrich_opportunities_with_pipeline_stage_names(data):
    """
    Enrich each opportunity dict with pipeline_name and pipeline_stage_name using
    GET /opportunities/pipelines?locationId=xxx (returns pipelines with stages).
    """
    import logging
    logger = logging.getLogger(__name__)
    if not data:
        return
    # Unique location_ids in this result set
    location_ids = set()
    for item in data:
        loc = (item.get('location_id') or '').strip()
        if loc:
            location_ids.add(loc)
    # Fetch pipelines (with stages) once per location
    pipeline_names = {}
    stage_names = {}
    for loc in location_ids:
        try:
            client = GHLClient(location_id=loc)
            pipelines = client.get_pipelines()
            for p in pipelines:
                if not isinstance(p, dict) or not p.get('id'):
                    continue
                pipe_id = (p.get('id') or '').strip()
                pipe_name = (p.get('name') or p.get('pipelineName') or '').strip()
                pipeline_names[(loc, pipe_id)] = pipe_name
                stages = p.get('stages') or []
                for s in stages:
                    if not isinstance(s, dict) or not s.get('id'):
                        continue
                    sid = (s.get('id') or '').strip()
                    sname = (s.get('name') or s.get('stageName') or '').strip()
                    if sid:
                        stage_names[(loc, pipe_id, sid)] = sname
        except Exception as e:
            logger.warning("Pipeline/stages fetch failed for location_id=%s: %s", loc, e)
    for item in data:
        loc = (item.get('location_id') or '').strip()
        pipe_id = (item.get('pipeline_id') or '').strip()
        stage_id = (item.get('pipeline_stage_id') or '').strip()
        item['pipeline_name'] = pipeline_names.get((loc, pipe_id), '') or item.get('pipeline_name', '')
        item['pipeline_stage_name'] = stage_names.get((loc, pipe_id, stage_id), '') or item.get('pipeline_stage_name', '')


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


def _fetch_from_opportunity_report(page, page_size, search, pipeline_id=None, pipeline_stage_id=None, source=None, status=None):
    """Fetch opportunities from opportunity_report table (PostgreSQL)."""
    import re
    cols = [
        'opportunity_id', 'pipeline_id', 'pipeline_stage_id', 'assigned_to', 'contact_id',
        'location_id', 'lost_reason_id', 'opportunity_name', 'monetary_value', 'status', 'source',
        'last_status_change_at', 'last_stage_change_at', 'created_at', 'updated_at',
        'contact_name', 'email', 'phone', 'company_name'
    ]
    col_list = ', '.join(cols)
    base_where = []
    base_params = []
    if pipeline_id and pipeline_id.strip():
        base_where.append("pipeline_id = %s")
        base_params.append(pipeline_id.strip())
    if pipeline_stage_id and pipeline_stage_id.strip():
        base_where.append("pipeline_stage_id = %s")
        base_params.append(pipeline_stage_id.strip())
    if source is not None and str(source).strip():
        base_where.append("source = %s")
        base_params.append(str(source).strip())
    if status is not None and str(status).strip():
        base_where.append("status = %s")
        base_params.append(str(status).strip())
    base_sql = f" WHERE {' AND '.join(base_where)}" if base_where else ""
    if search:
        q = search.strip()
        q_esc = q.replace('%', '\\%').replace('_', '\\_')
        q_digits = re.sub(r'\D', '', q)
        conditions = [
            "opportunity_id ILIKE %s",
            "contact_name ILIKE %s",
            "email ILIKE %s",
            "COALESCE(phone::text, '') ILIKE %s",
        ]
        pattern = f'%{q_esc}%'
        params = base_params + [pattern] * 4
        if q_digits:
            conditions.append("REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') LIKE %s")
            params.append(f'%{q_digits}%')
        search_conds = " OR ".join(conditions)
        if base_where:
            where = base_sql + " AND (" + search_conds + ")"
        else:
            where = " WHERE " + search_conds
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {col_list} FROM opportunity_report{where} ORDER BY updated_at DESC NULLS LAST",
                params
            )
            rows = cursor.fetchall()
        data = [_row_to_opportunity_dict(r, cols) for r in rows]
        total_count = len(data)
    else:
        offset = (page - 1) * page_size
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM opportunity_report{base_sql}",
                base_params
            )
            total_count = cursor.fetchone()[0]
            cursor.execute(
                f"SELECT {col_list} FROM opportunity_report{base_sql} ORDER BY updated_at DESC NULLS LAST LIMIT %s OFFSET %s",
                base_params + [page_size, offset]
            )
            rows = cursor.fetchall()
        data = [_row_to_opportunity_dict(r, cols) for r in rows]
    return data, total_count


class OpportunityListView(APIView):
    """
    List opportunities with backend pagination and search.
    Uses opportunity_report table when on PostgreSQL, else GHLOpportunity.
    GET /api/ghlpage/opportunities/?page=1&page_size=10&search=...
    """
    permission_classes = [AllowAny]

    def get(self, request):
        from django.conf import settings as django_settings
        from django.db import connections

        page = max(1, int(request.query_params.get('page', 1)))
        page_size = max(1, min(100, int(request.query_params.get('page_size', 10))))
        search = (request.query_params.get('search') or '').strip()
        pipeline_id = (request.query_params.get('pipeline_id') or '').strip() or None
        pipeline_stage_id = (request.query_params.get('pipeline_stage_id') or '').strip() or None
        source = (request.query_params.get('source') or '').strip() or None
        status_filter = (request.query_params.get('status') or '').strip() or None

        use_report_table = (
            connections['default'].settings_dict['ENGINE'] == 'django.db.backends.postgresql'
        )

        if use_report_table:
            try:
                data, total_count = _fetch_from_opportunity_report(
                    page, page_size, search,
                    pipeline_id=pipeline_id,
                    pipeline_stage_id=pipeline_stage_id,
                    source=source,
                    status=status_filter,
                )
            except Exception:
                use_report_table = False

        if not use_report_table:
            from django.db.models import Q
            qs = GHLOpportunity.objects.select_related('location').order_by('-updated_at')
            if pipeline_id:
                qs = qs.filter(
                    Q(raw_data__opportunity__pipelineId=pipeline_id) | Q(raw_data__pipelineId=pipeline_id)
                )
            if pipeline_stage_id:
                qs = qs.filter(
                    Q(raw_data__opportunity__pipelineStageId=pipeline_stage_id) | Q(raw_data__pipelineStageId=pipeline_stage_id)
                )
            if source:
                qs = qs.filter(
                    Q(raw_data__opportunity__source=source) | Q(raw_data__source=source)
                )
            if status_filter:
                qs = qs.filter(
                    Q(raw_data__opportunity__status=status_filter) | Q(raw_data__status=status_filter)
                )
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

        _enrich_opportunities_with_pipeline_stage_names(data)

        total_pages = max(1, (total_count + page_size - 1) // page_size)
        if search:
            total_pages = 1

        return Response({
            'results': data,
            'count': total_count,
            'page': page,
            'page_size': page_size,
            'total_pages': total_pages,
        })


class PipelinesListView(APIView):
    """
    List pipelines for the first active location (for dropdown), including stages.
    GET /api/ghlpage/pipelines/
    Returns [{ "id", "name", "stages": [{ "id", "name" }] }, ...] and default_pipeline_id (HMG).
    """
    permission_classes = [AllowAny]

    def get(self, request):
        location = GHLLocation.objects.filter(status='active').first()
        if not location:
            return Response({'pipelines': [], 'default_pipeline_id': None})
        try:
            client = GHLClient(location_id=location.location_id)
            pipelines = client.get_pipelines()
            result = []
            for p in pipelines:
                if not isinstance(p, dict) or not p.get('id'):
                    continue
                stages = p.get('stages') or []
                stage_list = [
                    {'id': s.get('id', ''), 'name': s.get('name') or s.get('stageName') or ''}
                    for s in stages if isinstance(s, dict) and s.get('id')
                ]
                result.append({
                    'id': p.get('id', ''),
                    'name': p.get('name') or p.get('pipelineName') or '',
                    'stages': stage_list,
                })
            hmg_name = (getattr(settings, 'GHL_HMG_PIPELINE_NAME', 'HMG') or '').strip().lower()
            default_pipeline_id = None
            if hmg_name:
                for p in pipelines:
                    if isinstance(p, dict) and (p.get('name') or '').strip().lower() == hmg_name:
                        default_pipeline_id = p.get('id')
                        break
            return Response({'pipelines': result, 'default_pipeline_id': default_pipeline_id})
        except Exception:
            return Response({'pipelines': [], 'default_pipeline_id': None})


class OpportunityFiltersView(APIView):
    """
    Distinct values for source and status (for filter dropdowns).
    GET /api/ghlpage/opportunities/filters/
    Returns { "sources": [...], "statuses": [...] } from DB.
    """
    permission_classes = [AllowAny]

    def get(self, request):
        from django.db import connections
        sources = []
        statuses = []
        use_report_table = (
            connections['default'].settings_dict['ENGINE'] == 'django.db.backends.postgresql'
        )
        if use_report_table:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT DISTINCT TRIM(source) FROM opportunity_report "
                        "WHERE source IS NOT NULL AND TRIM(COALESCE(source, '')) != '' ORDER BY 1"
                    )
                    sources = [row[0] for row in cursor.fetchall() if row[0]]
                    cursor.execute(
                        "SELECT DISTINCT TRIM(status) FROM opportunity_report "
                        "WHERE status IS NOT NULL AND TRIM(COALESCE(status, '')) != '' ORDER BY 1"
                    )
                    statuses = [row[0] for row in cursor.fetchall() if row[0]]
            except Exception:
                pass
        # Fallback or supplement: from GHLOpportunity raw_data (works for SQLite or if report query failed)
        if not sources or not statuses:
            seen_sources = set(sources)
            seen_statuses = set(statuses)
            for opp in GHLOpportunity.objects.only('raw_data').iterator(chunk_size=500):
                raw = opp.raw_data or {}
                opp_obj = raw.get('opportunity') if isinstance(raw.get('opportunity'), dict) else raw
                if isinstance(opp_obj, dict):
                    s = opp_obj.get('source') or opp_obj.get('sourceId')
                    if s and str(s).strip():
                        seen_sources.add(str(s).strip())
                    st = opp_obj.get('status')
                    if st and str(st).strip():
                        seen_statuses.add(str(st).strip())
            sources = sorted(seen_sources) if seen_sources else list(sources)
            statuses = sorted(seen_statuses) if seen_statuses else list(statuses)
        # Ensure at least common GHL statuses so dropdown is never empty
        default_statuses = ['open', 'won', 'lost']
        for ds in default_statuses:
            if ds not in statuses:
                statuses.append(ds)
        statuses.sort()
        return Response({'sources': sources, 'statuses': statuses})


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

        version_id = request.query_params.get('version_id', '69ab41154f90be25f703fe86')
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

        version_id = request.query_params.get('version_id', '69ab41154f90be25f703fe86')
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
