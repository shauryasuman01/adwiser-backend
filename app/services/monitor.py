"""Request-scoped Monitor metadata and JSON write validation."""
import json
from fastapi.responses import JSONResponse
from app.middleware.auth import AuthError
from app.routes.overview import now_ms, scope_window, site_view, refresh_seconds
from app.services.session import session_response


async def scope(request, user):
    db = request.app.state.pg_pool
    session = await session_response(db, user)
    permissions = session['permissions']
    window = scope_window(request.query_params, permissions, now_ms())
    sites = await db.fetch('SELECT site_id,name,timezone,sla_target_pct FROM site ORDER BY name,site_id')
    selected = request.query_params.get('site')
    site = next((s for s in sites if s['site_id'] == selected), None) if selected is not None else (sites[0] if sites else None)
    if site is None:
        raise AuthError(400 if sites else 503, 'INVALID_REQUEST' if sites else 'INTERNAL_ERROR', 'Unknown site.' if sites else 'No sites configured.')
    counts = {'critical': 0, 'warning': 0, 'info': 0}
    for row in await db.fetch("SELECT severity,count(*) AS n FROM alert WHERE site_id=$1 AND ended_at IS NULL AND status<>'resolved' GROUP BY severity", site['site_id']):
        counts[row['severity']] = row['n']
    meta = {'user': session['user'], 'permissions': permissions, 'sites': [site_view(s) for s in sites],
            'site': site_view(site), 'window': window, 'generatedAt': now_ms(), 'source': 'api',
            'openAlerts': counts, 'refreshS': refresh_seconds(window, permissions)}
    return site['site_id'], meta


def envelope(meta, data):
    return JSONResponse({'meta': meta, 'data': data}, headers={'Cache-Control': 'no-store'})


async def json_body(request, note=False):
    if request.headers.get('content-type', '').split(';')[0].strip().lower() != 'application/json':
        raise AuthError(415, 'UNSUPPORTED_MEDIA_TYPE', 'Content-Type must be application/json.')
    try:
        raw = await request.body()
        if len(raw) > 8192:
            raise ValueError
        body = json.loads(raw)
        if not isinstance(body, dict):
            raise ValueError
        if note:
            if set(body) != {'text'} or not isinstance(body['text'], str) or not 1 <= len(body['text'].strip()) <= 2000:
                raise ValueError
            body['text'] = body['text'].strip()
        elif body:
            raise ValueError
        return body
    except (ValueError, UnicodeDecodeError):
        raise AuthError(400, 'INVALID_REQUEST', 'Invalid JSON body.')
