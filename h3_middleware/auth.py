from __future__ import annotations

import hmac
import secrets
import time
from typing import Awaitable, Callable

from aiohttp import web

from .config import Settings
from .database import Database
from .service import GatewayService


ADMIN_COOKIE = "h3_admin_session"
SETTINGS_KEY = web.AppKey("settings", Settings)
SERVICE_KEY = web.AppKey("service", GatewayService)
IS_ADMIN_KEY = web.RequestKey("is_admin", bool)
PRINCIPAL_KEY = web.RequestKey("principal", str)


class AdminSessions:
    def __init__(self, ttl: float = 12 * 60 * 60) -> None:
        self.ttl = ttl
        self.sessions: dict[str, float] = {}

    def create(self) -> str:
        token = secrets.token_urlsafe(32)
        self.sessions[token] = time.time() + self.ttl
        return token

    def valid(self, token: str | None) -> bool:
        if not token:
            return False
        expires_at = self.sessions.get(token)
        if expires_at is None:
            return False
        if expires_at <= time.time():
            self.sessions.pop(token, None)
            return False
        return True

    def revoke(self, token: str | None) -> None:
        if token:
            self.sessions.pop(token, None)


ADMIN_SESSIONS_KEY = web.AppKey("admin_sessions", AdminSessions)


def request_token(request: web.Request) -> str | None:
    authorization = request.headers.get("Authorization", "")
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return (request.headers.get("X-API-Key") or request.headers.get("X-API-Token") or "").strip() or None


def unauthorized(message: str = "Unauthorized") -> web.Response:
    return web.json_response({"ok": False, "error": {"message": message}}, status=401)


def create_auth_middleware(settings: Settings) -> Callable[..., Awaitable[web.StreamResponse]]:
    @web.middleware
    async def auth_middleware(request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]) -> web.StreamResponse:
        path = request.path
        if request.method == "OPTIONS":
            response: web.StreamResponse = web.Response(status=204)
        elif path in {"/", "/health", "/admin", "/admin/", "/admin/api/login", "/admin/api/session"} or path.startswith("/static/"):
            response = await handler(request)
        elif path.startswith("/admin/api/"):
            sessions = request.app[ADMIN_SESSIONS_KEY]
            bearer = request_token(request)
            cookie = request.cookies.get(ADMIN_COOKIE)
            if not (
                (bearer and hmac.compare_digest(bearer, settings.admin_token))
                or sessions.valid(cookie)
            ):
                return unauthorized()
            request[IS_ADMIN_KEY] = True
            request[PRINCIPAL_KEY] = "admin"
            response = await handler(request)
        elif path.startswith("/v1/"):
            token = request_token(request)
            sessions = request.app[ADMIN_SESSIONS_KEY]
            if sessions.valid(request.cookies.get(ADMIN_COOKIE)):
                request[IS_ADMIN_KEY] = True
                request[PRINCIPAL_KEY] = "admin"
            elif not token:
                return unauthorized()
            elif hmac.compare_digest(token, settings.admin_token):
                request[IS_ADMIN_KEY] = True
                request[PRINCIPAL_KEY] = "admin"
            elif hmac.compare_digest(token, settings.bootstrap_api_token):
                request[IS_ADMIN_KEY] = False
                request[PRINCIPAL_KEY] = "bootstrap"
            else:
                database: Database = request.app[SERVICE_KEY].database
                key_id = await database.validate_api_key(token)
                if not key_id:
                    return unauthorized("Invalid or expired API key")
                request[IS_ADMIN_KEY] = False
                request[PRINCIPAL_KEY] = key_id
            response = await handler(request)
        else:
            response = await handler(request)

        if path.startswith("/v1/"):
            response.headers["Access-Control-Allow-Origin"] = "*"
            response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type, X-API-Key, X-API-Token"
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
        return response

    return auth_middleware
