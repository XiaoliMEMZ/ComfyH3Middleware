from __future__ import annotations

import logging
from pathlib import Path

from aiohttp import web

from . import __version__
from .admin_api import routes as admin_routes
from .auth import ADMIN_SESSIONS_KEY, SERVICE_KEY, SETTINGS_KEY, AdminSessions, create_auth_middleware
from .config import Settings
from .public_api import routes as public_routes
from .service import GatewayService


LOGGER = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).resolve().parent / "static"


async def on_startup(app: web.Application) -> None:
    await app[SERVICE_KEY].start()


async def on_cleanup(app: web.Application) -> None:
    await app[SERVICE_KEY].stop()


def create_app(settings: Settings | None = None, service: GatewayService | None = None) -> web.Application:
    settings = settings or Settings.from_env()
    service = service or GatewayService(settings)
    app = web.Application(
        middlewares=[create_auth_middleware(settings)],
        client_max_size=settings.max_request_bytes,
    )
    app[SETTINGS_KEY] = settings
    app[SERVICE_KEY] = service
    app[ADMIN_SESSIONS_KEY] = AdminSessions()
    app.add_routes(public_routes)
    app.add_routes(admin_routes)
    app.router.add_static("/static/", STATIC_DIR, name="static", append_version=True)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    if settings.admin_token == "h3-admin-change-me" or settings.bootstrap_api_token == "h3-api-change-me":
        LOGGER.warning("Default credentials are active; set H3_ADMIN_TOKEN and H3_API_TOKEN before exposing the service")
    LOGGER.info("Starting H3 middleware %s on %s:%s", __version__, settings.host, settings.port)
    web.run_app(create_app(settings), host=settings.host, port=settings.port, print=None)


if __name__ == "__main__":
    main()
