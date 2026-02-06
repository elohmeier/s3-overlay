from __future__ import annotations

from typing import TYPE_CHECKING

from litestar import Litestar, Request, get
from litestar.config.cors import CORSConfig
from litestar.handlers import asgi
from litestar.plugins.prometheus import PrometheusConfig, PrometheusController

from .proxy import S3OverlayProxy

if TYPE_CHECKING:
    from litestar.types import Receive, Scope, Send


prometheus_config = PrometheusConfig(app_name="s3_overlay", prefix="s3_overlay")


def create_app() -> Litestar:
    """Create the S3 overlay proxy ASGI application."""
    proxy = S3OverlayProxy.from_env()

    @get("/health", include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @asgi(path="/", is_mount=True, copy_scope=True)
    async def proxy_handler(scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope=scope, receive=receive)
        path = scope.get("path", "/")
        if not path.startswith("/"):
            path = f"/{path}"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/") or "/"
        response = await proxy.handle(request, path)
        asgi_response = response.to_asgi_response(None, request)
        await asgi_response(scope, receive, send)

    async def startup(app: Litestar) -> None:
        await proxy.startup()

    async def shutdown(app: Litestar) -> None:
        await proxy.shutdown()

    cors_config = CORSConfig(
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["ETag", "x-amz-*"],
    )

    return Litestar(
        route_handlers=[health, proxy_handler, PrometheusController],
        on_startup=[startup],
        on_shutdown=[shutdown],
        cors_config=cors_config,
        middleware=[prometheus_config.middleware],
    )


app = create_app()
