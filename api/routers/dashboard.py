"""Read-only product dashboard route serving hardened static assets."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter(tags=["dashboard"])

_STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
_INDEX_HTML = _STATIC_DIR / "index.html"

_SECURITY_HEADERS = {
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
    ),
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "same-origin",
    "Cache-Control": "no-store",
}


def _serve_index() -> FileResponse:
    if not _INDEX_HTML.exists():
        raise HTTPException(
            status_code=404,
            detail={
                "error": "DASHBOARD_UNAVAILABLE",
                "message": "Dashboard UI bundle is not installed.",
            },
        )
    return FileResponse(
        path=str(_INDEX_HTML),
        media_type="text/html; charset=utf-8",
        headers=_SECURITY_HEADERS,
    )


@router.get("/", include_in_schema=False)
def serve_root_dashboard() -> FileResponse:
    """Serve the interactive MarketPulseAI product dashboard at root."""
    return _serve_index()


@router.get("/dashboard", include_in_schema=False)
def serve_dashboard() -> FileResponse:
    """Serve the interactive MarketPulseAI product dashboard."""
    return _serve_index()
