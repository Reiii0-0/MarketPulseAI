from __future__ import annotations

import collections
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic_settings import BaseSettings, SettingsConfigDict

from api.routers import dashboard, health, model, pipeline_read, predict
from consumer.db import DatabaseConfig, DatabaseManager
from model.predictor import Predictor
from pipeline.contract import load_forecast_contract

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class ApiConfig(BaseSettings):
    api_version: str = "1.0.0"
    api_cors_origins: str = "*"
    api_max_requests_per_minute: int = 60
    api_readonly_demo: bool = False
    model_artefact_path: str = "model/artefacts/xgb_model.json"
    scaler_artefact_path: str = "model/artefacts/scaler.pkl"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")


config = ApiConfig()


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    """7-step startup sequence with graceful handling for missing artifacts."""
    app.state.start_time = time.time()
    logger.info("🚀 Starting up MarketPulseAI API...")

    # 1. Validate environment & forecast contract
    db_config = DatabaseConfig()  # type: ignore
    try:
        app.state.contract = load_forecast_contract()
    except Exception as exc:
        logger.error("❌ Failed to load forecast contract: %s", type(exc).__name__)
        app.state.contract = None

    # 2. Initialise PostgreSQL connection pool
    try:
        app.state.db_manager = DatabaseManager(db_config)
        logger.info("🐘 Database connection pool initialized.")
    except Exception as e:
        logger.error("❌ Failed to initialize database pool: %s", type(e).__name__)
        app.state.db_manager = None

    try:
        # 3, 4, 5. Load model bundle, scaler, and explainer
        logger.info("🧠 Attempting to load model artifacts from %s", config.model_artefact_path)
        app.state.predictor = Predictor.load(
            config.model_artefact_path,
        )
        app.state.model_loaded = True
        logger.info("✅ Model artifacts loaded successfully.")

    except (FileNotFoundError, Exception) as e:
        logger.warning("⚠️ Model artifacts not found or failed to load: %s", type(e).__name__)
        logger.warning("⚠️ API will start in DEGRADED mode; /health/ready will return 503.")
        app.state.model_loaded = False
        app.state.predictor = None

    logger.info("🏁 Startup sequence complete. Ready for requests.")

    yield

    logger.info("🛑 Shutting down MarketPulseAI API...")
    if getattr(app.state, "db_manager", None) is not None:
        app.state.db_manager.close()


app = FastAPI(
    title="MarketPulseAI API",
    version=config.api_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

# --- Middleware ---

_cors_origins = [o.strip() for o in config.api_cors_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=("*" not in _cors_origins),
    allow_methods=["GET", "POST", "OPTIONS"]
    if not config.api_readonly_demo
    else ["GET", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def enforce_readonly_demo(request: Request, call_next: Any) -> Any:
    """Reject direct inference/mutating requests when public read-only demo mode is enabled."""
    if config.api_readonly_demo and request.method not in {"GET", "HEAD", "OPTIONS"}:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "error": "READONLY_DEMO_MODE",
                "message": "Direct prediction and write endpoints are disabled in public read-only demo mode.",
            },
        )
    return await call_next(request)


@app.middleware("http")
async def add_request_id_header(request: Request, call_next: Any) -> Any:
    import uuid

    req_id = str(uuid.uuid4())
    request.state.request_id = req_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = req_id
    return response


@app.middleware("http")
async def log_requests(request: Request, call_next: Any) -> Any:
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(
        "method=%s path=%s status=%d latency=%.4fs req_id=%s",
        request.method,
        request.url.path,
        response.status_code,
        process_time,
        getattr(request.state, "request_id", "unknown"),
    )
    return response


_rate_limits: dict[str, list[float]] = collections.defaultdict(list)
_RATE_LIMIT_BYPASS_PATHS = {
    "/",
    "/dashboard",
    "/health",
    "/api/v1/health",
    "/health/ready",
    "/api/v1/health/ready",
}


@app.middleware("http")
async def rate_limit(request: Request, call_next: Any) -> Any:
    # Bypass rate limits for liveness/readiness probes and static dashboard UI assets
    if request.url.path in _RATE_LIMIT_BYPASS_PATHS or request.url.path.startswith("/static/"):
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    if len(_rate_limits) > 1000:
        stale_ips = [
            ip for ip, ts_list in list(_rate_limits.items()) if all(now - t >= 60 for t in ts_list)
        ]
        for stale_ip in stale_ips:
            _rate_limits.pop(stale_ip, None)

    _rate_limits[client_ip] = [t for t in _rate_limits[client_ip] if now - t < 60]

    if len(_rate_limits[client_ip]) >= config.api_max_requests_per_minute:
        return JSONResponse(
            status_code=429,
            content={"error": "RATE_LIMIT_EXCEEDED", "message": "Too many requests."},
        )

    _rate_limits[client_ip].append(now)
    return await call_next(request)


# --- Exception Handlers ---
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    errors = exc.errors()
    details = {str(e["loc"]): e["msg"] for e in errors}
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "VALIDATION_ERROR",
            "message": "Request validation failed.",
            "details": details,
        },
    )


# --- Static Assets & Routers ---
_STATIC_DIR = Path(__file__).resolve().parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

# EXPLICITLY INCLUDE HEALTH ROUTER TO PREVENT 404
app.include_router(dashboard.router)
app.include_router(health.router)  # Root level for Docker healthcheck
app.include_router(health.router, prefix="/api/v1")
app.include_router(predict.router, prefix="/api/v1")
app.include_router(model.router, prefix="/api/v1")
app.include_router(pipeline_read.router, prefix="/api/v1")
