from __future__ import annotations

import logging
import time
import collections
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings, SettingsConfigDict

from api.routers import health, model, predict
from consumer.db import DatabaseConfig, DatabaseManager
from model.predictor import Predictor

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
    model_artefact_path: str = "model/artefacts/xgb_model.json"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )


config = ApiConfig()


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    """7-step startup sequence with graceful handling for missing artifacts."""
    app.state.start_time = time.time()
    logger.info("🚀 Starting up MarketPulseAI API...")

    # 1. Validate environment
    db_config = DatabaseConfig()  # type: ignore

    # 2. Initialise PostgreSQL connection pool
    try:
        app.state.db_manager = DatabaseManager(db_config)
        logger.info("🐘 Database connection pool initialized.")
    except Exception as e:
        logger.error("❌ Failed to initialize database pool: %s", e)
        # We allow startup to continue; healthcheck will reflect the failure.

    try:
        # 3, 4, 5. Load model, scaler, and SHAP explainer
        logger.info("🧠 Attempting to load model artifacts from %s", config.model_artefact_path)
        app.state.predictor = Predictor.load("model/artefacts")
        
        # 6. Set app.state.model_loaded = True
        app.state.model_loaded = True
        logger.info("✅ Model artifacts loaded successfully.")
        
    except (FileNotFoundError, Exception) as e:
        # Graceful degradation if Phase 4 (Training) hasn't been run
        logger.warning("⚠️ Model artifacts not found or failed to load: %s", e)
        logger.warning("⚠️ API will start in DEGRADED mode. Predictions will return 500.")
        app.state.model_loaded = False
        app.state.predictor = None

    # 7. Log completion
    logger.info("🏁 Startup sequence complete. Ready for requests.")

    yield

    # Shutdown sequence
    logger.info("🛑 Shutting down MarketPulseAI API...")
    if hasattr(app.state, "db_manager"):
        app.state.db_manager.close()


app = FastAPI(
    title="MarketPulseAI API",
    version=config.api_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

# --- Middleware ---

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.api_cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

@app.middleware("http")
async def rate_limit(request: Request, call_next: Any) -> Any:
    # Bypass rate limits for health checks
    if request.url.path in ["/health", "/api/v1/health"]:
        return await call_next(request)
        
    client_ip = request.client.host if request.client else "unknown"
    now = time.time()
    _rate_limits[client_ip] = [t for t in _rate_limits[client_ip] if now - t < 60]
    
    if len(_rate_limits[client_ip]) >= config.api_max_requests_per_minute:
        return JSONResponse(
            status_code=429,
            content={"error": "RATE_LIMIT_EXCEEDED", "message": "Too many requests."}
        )
        
    _rate_limits[client_ip].append(now)
    return await call_next(request)


# --- Exception Handlers ---
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
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


# --- Routers ---
# EXPLICITLY INCLUDE HEALTH ROUTER TO PREVENT 404
app.include_router(health.router) # Root level for Docker healthcheck
app.include_router(health.router, prefix="/api/v1")
app.include_router(predict.router, prefix="/api/v1")
app.include_router(model.router, prefix="/api/v1")
