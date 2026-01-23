from __future__ import annotations

import time
from typing import Literal

from fastapi import APIRouter, Request, Response
from api.schemas.response import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def get_health(request: Request, response: Response) -> HealthResponse:
    """Check API liveness and core dependency status.
    
    Returns 200 OK even if degraded, so Docker/K8s knows the process is alive.
    """
    app = request.app
    
    uptime = int(time.time() - getattr(app.state, "start_time", time.time()))
    model_loaded = getattr(app.state, "model_loaded", False)
    
    # Check DB connection liveness
    db_connected = False
    try:
        if hasattr(app.state, "db_manager"):
            with app.state.db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                db_connected = True
    except Exception:
        pass
        
    status: Literal["healthy", "degraded", "unhealthy"] = "healthy"
    if not model_loaded or not db_connected:
        status = "degraded"
        # We return 200 even for degraded status to prevent unnecessary container restarts
        # if the system is still functional (e.g. model training in progress)
        response.status_code = 200
        
    return HealthResponse(
        status=status,
        model_loaded=model_loaded,
        db_connected=db_connected,
        uptime_seconds=uptime,
        version="1.0.0"
    )
