import json
import logging
import time
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.routers import bars, health, ticks


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "api",
            "msg": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


_handler = logging.StreamHandler()
_handler.setFormatter(_JSONFormatter())
logger = logging.getLogger("stockpulse.api")
logger.setLevel(logging.INFO)
logger.handlers = [_handler]
logger.propagate = False

app = FastAPI(
    title="StockPulse Analytics API",
    description="Real-time and aggregated stock market data analytics",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    latency_ms = round((time.time() - start) * 1000, 2)
    logger.info(json.dumps({
        "event": "http_request",
        "method": request.method,
        "path": request.url.path,
        "status": response.status_code,
        "latency_ms": latency_ms,
    }))
    return response


app.include_router(health.router, tags=["Health"])
app.include_router(ticks.router, tags=["Ticks"])
app.include_router(bars.router, tags=["Bars"])

# Prometheus metrics — exposed at /metrics
Instrumentator().instrument(app).expose(app)
