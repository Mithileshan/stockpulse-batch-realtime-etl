from fastapi import FastAPI
from app.routers import health, ticks, bars

app = FastAPI(
    title="StockPulse Analytics API",
    description="Real-time and aggregated stock market data analytics",
    version="1.0.0",
)

app.include_router(health.router, tags=["Health"])
app.include_router(ticks.router, tags=["Ticks"])
app.include_router(bars.router, tags=["Bars"])
