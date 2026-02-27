from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db

router = APIRouter()


@router.get("/health")
def health(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    return {"status": "ok", "db": "ok"}


@router.get("/ready")
def ready(db: Session = Depends(get_db)):
    """Deep health check — verifies DB connectivity and data presence."""
    db.execute(text("SELECT 1"))
    ticks_count = db.execute(text("SELECT COUNT(*) FROM stock_ticks")).scalar()
    bars_count = db.execute(text("SELECT COUNT(*) FROM stock_bars_1m")).scalar()
    return {
        "status": "ready",
        "checks": {
            "db": "ok",
            "stock_ticks": ticks_count,
            "stock_bars_1m": bars_count,
        },
    }


@router.get("/version")
def version():
    return {"version": "1.0.0", "service": "stockpulse-api"}
