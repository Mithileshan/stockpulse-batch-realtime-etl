import re
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db

router = APIRouter()

SYMBOL_RE = re.compile(r"^[A-Za-z]{1,10}$")


def validate_symbol(symbol: str) -> str:
    if not SYMBOL_RE.match(symbol):
        raise HTTPException(
            status_code=422,
            detail={"error": {"code": "VALIDATION_ERROR", "message": "symbol must be 1-10 letters"}},
        )
    return symbol.upper()


@router.get("/symbols")
def get_symbols(db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT DISTINCT symbol FROM stock_ticks ORDER BY symbol")
    ).fetchall()
    return {"symbols": [r[0] for r in rows]}


@router.get("/ticks/latest")
def latest_ticks(
    symbol: str = Query(..., description="Stock ticker symbol"),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    symbol = validate_symbol(symbol)
    rows = db.execute(
        text("""
            SELECT symbol, price, volume, event_time
            FROM stock_ticks
            WHERE symbol = :symbol
            ORDER BY event_time DESC
            LIMIT :limit
        """),
        {"symbol": symbol, "limit": limit},
    ).fetchall()
    return {
        "symbol": symbol,
        "count": len(rows),
        "ticks": [
            {
                "symbol": r[0],
                "price": float(r[1]),
                "volume": r[2],
                "event_time": r[3].isoformat(),
            }
            for r in rows
        ],
    }


@router.get("/ticks/summary")
def tick_summary(
    symbol: str = Query(..., description="Stock ticker symbol"),
    minutes: int = Query(5, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    symbol = validate_symbol(symbol)
    row = db.execute(
        text("""
            SELECT
                COUNT(*)               AS count,
                ROUND(AVG(price)::numeric, 4)  AS avg_price,
                MIN(price)             AS min_price,
                MAX(price)             AS max_price,
                SUM(COALESCE(volume, 0)) AS sum_volume,
                MIN(event_time)        AS start_time,
                MAX(event_time)        AS end_time
            FROM stock_ticks
            WHERE symbol = :symbol
              AND event_time >= NOW() - (:minutes * INTERVAL '1 minute')
        """),
        {"symbol": symbol, "minutes": minutes},
    ).fetchone()

    if not row or not row[0]:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "NOT_FOUND", "message": f"No data for {symbol} in last {minutes} minutes"}},
        )

    return {
        "symbol": symbol,
        "window_minutes": minutes,
        "count": row[0],
        "avg_price": float(row[1]) if row[1] else None,
        "min_price": float(row[2]) if row[2] else None,
        "max_price": float(row[3]) if row[3] else None,
        "sum_volume": row[4],
        "start_time": row[5].isoformat() if row[5] else None,
        "end_time": row[6].isoformat() if row[6] else None,
    }
