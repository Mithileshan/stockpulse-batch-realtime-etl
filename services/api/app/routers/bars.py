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


@router.get("/bars/latest")
def latest_bars(
    symbol: str = Query(..., description="Stock ticker symbol"),
    limit: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    symbol = validate_symbol(symbol)
    rows = db.execute(
        text("""
            SELECT symbol, bucket_start, open, high, low, close, volume_sum, tick_count
            FROM stock_bars_1m
            WHERE symbol = :symbol
            ORDER BY bucket_start DESC
            LIMIT :limit
        """),
        {"symbol": symbol, "limit": limit},
    ).fetchall()
    return {
        "symbol": symbol,
        "count": len(rows),
        "bars": [
            {
                "symbol": r[0],
                "bucket_start": r[1].isoformat(),
                "open": float(r[2]),
                "high": float(r[3]),
                "low": float(r[4]),
                "close": float(r[5]),
                "volume_sum": r[6],
                "tick_count": r[7],
            }
            for r in rows
        ],
    }


@router.get("/bars/summary")
def bars_summary(
    symbol: str = Query(..., description="Stock ticker symbol"),
    minutes: int = Query(60, ge=1, le=1440),
    db: Session = Depends(get_db),
):
    symbol = validate_symbol(symbol)
    row = db.execute(
        text("""
            SELECT
                COUNT(*)                        AS bar_count,
                (array_agg(open ORDER BY bucket_start ASC))[1]   AS period_open,
                MAX(high)                       AS period_high,
                MIN(low)                        AS period_low,
                (array_agg(close ORDER BY bucket_start DESC))[1] AS period_close,
                SUM(volume_sum)                 AS total_volume,
                SUM(tick_count)                 AS total_ticks,
                MIN(bucket_start)               AS start_time,
                MAX(bucket_start)               AS end_time
            FROM stock_bars_1m
            WHERE symbol = :symbol
              AND bucket_start >= NOW() - (:minutes * INTERVAL '1 minute')
        """),
        {"symbol": symbol, "minutes": minutes},
    ).fetchone()

    if not row or not row[0]:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "NOT_FOUND", "message": f"No bars for {symbol} in last {minutes} minutes"}},
        )

    open_p = float(row[1]) if row[1] else None
    close_p = float(row[4]) if row[4] else None
    change_pct = (
        round((close_p - open_p) / open_p * 100, 4)
        if open_p and close_p and open_p != 0
        else None
    )

    return {
        "symbol": symbol,
        "window_minutes": minutes,
        "bar_count": row[0],
        "open": open_p,
        "high": float(row[2]) if row[2] else None,
        "low": float(row[3]) if row[3] else None,
        "close": close_p,
        "change_pct": change_pct,
        "total_volume": row[5],
        "total_ticks": row[6],
        "start_time": row[7].isoformat() if row[7] else None,
        "end_time": row[8].isoformat() if row[8] else None,
    }


@router.get("/movers")
def top_movers(
    minutes: int = Query(5, ge=1, le=1440),
    limit: int = Query(5, ge=1, le=20),
    db: Session = Depends(get_db),
):
    rows = db.execute(
        text("""
            WITH first_bar AS (
                SELECT DISTINCT ON (symbol)
                    symbol, open
                FROM stock_bars_1m
                WHERE bucket_start >= NOW() - (:minutes * INTERVAL '1 minute')
                ORDER BY symbol, bucket_start ASC
            ),
            last_bar AS (
                SELECT DISTINCT ON (symbol)
                    symbol, close
                FROM stock_bars_1m
                WHERE bucket_start >= NOW() - (:minutes * INTERVAL '1 minute')
                ORDER BY symbol, bucket_start DESC
            )
            SELECT
                f.symbol,
                f.open        AS price_open,
                l.close       AS price_close,
                ROUND(((l.close - f.open) / NULLIF(f.open, 0) * 100)::numeric, 4) AS change_pct
            FROM first_bar f
            JOIN last_bar l ON f.symbol = l.symbol
            ORDER BY ABS(change_pct) DESC NULLS LAST
            LIMIT :limit
        """),
        {"minutes": minutes, "limit": limit},
    ).fetchall()

    return {
        "window_minutes": minutes,
        "movers": [
            {
                "symbol": r[0],
                "price_open": float(r[1]),
                "price_close": float(r[2]),
                "change_pct": float(r[3]) if r[3] is not None else None,
            }
            for r in rows
        ],
    }
