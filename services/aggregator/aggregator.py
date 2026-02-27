#!/usr/bin/env python3
"""
StockPulse Aggregator — Tick → 1-minute OHLCV bars
Reads stock_ticks, computes 1m OHLCV bars, upserts into stock_bars_1m.
Stores watermark in etl_runs to support idempotent re-runs.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "stockpulse"),
    "user": os.getenv("POSTGRES_USER", "stockpulse"),
    "password": os.getenv("POSTGRES_PASSWORD", "stockpulse_pass"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
}

INTERVAL = int(os.getenv("AGGREGATE_INTERVAL", "30"))

AGGREGATE_SQL = """
    SELECT
        symbol,
        date_trunc('minute', event_time)               AS bucket_start,
        (array_agg(price ORDER BY event_time ASC))[1]  AS open,
        MAX(price)                                     AS high,
        MIN(price)                                     AS low,
        (array_agg(price ORDER BY event_time DESC))[1] AS close,
        SUM(COALESCE(volume, 0))                       AS volume_sum,
        COUNT(*)                                       AS tick_count
    FROM stock_ticks
    WHERE event_time >= %s AND event_time < %s
    GROUP BY symbol, date_trunc('minute', event_time)
"""

UPSERT_SQL = """
    INSERT INTO stock_bars_1m (symbol, bucket_start, open, high, low, close, volume_sum, tick_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (symbol, bucket_start) DO UPDATE SET
        open        = EXCLUDED.open,
        high        = EXCLUDED.high,
        low         = EXCLUDED.low,
        close       = EXCLUDED.close,
        volume_sum  = EXCLUDED.volume_sum,
        tick_count  = EXCLUDED.tick_count
"""


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": "aggregator",
            "msg": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def _setup_logger(name: str) -> logging.Logger:
    _logger = logging.getLogger(name)
    _logger.setLevel(logging.INFO)
    _logger.handlers = []
    handler = logging.StreamHandler()
    handler.setFormatter(_JSONFormatter())
    _logger.addHandler(handler)
    _logger.propagate = False
    return _logger


logger = _setup_logger("aggregator")


def connect_db(retries: int = 10, delay: int = 3) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            logger.info(json.dumps({"event": "db_connected", "attempt": attempt}))
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(json.dumps({"event": "db_connect_failed", "attempt": attempt, "error": str(e)}))
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL after retries")


def get_watermark(cursor) -> datetime | None:
    cursor.execute("""
        SELECT completed_at FROM etl_runs
        WHERE source = 'aggregator' AND status = 'complete'
        ORDER BY completed_at DESC LIMIT 1
    """)
    row = cursor.fetchone()
    if row:
        return row[0]
    # No watermark yet — start from earliest available tick
    cursor.execute("SELECT MIN(event_time) FROM stock_ticks")
    row = cursor.fetchone()
    return row[0] if row and row[0] else None


def save_watermark(cursor, to_time: datetime, records: int) -> None:
    cursor.execute("""
        INSERT INTO etl_runs (source, records_processed, status, started_at, completed_at)
        VALUES ('aggregator', %s, 'complete', NOW(), %s)
    """, (records, to_time))


def run_aggregation(db: psycopg2.extensions.connection) -> None:
    with db.cursor() as cur:
        from_time = get_watermark(cur)
        if from_time is None:
            logger.info(json.dumps({"event": "no_ticks_yet"}))
            return

        # Only process completed minutes — exclude the current in-progress minute
        to_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        if from_time >= to_time:
            return  # Nothing new to aggregate

        cur.execute(AGGREGATE_SQL, (from_time, to_time))
        rows = cur.fetchall()

        for row in rows:
            cur.execute(UPSERT_SQL, row)

        save_watermark(cur, to_time, len(rows))
        db.commit()

        if rows:
            logger.info(json.dumps({"event": "bars_upserted", "count": len(rows), "from": from_time.isoformat(), "to": to_time.isoformat()}))


def main():
    logger.info(json.dumps({"event": "startup", "interval_s": INTERVAL}))
    db = connect_db()

    try:
        while True:
            try:
                run_aggregation(db)
            except Exception as e:
                logger.error(json.dumps({"event": "aggregation_error", "error": str(e)}))
                db.rollback()
            time.sleep(INTERVAL)
    except KeyboardInterrupt:
        logger.info(json.dumps({"event": "shutdown"}))
    finally:
        db.close()


if __name__ == "__main__":
    main()
