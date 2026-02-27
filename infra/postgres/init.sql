CREATE TABLE IF NOT EXISTS stock_ticks (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price NUMERIC(12,4) NOT NULL,
    volume BIGINT,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_ticks_symbol_time
ON stock_ticks (symbol, event_time DESC);

CREATE TABLE IF NOT EXISTS stock_bars_1m (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    bucket_start TIMESTAMPTZ NOT NULL,
    open NUMERIC(12,4) NOT NULL,
    high NUMERIC(12,4) NOT NULL,
    low NUMERIC(12,4) NOT NULL,
    close NUMERIC(12,4) NOT NULL,
    volume_sum BIGINT NOT NULL DEFAULT 0,
    tick_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_bars_symbol_bucket UNIQUE (symbol, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_stock_bars_1m_symbol_bucket
ON stock_bars_1m (symbol, bucket_start DESC);

CREATE TABLE IF NOT EXISTS etl_runs (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50),
    records_processed INTEGER,
    status VARCHAR(20),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
