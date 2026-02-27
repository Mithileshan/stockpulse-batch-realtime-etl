CREATE TABLE IF NOT EXISTS stock_ticks (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price NUMERIC(12,4) NOT NULL,
    volume BIGINT,
    event_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_stock_ticks_symbol_time
ON stock_ticks (symbol, event_time DESC);

CREATE TABLE IF NOT EXISTS etl_runs (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50),
    records_processed INTEGER,
    status VARCHAR(20),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);
