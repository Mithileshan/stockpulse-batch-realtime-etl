# StockPulse — Batch & Real-Time Stock ETL

Production-ready data pipeline for ingesting, processing, and serving stock market data using Redpanda (Kafka-compatible), PostgreSQL, and FastAPI. Supports both real-time tick streaming and aggregated OHLCV analytics.

![Redpanda](https://img.shields.io/badge/redpanda-v24.1.15-red?style=flat-square)
![Postgres](https://img.shields.io/badge/postgres-16.4-blue?style=flat-square)
![Python](https://img.shields.io/badge/python-3.11+-blue?style=flat-square)
![FastAPI](https://img.shields.io/badge/fastapi-0.111-green?style=flat-square)
![Docker](https://img.shields.io/badge/docker-compose-blue?style=flat-square)

---

## Architecture

```
Producer (simulated ticks — 6 symbols, 2s interval)
        ↓
  Redpanda (Kafka-compatible)
   └── stock.ticks.v1     (real-time tick stream, 3 partitions)
        ↓
  Consumer (Redpanda → Postgres sink)
        ↓
  PostgreSQL
   ├── stock_ticks         (raw tick data)
   └── stock_bars_1m       (1-minute OHLCV bars)
        ↑
  Aggregator (runs every 30s — ticks → bars, idempotent upsert)
        ↓
  FastAPI Analytics API (port 8000)
   ├── /ticks/latest       latest raw ticks per symbol
   ├── /ticks/summary      windowed tick aggregates
   ├── /bars/latest        latest 1m OHLCV bars
   ├── /bars/summary       period OHLCV summary
   └── /movers             top movers by % change
```

### Components

| Component | Role | Port |
|-----------|------|------|
| **Redpanda** | Kafka-compatible message broker | 9092 |
| **Console** | Kafka UI for topic/message inspection | 8080 |
| **PostgreSQL** | Persistent data store | 5432 |
| **Producer** | Simulated tick generator → Redpanda | — |
| **Consumer** | Redpanda → Postgres raw tick sink | — |
| **Aggregator** | Ticks → 1m OHLCV bars (idempotent, watermarked) | — |
| **API** | FastAPI REST analytics layer | 8000 |

---

## Project Structure

```
stockpulse-batch-realtime-etl/
├── docker-compose.yml          # Full stack orchestration
├── .env.example                # Environment variable template
├── infra/
│   ├── kafka/                  # Topic configs
│   └── postgres/
│       └── init.sql            # Schema: stock_ticks, stock_bars_1m, etl_runs
├── services/
│   ├── producer/               # Tick generator → Redpanda
│   ├── consumer/               # Redpanda → Postgres sink
│   ├── aggregator/             # Ticks → 1m OHLCV bars
│   └── api/                    # FastAPI analytics layer
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Ports 9092, 8080, 5432, 8000 available

### 1. Configure environment

```bash
cp .env.example .env
```

### 2. Start full stack

```bash
docker compose up -d --build
```

### 3. Verify all services

```bash
docker compose ps
```

All containers should be healthy/running:
- `stockpulse_redpanda` — healthy
- `stockpulse_postgres` — healthy
- `stockpulse_console` — running
- `stockpulse_producer` — running
- `stockpulse_consumer` — running
- `stockpulse_aggregator` — running
- `stockpulse_api` — healthy

### 4. Create Kafka topics (first run only)

```bash
docker exec stockpulse_redpanda rpk topic create stock.ticks.v1 -p 3
docker exec stockpulse_redpanda rpk topic create stock.bars.1m.v1 -p 3
```

### 5. Open Kafka Console

`http://localhost:8080` — inspect topics, messages, consumer groups.

### 6. Open API docs

`http://localhost:8000/docs` — Swagger UI with all endpoints.

---

## API Endpoints

### Health

```bash
curl http://localhost:8000/health
# {"status":"ok","db":"ok"}
```

### List symbols

```bash
curl http://localhost:8000/symbols
# {"symbols":["AAPL","AMZN","GOOG","MSFT","NVDA","TSLA"]}
```

### Latest ticks

```bash
curl "http://localhost:8000/ticks/latest?symbol=AAPL&limit=5"
```

```json
{
  "symbol": "AAPL",
  "count": 5,
  "ticks": [
    {"symbol": "AAPL", "price": 189.72, "volume": 8341, "event_time": "2026-02-27T12:05:00Z"},
    ...
  ]
}
```

### Tick summary (windowed)

```bash
curl "http://localhost:8000/ticks/summary?symbol=AAPL&minutes=5"
```

```json
{
  "symbol": "AAPL",
  "window_minutes": 5,
  "count": 150,
  "avg_price": 189.84,
  "min_price": 189.52,
  "max_price": 190.11,
  "sum_volume": 1243500,
  "start_time": "2026-02-27T12:00:00Z",
  "end_time": "2026-02-27T12:05:00Z"
}
```

### Latest 1m bars

```bash
curl "http://localhost:8000/bars/latest?symbol=AAPL&limit=10"
```

```json
{
  "symbol": "AAPL",
  "count": 10,
  "bars": [
    {
      "symbol": "AAPL",
      "bucket_start": "2026-02-27T12:05:00Z",
      "open": 189.72,
      "high": 190.11,
      "low": 189.52,
      "close": 189.98,
      "volume_sum": 87400,
      "tick_count": 30
    },
    ...
  ]
}
```

### Bar summary (period OHLCV)

```bash
curl "http://localhost:8000/bars/summary?symbol=AAPL&minutes=60"
```

```json
{
  "symbol": "AAPL",
  "window_minutes": 60,
  "bar_count": 60,
  "open": 189.50,
  "high": 190.75,
  "low": 188.90,
  "close": 190.12,
  "change_pct": 0.3278,
  "total_volume": 5240000,
  "total_ticks": 1800
}
```

### Top movers

```bash
curl "http://localhost:8000/movers?minutes=5&limit=5"
```

```json
{
  "window_minutes": 5,
  "movers": [
    {"symbol": "NVDA", "price_open": 874.50, "price_close": 876.20, "change_pct": 0.1944},
    {"symbol": "TSLA", "price_open": 244.80, "price_close": 245.60, "change_pct": 0.3268},
    ...
  ]
}
```

---

## Database Schema

### `stock_ticks`
Raw tick data from the producer.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGSERIAL | Primary key |
| `symbol` | VARCHAR(10) | Ticker symbol |
| `price` | NUMERIC(12,4) | Tick price |
| `volume` | BIGINT | Tick volume |
| `event_time` | TIMESTAMPTZ | Event timestamp |

### `stock_bars_1m`
1-minute OHLCV aggregated bars. Idempotent upsert on `(symbol, bucket_start)`.

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | VARCHAR(10) | Ticker symbol |
| `bucket_start` | TIMESTAMPTZ | Minute bucket start |
| `open/high/low/close` | NUMERIC(12,4) | OHLCV prices |
| `volume_sum` | BIGINT | Total volume in bucket |
| `tick_count` | INTEGER | Number of ticks aggregated |

### `etl_runs`
Audit log + aggregator watermark.

---

## Verification

```bash
# Confirm ticks are flowing
docker compose logs consumer --tail 20

# Confirm aggregator is creating bars
docker compose logs aggregator --tail 20

# Query bars directly
docker exec -it stockpulse_postgres psql -U stockpulse -d stockpulse \
  -c "SELECT symbol, COUNT(*) FROM stock_bars_1m GROUP BY symbol ORDER BY symbol;"

# Check watermark
docker exec -it stockpulse_postgres psql -U stockpulse -d stockpulse \
  -c "SELECT source, records_processed, completed_at FROM etl_runs ORDER BY id DESC LIMIT 5;"
```

---

## Phases

- **Phase 1** ✅ Complete: Production-ready local infrastructure
  - ✅ Redpanda v24.1.15 (Kafka-compatible, single-node)
  - ✅ Redpanda Console v2.6.1 (Kafka UI)
  - ✅ PostgreSQL 16.4 with persistent volume
  - ✅ Pinned versions, healthchecks, env management
  - ✅ Topic naming convention established

- **Phase 2** ✅ Complete: Data ingestion services
  - ✅ Producer: simulated tick generator → Redpanda (6 symbols, 2s interval)
  - ✅ Consumer: Redpanda → PostgreSQL sink (confluent-kafka + psycopg2)
  - ✅ DB schema: `stock_ticks` + `etl_runs` tables, auto-init

- **Phase 3** ✅ Complete: Analytics API
  - ✅ FastAPI 0.111 with Swagger UI
  - ✅ SQLAlchemy 2.0 + connection pooling
  - ✅ Endpoints: `/health`, `/symbols`, `/ticks/latest`, `/ticks/summary`
  - ✅ Input validation + consistent error response format

- **Phase 4** ✅ Complete: Aggregation pipeline
  - ✅ `stock_bars_1m` table with UNIQUE(symbol, bucket_start) constraint
  - ✅ Aggregator service: ticks → 1m OHLCV bars every 30s
  - ✅ Idempotent upsert — safe to re-run without duplicates
  - ✅ Watermark persisted in `etl_runs` for incremental processing
  - ✅ Bar endpoints: `/bars/latest`, `/bars/summary`, `/movers`

- **Phase 5** (Planned): Real data ingestion
  - AlphaVantage / yFinance integration
  - Replace simulated producer with live market data

- **Phase 6** (Planned): CI + tests
  - Pytest integration tests
  - GitHub Actions pipeline

---

## Author

[Mithileshan](https://github.com/Mithileshan)

---

## License

MIT License
