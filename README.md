# StockPulse — Batch & Real-Time Stock ETL

Production-ready data pipeline for ingesting, processing, and serving stock market data using Redpanda (Kafka-compatible), PostgreSQL, and FastAPI. Supports both real-time tick streaming and batch historical backfill.

![Redpanda](https://img.shields.io/badge/redpanda-v24.1.15-red?style=flat-square)
![Postgres](https://img.shields.io/badge/postgres-16.4-blue?style=flat-square)
![Python](https://img.shields.io/badge/python-3.11+-blue?style=flat-square)
![Docker](https://img.shields.io/badge/docker-compose-blue?style=flat-square)

---

## Architecture

```
Stock Data Provider (AlphaVantage / yFinance)
        ↓
  Producer Service
        ↓
  Redpanda (Kafka-compatible)
   ├── stock.ticks.v1     (real-time tick stream)
   └── stock.bars.1m.v1   (1-minute OHLCV bars)
        ↓
  Consumer Service
        ↓
  PostgreSQL (persistent store)
        ↓
  FastAPI (analytics API)
```

### Components

| Component | Role | Port |
|-----------|------|------|
| **Redpanda** | Kafka-compatible message broker | 9092 |
| **Console** | Kafka UI for topic/message inspection | 8080 |
| **PostgreSQL** | Persistent data store | 5432 |
| **Producer** | Ingests stock data → publishes to topics | — |
| **Consumer** | Subscribes to topics → sinks to Postgres | — |
| **FastAPI** | REST analytics API | 8000 |

---

## Project Structure

```
stockpulse-batch-realtime-etl/
├── docker-compose.yml          # Infrastructure orchestration
├── .env.example                # Environment variable template
├── infra/
│   ├── kafka/                  # Topic configs
│   └── postgres/               # DB init scripts
├── services/
│   ├── producer/               # Stock data ingestion
│   ├── consumer/               # Stream → Postgres sink
│   └── api/                    # FastAPI analytics layer
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Ports 9092, 8080, 5432 available

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set ALPHAVANTAGE_API_KEY for Phase 2+
```

### 2. Start infrastructure

```bash
docker compose up -d
```

### 3. Verify services

```bash
docker compose ps
```

All 3 containers should be healthy:
- `stockpulse_redpanda` — healthy
- `stockpulse_console` — running
- `stockpulse_postgres` — healthy

### 4. Create Kafka topics

```bash
docker exec stockpulse_redpanda rpk topic create stock.ticks.v1 -p 3
docker exec stockpulse_redpanda rpk topic create stock.bars.1m.v1 -p 3
```

### 5. Smoke test — produce & consume

```bash
# Produce a test message (pipe value via stdin)
echo '{"symbol":"AAPL","price":190.12,"ts":"2026-02-27T12:00:00Z"}' | \
  docker exec -i stockpulse_redpanda rpk topic produce stock.ticks.v1 -k AAPL

# Consume it back
docker exec stockpulse_redpanda rpk topic consume stock.ticks.v1 -n 1
```

### 6. Open Kafka Console

`http://localhost:8080` — inspect topics, messages, consumer groups.

---

## Topic Schema

### `stock.ticks.v1`
```json
{
  "symbol": "AAPL",
  "price": 190.12,
  "volume": 1500,
  "ts": "2026-02-27T12:00:00Z"
}
```

### `stock.bars.1m.v1`
```json
{
  "symbol": "AAPL",
  "open": 190.00,
  "high": 190.50,
  "low": 189.80,
  "close": 190.12,
  "volume": 45000,
  "ts": "2026-02-27T12:00:00Z"
}
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
  - ✅ DB schema: `stock_ticks` table + `etl_runs` audit table
  - ✅ Auto-init via `docker-entrypoint-initdb.d/init.sql`

- **Phase 3** (Planned): Analytics API
  - FastAPI with OHLCV endpoints
  - Aggregation queries
  - Structured logging + CI

---

## Author

[Mithileshan](https://github.com/Mithileshan)

---

## License

MIT License
