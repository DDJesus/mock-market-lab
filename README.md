# Mock Market Data Lab

A synthetic market-data platform built to model real data-engineering problems across streaming and batch workflows.

The project provides a small market-data producer that generates canonical trade events, persists them to SQLite, exposes them as a live SSE stream, and publishes deterministic daily CSV batches from the same underlying event history.

## Current Architecture

```text
Synthetic Market Engine
        |
        v
    TradeEvent
        |
        v
      SQLite
     /      \
    /        \
  SSE      Daily CSV
 stream     publisher
              |
              v
          Batch API
```

## Implemented
- Synthetic multi-symbol market generator
- Canonical trade-event schema
- Globally unique event IDs
- Monotonic sequence numbers for gap detection
- Persistence-before-publish semantics
- SQLite-backed event history
- Sequence recovery across application restarts
- Server-Sent Events streaming endpoint
- Daily CSV materialization
- America/New_York market-date boundaries with DST awareness
- Automatic 06:00 ET publication of the previous completed market day
- Batch discovery and download endpoints
- FastAPI lifespan-managed producer and publisher workers
- Automated unit and integration tests

## Why This Exists
The goal is to build a realistic but fully synthetic environment for practicing and demonstrating data-engineering patterns without depending on a commercial market-data provider.
A key design choice is that batch and streaming interfaces represent the same canonical event world. Streaming consumers can process events in real time, while retained daily artifacts provide an independent path for recovery, reconciliation, and historical processing.

## Planned Consumer Pipeline
The producer is only one side of the lab. Planned downstream work includes:
```text
API / Batch Artifacts
        |
        v
Raw Landing
        |
        v
Validation / Normalization
        |
        v
Parquet
        |
        v
DuckDB / Analytics
```

Future experiments will include checkpointing, idempotent loading, batch-to-stream reconciliation, gap recovery, orchestration, containers, and additional streaming infrastructure.

## Running Locally
Requires Python 3.12+.
```text
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[test]"
```
Run the API:
```text
python -m uvicorn market_api.main:app --host 127.0.0.1 --port 8100
```
Run the tests:
```text
python -m pytest -v --basetemp=.pytest-tmp
```

## API
Live stream:
```text
GET /api/v1/stream
```
Current market snapshot:
```text
GET /api/v1/market/snapshot
```
Available batch artifacts:
```text
GET /api/v1/batches
```
Download a daily batch:
```text
GET /api/v1/batches/{market_date}
```

## Status
The local producer side is functional and tested. The next phase is deployment as an independent service followed by development of downstream batch and streaming consumers.
