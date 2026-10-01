from datetime import datetime, timezone, date
from decimal import Decimal
from contextlib import asynccontextmanager
import threading
import json
import queue
from dataclasses import asdict
import asyncio
from pathlib import Path
from fastapi import Request, FastAPI, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel

from market_api import database
from market_api.database import Database
from market_api.market import MarketEngine


class Symbol(BaseModel):
    symbol: str
    price: Decimal
    volume: int


def create_app(
            database_path: str | Path = Path("data") / "market.sqlite3",
            batch_dir: str | Path = Path("data") / "batches",
        ) -> FastAPI:
    database = Database(database_path)
    batch_dir = Path(batch_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database.initialize()

        market = MarketEngine(
            database=database,
            initial_sequence=database.latest_sequence(),
        )

        app.state.market = market
        app.state.database = database
        app.state.batch_dir = batch_dir

        stop_event = threading.Event()

        producer = threading.Thread(
            target=market.run,
            args=(stop_event,),
            kwargs={"interval": 0.5},
            name="market-producer",
            daemon=True,
        )

        producer.start()

        try:
            yield
        finally:
            stop_event.set()
            producer.join(timeout=2)

    app = FastAPI(
        title="Mock Market Data API",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/api/v1/stream")
    async def stream(request: Request):
        subscriber = app.state.market.subscribe()

        async def event_generator():
            try:
                while True:
                    if await request.is_disconnected():
                        break

                    try:
                        event = await asyncio.to_thread(
                            subscriber.get,
                            True,
                            1.0,
                        )
                    except queue.Empty:
                        continue

                    payload = asdict(event)
                    payload["price"] = str(payload["price"])

                    yield (
                        f"id: {event.sequence}\n"
                        f"event: trade\n"
                        f"data: {json.dumps(payload)}\n\n"
                    )
            finally:
                app.state.market.unsubscribe(subscriber)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )

    @app.get("/api/v1/batches")
    async def list_batches():
        batch_dir = app.state.batch_dir

        if not batch_dir.exists():
            return {"batches": []}

        batches = sorted(
            path.name
            for path in batch_dir.glob("market-trades-*.csv")
            if path.is_file()
        )

        return {"batches": batches}

    @app.get("/api/v1/batches/{market_date}")
    async def get_batch(market_date: date):
        filename = f"market-trades-{market_date.isoformat()}.csv"
        batch_path = app.state.batch_dir / filename

        if not batch_path.is_file():
            raise HTTPException(
                status_code=404,
                detail=f"No batch available for {market_date.isoformat()}",
            )

        return FileResponse(
            path=batch_path,
            media_type="text/csv",
            filename=filename,
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/v1/symbols")
    def symbols():
        return {"symbols": app.state.market.symbols()}

    @app.get("/api/v1/market/snapshot")
    def snapshot():
        return {
            "as_of": datetime.now(timezone.utc),
            "market": app.state.market.snapshot(),
        }

    return app


app = create_app()