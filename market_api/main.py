import asyncio
import json
import queue
import threading

from contextlib import asynccontextmanager
from datetime import datetime, timezone, date
from dataclasses import asdict
from decimal import Decimal
from fastapi import Request, FastAPI, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from pathlib import Path
from pydantic import BaseModel

from market_api.database import Database
from market_api.market import MarketEngine
from market_api.batch import BatchWriter
from market_api.scheduler import BatchPublisher, BatchScheduler


class Symbol(BaseModel):
    symbol: str
    price: Decimal
    volume: int


def create_app(
            database_path: str | Path = Path("data") / "market.sqlite3",
            batch_dir: str | Path = Path("data") / "batches",
        ) -> FastAPI:
    """
    Create a FastAPI application for the mock market data API.

    :param database_path: The path to the SQLite database file. Can be changed to a different database if desired.
    :param batch_dir: The directory where batch files will be stored.
    """
    database = Database(database_path)
    batch_dir = Path(batch_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):

        database.initialize()  # 1. Initialize the database, creating the trades table if it doesn't exist.

        # Construct the market.
        market = MarketEngine(
            database=database,
            initial_sequence=database.latest_sequence(),
        )

        # Construct batch components.
        batch_writer = BatchWriter(
            database=database,
            output_dir=batch_dir,
        )

        batch_scheduler = BatchScheduler(batch_writer)

        # Construct shutdown signals.
        stop_event = threading.Event()
        batch_stop_event = threading.Event()

        batch_publisher = BatchPublisher(
            scheduler=batch_scheduler,
            stop_event=batch_stop_event,
        )

        # Construct worker threads.
        producer = threading.Thread(
            target=market.run,
            args=(stop_event,),
            kwargs={"interval": 0.5},
            name="market-producer",
            daemon=True,
        )

        # Construct batch publisher thread.
        batch_thread = threading.Thread(
            target=batch_publisher.run,
            name="batch-publisher",
            daemon=True,
        )

        # Expose application state.
        app.state.market = market
        app.state.database = database
        app.state.batch_dir = batch_dir
        app.state.producer_thread = producer
        app.state.batch_thread = batch_thread

        # Start workers.
        producer.start()
        batch_thread.start()

        try:
            yield
        finally:
            # Signal both workers and wait for shutdown.
            stop_event.set()
            batch_stop_event.set()

            producer.join(timeout=2)
            batch_thread.join(timeout=2)

    app = FastAPI(
        title="Mock Market Data API",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/api/v1/stream")
    async def stream(request: Request):
        """
        Stream trade events in real-time.
        """
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
        """
        List all available batch files. Batch files are generated daily and can be accessed at /api/v1/batches/{market_date}.
        """
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
        """
        Retrieve a specific batch file.
        """
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