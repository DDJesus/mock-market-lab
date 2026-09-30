from datetime import datetime, timezone
from decimal import Decimal
from contextlib import asynccontextmanager
import threading
import json
import queue
from dataclasses import asdict
import asyncio

from fastapi import Request, FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel


from market_api.market import MarketEngine


class Symbol(BaseModel):
    symbol: str
    price: Decimal
    volume: int


def create_app():
    market = MarketEngine()
    stop_event = threading.Event()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.market = market

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