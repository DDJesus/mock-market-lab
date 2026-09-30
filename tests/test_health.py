from decimal import Decimal
from market_api.main import create_app
import time

from fastapi.testclient import TestClient
from market_api.market import MarketEngine


def test_health():
    with TestClient(create_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_symbols():
    with TestClient(create_app()) as client:
        response = client.get("/api/v1/symbols")

    assert response.status_code == 200
    assert response.json() == {
        "symbols": ["CYDE", "IKOR", "RELL", "UIM", "RBN"]
    }


def test_market_snapshot():
    with TestClient(create_app()) as client:
        response = client.get("/api/v1/market/snapshot")

    assert response.status_code == 200

    body = response.json()

    assert "as_of" in body
    assert len(body["market"]) == 5

    cyde = body["market"][0]
    assert cyde == {
        "symbol": "CYDE",
        "price": Decimal("184.25"),
        "volume": 0,
    }


def test_market_moves_without_manual_tick():
    with TestClient(create_app()) as client:
        before = client.get("/api/v1/market/snapshot").json()

        time.sleep(1.1)

        after = client.get("/api/v1/market/snapshot").json()

    before_market = {
        item["symbol"]: item
        for item in before["market"]
    }

    after_market = {
        item["symbol"]: item
        for item in after["market"]
    }

    assert any(
        after_market[symbol]["volume"] > before_market[symbol]["volume"]
        for symbol in before_market
    )


def test_market_publishes_trade_to_subscriber():
    market = MarketEngine(seed=42)

    subscriber = market.subscribe()

    event = market.tick()
    received = subscriber.get_nowait()

    assert received == event
    assert received.sequence == 1
    assert received.symbol in {"CYDE", "IKOR", "RELL", "UIM", "RBN"}
    assert received.volume > 0