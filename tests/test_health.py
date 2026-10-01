from decimal import Decimal
from market_api.main import create_app
import time
import pytest

from fastapi.testclient import TestClient
from market_api.market import MarketEngine
from market_api.main import create_app


@pytest.fixture
def client(tmp_path):
    app = create_app(
        database_path=tmp_path / "test-market.sqlite3"
    )

    with TestClient(app) as client:
        yield client


def test_health(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_symbols(client):
    response = client.get("/api/v1/symbols")

    assert response.status_code == 200
    assert response.json() == {
        "symbols": ["CYDE", "IKOR", "RELL", "UIM", "RBN"]
    }


def test_market_snapshot(client):
    response = client.get("/api/v1/market/snapshot")

    assert response.status_code == 200

    body = response.json()

    assert "as_of" in body
    assert len(body["market"]) == 5

    cyde = body["market"][0]

    assert cyde["symbol"] == "CYDE"
    assert Decimal(str(cyde["price"])) > 0
    assert cyde["volume"] >= 0

    assert [item["symbol"] for item in body["market"]] == [
        "CYDE",
        "IKOR",
        "RELL",
        "UIM",
        "RBN",
    ]


def test_market_initial_state():
    market = MarketEngine(seed=42)

    snapshot = market.snapshot()

    cyde = snapshot[0]

    assert cyde.symbol == "CYDE"
    assert cyde.price == Decimal("184.25")
    assert cyde.volume == 0


def test_application_workers_stop_on_shutdown(tmp_path):
    app = create_app(
        database_path=tmp_path / "market.sqlite3",
        batch_dir=tmp_path / "batches",
    )

    with TestClient(app):
        assert app.state.producer_thread.is_alive()
        assert app.state.batch_thread.is_alive()

    assert not app.state.producer_thread.is_alive()
    assert not app.state.batch_thread.is_alive()
    

def test_market_moves_without_manual_tick(client):
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

