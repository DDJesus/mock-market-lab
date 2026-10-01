import csv

import pytest
from fastapi.testclient import TestClient

from market_api.main import create_app


@pytest.fixture
def batch_client(tmp_path):
    batch_dir = tmp_path / "batches"

    app = create_app(
        database_path=tmp_path / "market.sqlite3",
        batch_dir=batch_dir,
    )

    with TestClient(app) as client:
        yield client, batch_dir


def test_missing_batch_returns_404(batch_client):
    client, batch_dir = batch_client

    response = client.get(
        "/api/v1/batches/2026-09-30"
    )

    assert response.status_code == 404
    assert not batch_dir.exists()


def test_existing_batch_can_be_downloaded(batch_client):
    client, batch_dir = batch_client

    batch_dir.mkdir(parents=True)

    batch_path = (
        batch_dir / "market-trades-2026-09-30.csv"
    )

    expected = (
        "event_id,sequence,event_time,schema_version,"
        "symbol,price,volume\n"
        "event-1,1,2026-09-30T14:00:00+00:00,"
        "market.trade/v1,CYDE,184.25,700\n"
    )

    batch_path.write_text(
        expected,
        encoding="utf-8",
    )

    response = client.get(
        "/api/v1/batches/2026-09-30"
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith(
        "text/csv"
    )
    assert response.text.splitlines() == expected.splitlines()


def test_batches_can_be_listed(batch_client):
    client, batch_dir = batch_client

    batch_dir.mkdir(parents=True)

    (
        batch_dir / "market-trades-2026-09-30.csv"
    ).write_text("test", encoding="utf-8")

    (
        batch_dir / "market-trades-2026-09-29.csv"
    ).write_text("test", encoding="utf-8")

    response = client.get("/api/v1/batches")

    assert response.status_code == 200
    assert response.json() == {
        "batches": [
            "market-trades-2026-09-29.csv",
            "market-trades-2026-09-30.csv",
        ]
    }