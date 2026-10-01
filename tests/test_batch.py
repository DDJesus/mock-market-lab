import csv
from datetime import date
from decimal import Decimal

from market_api.batch import BatchWriter
from market_api.database import Database
from market_api.market import TradeEvent


def test_daily_batch_contains_canonical_trades(tmp_path):
    database = Database(tmp_path / "market.sqlite3")
    database.initialize()

    events = [
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="event-1",
            sequence=1,
            event_time="2026-09-30T14:00:00+00:00",
            symbol="CYDE",
            price=Decimal("184.25"),
            volume=700,
        ),
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="event-2",
            sequence=2,
            event_time="2026-09-30T14:00:01+00:00",
            symbol="IKOR",
            price=Decimal("327.80"),
            volume=300,
        ),
    ]

    for event in events:
        database.save_trade(event)

    writer = BatchWriter(
        database=database,
        output_dir=tmp_path / "batches",
    )

    output_path = writer.write_daily_batch(
        date(2026, 9, 30)
    )

    assert output_path.exists()

    with output_path.open(
        newline="",
        encoding="utf-8",
    ) as file:
        rows = list(csv.DictReader(file))

    assert rows == [
        {
            "event_id": "event-1",
            "sequence": "1",
            "event_time": "2026-09-30T14:00:00+00:00",
            "schema_version": "market.trade/v1",
            "symbol": "CYDE",
            "price": "184.25",
            "volume": "700",
        },
        {
            "event_id": "event-2",
            "sequence": "2",
            "event_time": "2026-09-30T14:00:01+00:00",
            "schema_version": "market.trade/v1",
            "symbol": "IKOR",
            "price": "327.80",
            "volume": "300",
        },
    ]


def test_daily_batch_excludes_other_market_dates(tmp_path):
    database = Database(tmp_path / "market.sqlite3")
    database.initialize()

    events = [
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="previous-day",
            sequence=1,
            event_time="2026-09-30T03:59:59+00:00",
            symbol="CYDE",
            price=Decimal("184.00"),
            volume=100,
        ),
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="requested-day",
            sequence=2,
            event_time="2026-09-30T04:00:00+00:00",
            symbol="CYDE",
            price=Decimal("184.10"),
            volume=200,
        ),
    ]

    for event in events:
        database.save_trade(event)

    writer = BatchWriter(
        database=database,
        output_dir=tmp_path / "batches",
    )

    output_path = writer.write_daily_batch(
        date(2026, 9, 30)
    )

    with output_path.open(
        newline="",
        encoding="utf-8",
    ) as file:
        rows = list(csv.DictReader(file))

    assert [row["event_id"] for row in rows] == [
        "requested-day"
    ]