from datetime import datetime, timezone, date
from decimal import Decimal

from market_api.database import Database
from market_api.market import TradeEvent, MarketEngine


def test_save_trade(tmp_path):
    database = Database(tmp_path / "market.sqlite3")
    database.initialize()

    event = TradeEvent(
        schema_version="market.trade/v1",
        event_id="test-event-1",
        sequence=1,
        event_time=datetime.now(timezone.utc).isoformat(),
        symbol="CYDE",
        price=Decimal("184.25"),
        volume=700,
    )

    database.save_trade(event)

    with database.connect() as db:
        row = db.execute(
            """
            SELECT
                event_id,
                sequence,
                symbol,
                price,
                volume
            FROM trades
            """
        ).fetchone()

    assert row == (
        "test-event-1",
        1,
        "CYDE",
        "184.25",
        700,
    )


def test_market_persists_trade_before_publish(tmp_path):
    database = Database(tmp_path / "market.sqlite3")
    database.initialize()

    market = MarketEngine(
        seed=42,
        database=database,
    )

    subscriber = market.subscribe()

    generated = market.tick()
    received = subscriber.get_nowait()

    with database.connect() as db:
        row = db.execute(
            """
            SELECT event_id
            FROM trades
            WHERE event_id = ?
            """,
            (received.event_id,),
        ).fetchone()

    assert received == generated
    assert row == (received.event_id,)


def test_sequence_recovers_from_database(tmp_path):
    database = Database(tmp_path / "market.sqlite3")
    database.initialize()

    first_market = MarketEngine(
        seed=42,
        database=database,
        initial_sequence=database.latest_sequence(),
    )

    first = first_market.tick()
    second = first_market.tick()

    assert first.sequence == 1
    assert second.sequence == 2

    # Simulate an application restart.
    assert database.latest_sequence() == 2

    restarted_market = MarketEngine(
        seed=42,
        database=database,
        initial_sequence=database.latest_sequence(),
    )

    third = restarted_market.tick()
    assert third.sequence == 3


def test_trades_for_date_uses_new_york_market_date(tmp_path):
    database = Database(tmp_path / "market.sqlite3")
    database.initialize()

    events = [
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="before-day",
            sequence=1,
            event_time="2026-09-30T03:59:59+00:00",
            symbol="CYDE",
            price=Decimal("184.00"),
            volume=100,
        ),
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="start-day",
            sequence=2,
            event_time="2026-09-30T04:00:00+00:00",
            symbol="CYDE",
            price=Decimal("184.10"),
            volume=200,
        ),
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="during-day",
            sequence=3,
            event_time="2026-09-30T18:00:00+00:00",
            symbol="IKOR",
            price=Decimal("328.20"),
            volume=300,
        ),
        TradeEvent(
            schema_version="market.trade/v1",
            event_id="next-day",
            sequence=4,
            event_time="2026-10-01T04:00:00+00:00",
            symbol="RELL",
            price=Decimal("92.50"),
            volume=500,
        ),
    ]

    for event in events:
        database.save_trade(event)

    rows = database.trades_for_date(
        date(2026, 9, 30)
    )

    assert [row["event_id"] for row in rows] == [
        "start-day",
        "during-day",
    ]

    assert [row["sequence"] for row in rows] == [
        2,
        3,
    ]