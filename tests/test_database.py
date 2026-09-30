from datetime import datetime, timezone
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