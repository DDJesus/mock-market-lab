import sqlite3

from pathlib import Path
from datetime import date, datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo

from market_api.market import TradeEvent


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        with self.connect() as db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    event_id TEXT PRIMARY KEY,
                    sequence INTEGER NOT NULL UNIQUE,
                    event_time TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price TEXT NOT NULL,
                    volume INTEGER NOT NULL
                )
            """)

    def connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def save_trade(self, event: TradeEvent) -> None:
        with self.connect() as db:
            db.execute(
                """
                INSERT INTO trades (
                    event_id,
                    sequence,
                    event_time,
                    schema_version,
                    symbol,
                    price,
                    volume
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.sequence,
                    event.event_time,
                    event.schema_version,
                    event.symbol,
                    str(event.price),
                    event.volume,
                ),
            )

    def latest_sequence(self) -> int:
        """
        Get the latest sequence number from the trades table.
        Prevents duplicate sequence numbers from being inserted into the database.
        """
        with self.connect() as db:
            row = db.execute(
                "SELECT COALESCE(MAX(sequence), 0) FROM trades"
            ).fetchone()

        return row[0]

    def trades_for_date(self, market_date: date) -> list[sqlite3.Row]:
        eastern = ZoneInfo("America/New_York")

        start_local = datetime.combine(
            market_date,
            time.min,
            tzinfo=eastern,
        )

        next_day_local = start_local + timedelta(days=1)

        start_utc = start_local.astimezone(timezone.utc).isoformat()
        next_day_utc = next_day_local.astimezone(timezone.utc).isoformat()

        with self.connect() as db:
            db.row_factory = sqlite3.Row

            return db.execute(
                """
                SELECT
                    event_id,
                    sequence,
                    event_time,
                    schema_version,
                    symbol,
                    price,
                    volume
                FROM trades
                WHERE event_time >= ?
                AND event_time < ?
                ORDER BY sequence ASC
                """,
                (start_utc, next_day_utc),
            ).fetchall()