import sqlite3
from pathlib import Path

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
        with self.connect() as db:
            row = db.execute(
                "SELECT COALESCE(MAX(sequence), 0) FROM trades"
            ).fetchone()

        return row[0]