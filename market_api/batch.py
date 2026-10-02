import csv

from datetime import date
from pathlib import Path

from market_api.database import Database


class BatchWriter:
    """
    A writer for creating daily market trade batches.
    """

    # The field names for the CSV file. The order of the fields is important, as it determines the order of the columns in the CSV file.
    # Currently dumping all fields from the database, but can be changed to a subset of fields if desired.
    FIELDNAMES = [
        "event_id",
        "sequence",
        "event_time",
        "schema_version",
        "symbol",
        "price",
        "volume",
    ]

    def __init__(
        self,
        database: Database,
        output_dir: str | Path,
    ):
        """
        Initialize the BatchWriter.

        Args:
            database: The database instance. Currently using SQLite, but can be changed to a different database if desired.
            output_dir: The directory where the batch files will be written.
        """
        self.database = database
        self.output_dir = Path(output_dir)

    def write_daily_batch(self, market_date: date) -> Path:
        rows = self.database.trades_for_date(market_date)

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        # CSV output for batch
        output_path = (
            self.output_dir / f"market-trades-{market_date.isoformat()}.csv"
        )

        with output_path.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as file: 
            writer = csv.DictWriter(
                file,
                fieldnames=self.FIELDNAMES,
            )

            writer.writeheader()

            for row in rows:
                writer.writerow(
                    {
                        field: row[field]
                        for field in self.FIELDNAMES
                    }
                )

        return output_path