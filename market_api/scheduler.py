from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from market_api.batch import BatchWriter


EASTERN = ZoneInfo("America/New_York")
PUBLISH_TIME = time(hour=6)


class BatchScheduler:
    def __init__(self, writer: BatchWriter):
        self.writer = writer

    def market_date_for_run(
        self,
        run_time: datetime,
    ) -> date:
        local_time = run_time.astimezone(EASTERN)
        return local_time.date() - timedelta(days=1)

    def publish_for_run(
        self,
        run_time: datetime,
    ):
        market_date = self.market_date_for_run(run_time)
        return self.writer.write_daily_batch(market_date)

    def next_run_after(
        self,
        current_time: datetime,
    ) -> datetime:
        local_time = current_time.astimezone(EASTERN)

        candidate = datetime.combine(
            local_time.date(),
            PUBLISH_TIME,
            tzinfo=EASTERN,
        )

        if local_time >= candidate:
            candidate += timedelta(days=1)

        return candidate