from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import threading

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


class BatchPublisher:
    def __init__(
        self,
        scheduler: BatchScheduler,
        stop_event: threading.Event,
        clock=None,
    ):
        self.scheduler = scheduler
        self.stop_event = stop_event
        self.clock = clock or (
            lambda: datetime.now(tz=EASTERN)
        )

    def run(self):
        while not self.stop_event.is_set():
            now = self.clock()
            next_run = self.scheduler.next_run_after(now)

            wait_seconds = max(
                0.0,
                (next_run - now).total_seconds(),
            )

            if self.stop_event.wait(wait_seconds):
                break

            self.scheduler.publish_for_run(next_run)