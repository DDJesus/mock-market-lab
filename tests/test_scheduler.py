from datetime import datetime
from unittest.mock import Mock
from zoneinfo import ZoneInfo
import threading

from market_api.scheduler import BatchScheduler, BatchPublisher


EASTERN = ZoneInfo("America/New_York")


def test_scheduler_publishes_previous_market_date():
    writer = Mock()
    scheduler = BatchScheduler(writer)

    run_time = datetime(
        2026,
        10,
        1,
        6,
        0,
        tzinfo=EASTERN,
    )

    scheduler.publish_for_run(run_time)

    writer.write_daily_batch.assert_called_once_with(
        datetime(2026, 9, 30).date()
    )


def test_next_run_before_publish_time():
    writer = Mock()
    scheduler = BatchScheduler(writer)

    current_time = datetime(
        2026,
        10,
        1,
        5,
        30,
        tzinfo=EASTERN,
    )

    next_run = scheduler.next_run_after(current_time)

    assert next_run == datetime(
        2026,
        10,
        1,
        6,
        0,
        tzinfo=EASTERN,
    )


def test_next_run_after_publish_time():
    writer = Mock()
    scheduler = BatchScheduler(writer)

    current_time = datetime(
        2026,
        10,
        1,
        8,
        30,
        tzinfo=EASTERN,
    )

    next_run = scheduler.next_run_after(current_time)

    assert next_run == datetime(
        2026,
        10,
        2,
        6,
        0,
        tzinfo=EASTERN,
    )


def test_next_run_respects_daylight_saving_time():
    writer = Mock()
    scheduler = BatchScheduler(writer)

    winter = datetime(
        2026,
        1,
        15,
        7,
        0,
        tzinfo=EASTERN,
    )

    summer = datetime(
        2026,
        7,
        15,
        7,
        0,
        tzinfo=EASTERN,
    )

    winter_run = scheduler.next_run_after(winter)
    summer_run = scheduler.next_run_after(summer)

    assert winter_run.hour == 6
    assert summer_run.hour == 6

    assert winter_run.utcoffset() != summer_run.utcoffset()


def test_batch_publisher_stops_cleanly():
    writer = Mock()
    scheduler = BatchScheduler(writer)
    stop_event = threading.Event()

    publisher = BatchPublisher(
        scheduler=scheduler,
        stop_event=stop_event,
        clock=lambda: datetime(
            2026,
            10,
            1,
            8,
            0,
            tzinfo=EASTERN,
        ),
    )

    thread = threading.Thread(target=publisher.run)
    thread.start()

    stop_event.set()
    thread.join(timeout=1)

    assert not thread.is_alive()
    writer.write_daily_batch.assert_not_called()