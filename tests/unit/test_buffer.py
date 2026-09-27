import json
import threading

import pytest

from preburn._buffer import PendingReport, ReportBuffer
from preburn._errors import ConfigurationError

UNLIMITED_BYTES = 10_000_000


def make_report(index: int, padding: str = "") -> PendingReport:
    body = {"decision_source": "server", "decision_id": f"dec_{index}", "usage": {}}
    if padding:
        body["attributes"] = {"padding": padding}
    return json.dumps(body).encode()


def collect_decision_ids(reports: list[PendingReport]) -> list[object]:
    return [json.loads(report)["decision_id"] for report in reports]


def test_take_batch_returns_oldest_first_up_to_maximum() -> None:
    buffer = ReportBuffer(max_pending=10)
    for index in range(5):
        buffer.add(make_report(index))
    batch = buffer.take_batch(3, UNLIMITED_BYTES)
    if collect_decision_ids(batch) != ["dec_0", "dec_1", "dec_2"]:
        pytest.fail(f"batch={collect_decision_ids(batch)}")
    if len(buffer) != 2:
        pytest.fail(f"pending={len(buffer)}")
    rest = buffer.take_batch(10, UNLIMITED_BYTES)
    if collect_decision_ids(rest) != ["dec_3", "dec_4"]:
        pytest.fail(f"rest={collect_decision_ids(rest)}")
    if buffer.take_batch(10, UNLIMITED_BYTES) != []:
        pytest.fail("empty buffer returned reports")


def test_overflow_drops_oldest_and_counts() -> None:
    buffer = ReportBuffer(max_pending=3)
    for index in range(5):
        buffer.add(make_report(index))
    if buffer.dropped != 2:
        pytest.fail(f"dropped={buffer.dropped}")
    batch = buffer.take_batch(10, UNLIMITED_BYTES)
    if collect_decision_ids(batch) != ["dec_2", "dec_3", "dec_4"]:
        pytest.fail(f"batch={collect_decision_ids(batch)}")


def test_requeue_front_keeps_order() -> None:
    buffer = ReportBuffer(max_pending=10)
    for index in range(5):
        buffer.add(make_report(index))
    batch = buffer.take_batch(3, UNLIMITED_BYTES)
    buffer.add(make_report(5))
    buffer.requeue_front(batch)
    reports = buffer.take_batch(10, UNLIMITED_BYTES)
    if collect_decision_ids(reports) != ["dec_0", "dec_1", "dec_2", "dec_3", "dec_4", "dec_5"]:
        pytest.fail(f"reports={collect_decision_ids(reports)}")
    if buffer.dropped != 0:
        pytest.fail(f"dropped={buffer.dropped}")


def test_requeue_front_over_capacity_drops_oldest_of_batch() -> None:
    buffer = ReportBuffer(max_pending=4)
    for index in range(3):
        buffer.add(make_report(index))
    batch = buffer.take_batch(3, UNLIMITED_BYTES)
    for index in range(3, 6):
        buffer.add(make_report(index))
    buffer.requeue_front(batch)
    if buffer.dropped != 2:
        pytest.fail(f"dropped={buffer.dropped}")
    reports = buffer.take_batch(10, UNLIMITED_BYTES)
    if collect_decision_ids(reports) != ["dec_2", "dec_3", "dec_4", "dec_5"]:
        pytest.fail(f"reports={collect_decision_ids(reports)}")


def test_acknowledged_drops_leave_later_drops_counted() -> None:
    buffer = ReportBuffer(max_pending=1)
    buffer.add(make_report(0))
    buffer.add(make_report(1))
    buffer.add(make_report(2))
    sent_count = buffer.dropped
    buffer.add(make_report(3))
    buffer.acknowledge_dropped(sent_count)
    if buffer.dropped != 1:
        pytest.fail(f"dropped={buffer.dropped}")


def test_acknowledging_more_than_dropped_fails_loud() -> None:
    buffer = ReportBuffer(max_pending=1)
    with pytest.raises(ValueError, match="dropped"):
        buffer.acknowledge_dropped(1)


def test_reports_dropped_outside_the_buffer_are_counted_until_acknowledged() -> None:
    buffer = ReportBuffer(max_pending=1)
    buffer.add(make_report(0))
    buffer.add(make_report(1))
    buffer.count_dropped(3)
    if buffer.dropped != 4 or len(buffer) != 1:
        pytest.fail(f"dropped={buffer.dropped} pending={len(buffer)}")
    buffer.acknowledge_dropped(4)
    if buffer.dropped != 0:
        pytest.fail(f"dropped={buffer.dropped}")


def test_take_batch_stops_before_the_byte_maximum() -> None:
    buffer = ReportBuffer(max_pending=10)
    for index in range(4):
        buffer.add(make_report(index, padding="x" * 100))
    report_size = len(make_report(0, padding="x" * 100))
    batch = buffer.take_batch(10, report_size * 2 + report_size // 2)
    if collect_decision_ids(batch) != ["dec_0", "dec_1"]:
        pytest.fail(f"batch={collect_decision_ids(batch)}")
    rest = buffer.take_batch(10, report_size * 2)
    if collect_decision_ids(rest) != ["dec_2", "dec_3"]:
        pytest.fail(f"rest={collect_decision_ids(rest)}")


def test_take_batch_always_takes_the_oldest_report() -> None:
    buffer = ReportBuffer(max_pending=10)
    buffer.add(make_report(0, padding="x" * 100))
    buffer.add(make_report(1))
    batch = buffer.take_batch(10, 10)
    if collect_decision_ids(batch) != ["dec_0"]:
        pytest.fail(f"batch={collect_decision_ids(batch)}")


def test_reset_after_fork_empties_the_buffer_and_its_dropped_count() -> None:
    buffer = ReportBuffer(max_pending=1)
    buffer.add(make_report(0))
    buffer.add(make_report(1))
    buffer.reset_after_fork()
    if len(buffer) != 0 or buffer.dropped != 0:
        pytest.fail(f"pending={len(buffer)} dropped={buffer.dropped}")
    buffer.add(make_report(2))
    if collect_decision_ids(buffer.take_batch(10, UNLIMITED_BYTES)) != ["dec_2"]:
        pytest.fail("buffer unusable after reset")


def test_take_batch_rejects_maximum_below_one() -> None:
    buffer = ReportBuffer(max_pending=1)
    with pytest.raises(ValueError, match="maximum"):
        buffer.take_batch(0, UNLIMITED_BYTES)


def test_max_pending_below_one_is_a_configuration_error() -> None:
    with pytest.raises(ConfigurationError, match="max_pending"):
        ReportBuffer(max_pending=0)


def test_concurrent_adds_lose_nothing() -> None:
    buffer = ReportBuffer(max_pending=100_000)
    thread_count = 8
    reports_per_thread = 2_000

    def add_reports(offset: int) -> None:
        for index in range(reports_per_thread):
            buffer.add(make_report(offset * reports_per_thread + index))

    threads = [
        threading.Thread(target=add_reports, args=(offset,)) for offset in range(thread_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if len(buffer) != thread_count * reports_per_thread:
        pytest.fail(f"pending={len(buffer)}")
    reports = buffer.take_batch(thread_count * reports_per_thread, UNLIMITED_BYTES)
    if len(set(collect_decision_ids(reports))) != thread_count * reports_per_thread:
        pytest.fail("duplicate or missing reports")
