"""Timestamp precision is a lifecycle boundary, including on Python 3.10."""

from datetime import datetime, timezone

import pytest

from streamt.deployer.kafka_streams_time import parse_utc_timestamp


@pytest.mark.parametrize("fraction", ["", *["123456789"[:length] for length in range(1, 10)]])
def test_supported_fractional_lengths_are_exact(fraction: str) -> None:
    stamp = "2026-09-05T10:00:01" + ("." + fraction if fraction else "") + "Z"
    assert parse_utc_timestamp(stamp) == (
        datetime(2026, 9, 5, 10, 0, 1, tzinfo=timezone.utc),
        int(fraction.ljust(9, "0")),
    )


def test_nanosecond_freshness_is_not_rounded_to_microseconds() -> None:
    assert parse_utc_timestamp("2026-09-05T10:00:01.123456788Z") < parse_utc_timestamp(
        "2026-09-05T10:00:01.123456789Z"
    )
    assert parse_utc_timestamp("2026-09-05T10:00:01.999999999Z") < parse_utc_timestamp(
        "2026-09-05T10:00:02Z"
    )


def test_equivalent_precision_has_identical_ordering() -> None:
    assert parse_utc_timestamp("2026-09-05T10:00:01.1Z") == parse_utc_timestamp(
        "2026-09-05T10:00:01.100000000Z"
    )


@pytest.mark.parametrize("stamp", [
    None, True, 123, "", "secret-do-not-echo", "2026-09-05T10:00:01",
    "2026-09-05T10:00:01+00:00", "2026-09-05T10:00:01.1234567890Z",
    "2026-09-05T10:00:01.Z", "2026-09-05T10:00:01,123Z", "2026-09-05 10:00:01Z",
    "2026-09-05T10:00:01z", "2026-02-29T10:00:01Z", "2026-13-05T10:00:01Z",
    "2026-09-31T10:00:01Z", "2026-09-05T24:00:01Z", "2026-09-05T10:60:01Z",
    "2026-09-05T10:00:60Z", "0000-01-01T00:00:00Z", "2026-09-05T10:00:01Z\n",
])
def test_invalid_timestamps_fail_without_echoing_values(stamp: object) -> None:
    with pytest.raises(ValueError) as caught:
        parse_utc_timestamp(stamp)
    assert "secret-do-not-echo" not in str(caught.value)
