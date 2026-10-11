"""Checks for src/checks.py: the retry loop behind the processor's 60 s wait for
Kafka and MySQL, and the one-sentence exit when a service stays unreachable."""
import pytest

from src import checks


class FlakyService:
    """Fails with ConnectionError a set number of times, then answers "ok"."""

    def __init__(self, failures):
        self.failures = failures
        self.calls = 0

    def __call__(self):
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("connection refused")
        return "ok"


def test_retry_returns_once_the_service_answers():
    service = FlakyService(failures=2)

    result = checks.retry("Kafka", service, ConnectionError, "Start it.", wait_s=5, every_s=0.01)

    assert result == "ok"
    assert service.calls == 3


def test_retry_gives_up_with_one_sentence_naming_the_service_and_the_fix():
    service = FlakyService(failures=1000)

    with pytest.raises(SystemExit) as stop:
        checks.retry("MySQL at 127.0.0.1:3307", service, ConnectionError, "Start it with: docker compose up -d",
                     wait_s=0.05, every_s=0.01)

    message = str(stop.value)
    assert "MySQL at 127.0.0.1:3307" in message
    assert "docker compose up -d" in message
    assert "connection refused" in message   # the reason, so a wrong password is not reported as "down"
    assert "\n" not in message
    assert service.calls > 1


def test_without_a_wait_it_tries_exactly_once():
    service = FlakyService(failures=1000)

    with pytest.raises(SystemExit):
        checks.retry("Kafka", service, ConnectionError, "Start it.", wait_s=0, every_s=0.01)

    assert service.calls == 1


def test_unexpected_errors_are_not_hidden():
    def broken():
        raise ValueError("a bug, not an outage")

    with pytest.raises(ValueError):
        checks.retry("Kafka", broken, ConnectionError, "Start it.", wait_s=1, every_s=0.01)
