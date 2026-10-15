"""Checks for scripts/reset_demo.py: it refuses while the pipeline runs, and deletes nothing then.

FakeAdmin and FakeConn stand in for Kafka and MySQL and record every call, so the
tests can prove the order of the steps and that a refusal changes nothing.
"""
from datetime import datetime, timedelta, timezone

import pytest

from scripts import reset_demo


class FakeAdmin:
    def __init__(self, members=None, topics=("machine-temperature",)):
        self.members = members or {}           # group -> number of running consumers
        self.topics = set(topics)
        self.calls = []

    def describe_groups(self, groups):
        return {g: {"members": [object()] * self.members.get(g, 0)} for g in groups}

    def list_groups(self):
        return [{"group_id": g} for g in ("scoring", "forecast")]

    def list_topics(self):
        return list(self.topics)

    def delete_topics(self, topics):
        self.calls.append(("delete_topics", topics))
        self.topics -= set(topics)

    def create_topics(self, spec, wait_for_metadata):
        self.calls.append(("create_topics", list(spec)))
        self.topics |= set(spec)

    def delete_groups(self, groups):
        self.calls.append(("delete_groups", groups))
        return {g: "OK" for g in groups}


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture
def system(monkeypatch, tmp_path):
    """An idle pipeline: no consumers, data last stored 1 minute ago, topic not growing."""
    calls = []
    state = tmp_path / ".producer_state"
    state.write_text("4999\n")
    monkeypatch.setattr(reset_demo.config, "PRODUCER_STATE", state)
    monkeypatch.setattr(reset_demo.db, "fetch_metrics", lambda conn: {"updated_at": utc_now() - timedelta(minutes=1)})
    monkeypatch.setattr(reset_demo.db, "empty_tables", lambda conn: calls.append(("empty_tables",)))
    monkeypatch.setattr(reset_demo, "topic_growing", lambda: False)
    return {"calls": calls, "state": state, "conn": object()}


def test_an_idle_pipeline_is_reset_in_order(system):
    admin = FakeAdmin()

    lines = reset_demo.reset_all(admin, system["conn"])

    assert [c[0] for c in admin.calls] == ["delete_topics", "create_topics", "delete_groups", "delete_groups"]
    assert system["calls"] == [("empty_tables",)]
    assert not system["state"].exists()
    assert lines[-1] == "Reset complete."


def test_a_running_processor_is_refused_and_nothing_is_deleted(system):
    admin = FakeAdmin(members={"scoring": 1})

    with pytest.raises(reset_demo.ResetError) as refused:
        reset_demo.reset_all(admin, system["conn"])

    assert "scoring" in str(refused.value) and "Ctrl+C" in str(refused.value)
    assert admin.calls == [] and system["calls"] == []
    assert system["state"].exists()


def test_a_running_forecaster_is_refused(system):
    with pytest.raises(reset_demo.ResetError) as refused:
        reset_demo.reset_all(FakeAdmin(members={"forecast": 2}), system["conn"])
    assert "forecast" in str(refused.value)


def test_data_stored_seconds_ago_is_refused(system, monkeypatch):
    monkeypatch.setattr(reset_demo.db, "fetch_metrics", lambda conn: {"updated_at": utc_now() - timedelta(seconds=3)})
    admin = FakeAdmin()

    with pytest.raises(reset_demo.ResetError):
        reset_demo.reset_all(admin, system["conn"])

    assert admin.calls == [] and system["calls"] == []


def test_a_running_producer_is_refused(system, monkeypatch):
    # The processor is stopped, but the producer is still adding messages to the topic.
    monkeypatch.setattr(reset_demo, "topic_growing", lambda: True)
    admin = FakeAdmin()

    with pytest.raises(reset_demo.ResetError) as refused:
        reset_demo.reset_all(admin, system["conn"])

    assert "producer" in str(refused.value)
    assert admin.calls == [] and system["calls"] == []


def test_empty_tables_and_no_state_file_are_fine(system, monkeypatch):
    # Running the reset twice in a row: nothing stored, no .producer_state.
    monkeypatch.setattr(reset_demo.db, "fetch_metrics", lambda conn: None)
    system["state"].unlink()

    lines = reset_demo.reset_all(FakeAdmin(), system["conn"])

    assert ".producer_state not present (nothing to remove)" in lines
