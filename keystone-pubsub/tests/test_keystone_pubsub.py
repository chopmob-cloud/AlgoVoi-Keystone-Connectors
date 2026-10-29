"""Tests for algovoi-keystone-pubsub (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_pubsub import (
    EXECUTION_OUTCOMES, KeystonePubSubError, keystone_publisher, pubsub_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000
TOPIC = "projects/my-proj/topics/order-events"


class FakeFuture:
    def result(self):
        return "msg-id-1"


class FakePublisher:
    def __init__(self):
        self.published = []

    def publish(self, topic, data=b"", **attrs):
        if data == b"boom":
            raise RuntimeError("permission denied")
        self.published.append((topic, data))
        return FakeFuture()

    def topic_path(self, project, topic):
        return "projects/%s/topics/%s" % (project, topic)   # helper, passes through unbound


def test_pubsub_execution_ref_matches_primitive():
    ref = pubsub_execution_ref(decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "pubsub:publish", "order-events", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = pubsub_execution_ref(decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert base != pubsub_execution_ref(decision_ref=DR, topic="refund-events", outcome="COMMITTED", executed_at_ms=TS)
    assert base != pubsub_execution_ref(decision_ref=DR, topic="order-events", outcome="FAILED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystonePubSubError):
        pubsub_execution_ref(decision_ref=DR, topic="t", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystonePubSubError):
        pubsub_execution_ref(decision_ref=DR, topic="", outcome="COMMITTED", executed_at_ms=TS)


def test_publish_binds_topic_from_path():
    c = keystone_publisher(FakePublisher(), decision_ref=DR, clock_ms=lambda: TS)
    c.publish(TOPIC, b"{}")
    assert c.execution_ref == execution_ref(DR, "pubsub:publish", "order-events", "COMMITTED", TS)
    assert c.executions[0]["outcome"] == "COMMITTED"


def test_failed_publish_records_failed_and_reraises():
    c = keystone_publisher(FakePublisher(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        c.publish(TOPIC, b"boom")
    assert c.executions[-1]["outcome"] == "FAILED"
    assert c.execution_ref == execution_ref(DR, "pubsub:publish", "order-events", "FAILED", TS)


def test_helper_passthrough_unbound():
    c = keystone_publisher(FakePublisher(), decision_ref=DR, clock_ms=lambda: TS)
    assert c.topic_path("my-proj", "order-events") == TOPIC
    assert c.executions == []


def test_rejects_bad_decision():
    with pytest.raises(KeystonePubSubError):
        keystone_publisher(FakePublisher(), decision_ref="")


def test_on_execution_callback():
    seen = []
    c = keystone_publisher(FakePublisher(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    c.publish(TOPIC, b"{}")
    assert seen[0]["execution_ref"] == c.execution_ref


def test_tamper_detected():
    ref = pubsub_execution_ref(decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, topic="refund-events", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
