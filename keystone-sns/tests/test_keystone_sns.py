"""Tests for algovoi-keystone-sns (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_sns import (
    EXECUTION_OUTCOMES, KeystoneSNSError, keystone_sns_client, sns_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000
ARN = "arn:aws:sns:us-east-1:123456789012:order-events"


class FakeSNS:
    def __init__(self):
        self.published = []

    def publish(self, **k):
        if k.get("Message") == "boom":
            raise RuntimeError("not authorized")
        self.published.append(k)
        return {"MessageId": "m1"}

    def list_topics(self, **k):
        return {"Topics": []}   # read path, must pass through unbound


def test_sns_execution_ref_matches_primitive():
    ref = sns_execution_ref(decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "sns:publish", "order-events", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = sns_execution_ref(decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert base != sns_execution_ref(decision_ref=DR, topic="refund-events", outcome="COMMITTED", executed_at_ms=TS)
    assert base != sns_execution_ref(decision_ref=DR, topic="order-events", outcome="FAILED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystoneSNSError):
        sns_execution_ref(decision_ref=DR, topic="t", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneSNSError):
        sns_execution_ref(decision_ref=DR, topic="", outcome="COMMITTED", executed_at_ms=TS)


def test_publish_binds_topic_from_arn():
    c = keystone_sns_client(FakeSNS(), decision_ref=DR, clock_ms=lambda: TS)
    c.publish(TopicArn=ARN, Message="{}")
    assert c.execution_ref == execution_ref(DR, "sns:publish", "order-events", "COMMITTED", TS)
    assert c.executions[0]["outcome"] == "COMMITTED"


def test_failed_publish_records_failed_and_reraises():
    c = keystone_sns_client(FakeSNS(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        c.publish(TopicArn=ARN, Message="boom")
    assert c.executions[-1]["outcome"] == "FAILED"
    assert c.execution_ref == execution_ref(DR, "sns:publish", "order-events", "FAILED", TS)


def test_list_passthrough_unbound():
    c = keystone_sns_client(FakeSNS(), decision_ref=DR, clock_ms=lambda: TS)
    assert c.list_topics() == {"Topics": []}
    assert c.executions == []


def test_rejects_bad_decision():
    with pytest.raises(KeystoneSNSError):
        keystone_sns_client(FakeSNS(), decision_ref="")


def test_on_execution_callback():
    seen = []
    c = keystone_sns_client(FakeSNS(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    c.publish(TopicArn=ARN, Message="{}")
    assert seen[0]["execution_ref"] == c.execution_ref


def test_tamper_detected():
    ref = sns_execution_ref(decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, topic="refund-events", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, topic="order-events", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
