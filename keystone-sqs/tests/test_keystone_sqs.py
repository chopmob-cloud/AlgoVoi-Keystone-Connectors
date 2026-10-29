"""Tests for algovoi-keystone-sqs (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_sqs import (
    EXECUTION_OUTCOMES, KeystoneSQSError, keystone_sqs_client, sqs_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000
QURL = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"


class FakeSQS:
    def __init__(self):
        self.sent = []

    def send_message(self, **k):
        if k.get("MessageBody") == "boom":
            raise RuntimeError("access denied")
        self.sent.append(k)
        return {"MessageId": "m1"}

    def send_message_batch(self, **k):
        self.sent.append(k)
        return {"Successful": []}

    def receive_message(self, **k):
        return {"Messages": []}   # read path, must pass through unbound


def test_sqs_execution_ref_matches_primitive():
    ref = sqs_execution_ref(decision_ref=DR, queue="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "sqs:send", "orders", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = sqs_execution_ref(decision_ref=DR, queue="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert base != sqs_execution_ref(decision_ref=DR, queue="refunds", outcome="COMMITTED", executed_at_ms=TS)
    assert base != sqs_execution_ref(decision_ref=DR, queue="orders", outcome="FAILED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystoneSQSError):
        sqs_execution_ref(decision_ref=DR, queue="q", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneSQSError):
        sqs_execution_ref(decision_ref=DR, queue="", outcome="COMMITTED", executed_at_ms=TS)


def test_send_message_binds_queue_from_url():
    c = keystone_sqs_client(FakeSQS(), decision_ref=DR, clock_ms=lambda: TS)
    c.send_message(QueueUrl=QURL, MessageBody="{}")
    assert c.execution_ref == execution_ref(DR, "sqs:send", "orders", "COMMITTED", TS)
    assert c.executions[0]["outcome"] == "COMMITTED"


def test_send_batch_binds():
    c = keystone_sqs_client(FakeSQS(), decision_ref=DR, clock_ms=lambda: TS)
    c.send_message_batch(QueueUrl=QURL, Entries=[])
    assert c.execution_ref == execution_ref(DR, "sqs:send", "orders", "COMMITTED", TS)


def test_failed_send_records_failed_and_reraises():
    c = keystone_sqs_client(FakeSQS(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        c.send_message(QueueUrl=QURL, MessageBody="boom")
    assert c.executions[-1]["outcome"] == "FAILED"
    assert c.execution_ref == execution_ref(DR, "sqs:send", "orders", "FAILED", TS)


def test_receive_passthrough_unbound():
    c = keystone_sqs_client(FakeSQS(), decision_ref=DR, clock_ms=lambda: TS)
    assert c.receive_message(QueueUrl=QURL) == {"Messages": []}
    assert c.executions == []


def test_rejects_bad_decision():
    with pytest.raises(KeystoneSQSError):
        keystone_sqs_client(FakeSQS(), decision_ref="")


def test_on_execution_callback():
    seen = []
    c = keystone_sqs_client(FakeSQS(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    c.send_message(QueueUrl=QURL, MessageBody="{}")
    assert seen[0]["execution_ref"] == c.execution_ref


def test_tamper_detected():
    ref = sqs_execution_ref(decision_ref=DR, queue="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, queue="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, queue="refunds", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, queue="orders", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
