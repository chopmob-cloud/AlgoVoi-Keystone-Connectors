"""Tests for algovoi-keystone-azure-servicebus (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_azure_servicebus import (
    EXECUTION_OUTCOMES, KeystoneServiceBusError, keystone_sender,
    servicebus_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000


class FakeSender:
    """azure-servicebus ServiceBusSender double; raises if the batch is the string 'boom'."""
    def __init__(self, entity_name="orders"):
        self.entity_name = entity_name
        self.sent = []

    def send_messages(self, message, **k):
        if message == "boom":
            raise RuntimeError("unauthorized")
        self.sent.append(message)
        return None

    def schedule_messages(self, messages, schedule_time_utc, **k):
        self.sent.append(("scheduled", messages))
        return [1]

    def receive_messages(self, **k):
        return []   # receive path, must pass through unbound


def test_servicebus_execution_ref_matches_primitive():
    ref = servicebus_execution_ref(decision_ref=DR, entity="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "servicebus:send", "orders", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = servicebus_execution_ref(decision_ref=DR, entity="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert base != servicebus_execution_ref(decision_ref=DR, entity="refunds", outcome="COMMITTED", executed_at_ms=TS)
    assert base != servicebus_execution_ref(decision_ref=DR, entity="orders", outcome="FAILED", executed_at_ms=TS)
    assert base != servicebus_execution_ref(decision_ref="sha256:" + "b" * 64, entity="orders", outcome="COMMITTED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystoneServiceBusError):
        servicebus_execution_ref(decision_ref=DR, entity="orders", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneServiceBusError):
        servicebus_execution_ref(decision_ref=DR, entity="", outcome="COMMITTED", executed_at_ms=TS)


def test_send_messages_binds():
    s = keystone_sender(FakeSender(), decision_ref=DR, clock_ms=lambda: TS)
    s.send_messages("m1")
    assert s.execution_ref == execution_ref(DR, "servicebus:send", "orders", "COMMITTED", TS)
    assert s.executions[0]["outcome"] == "COMMITTED"


def test_schedule_messages_binds():
    s = keystone_sender(FakeSender(), decision_ref=DR, clock_ms=lambda: TS)
    s.schedule_messages("m1", 0)
    assert s.execution_ref == execution_ref(DR, "servicebus:send", "orders", "COMMITTED", TS)


def test_entity_override():
    s = keystone_sender(FakeSender(entity_name=""), decision_ref=DR, entity="topic-a", clock_ms=lambda: TS)
    s.send_messages("m1")
    assert s.execution_ref == execution_ref(DR, "servicebus:send", "topic-a", "COMMITTED", TS)


def test_failed_send_records_failed_and_reraises():
    s = keystone_sender(FakeSender(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        s.send_messages("boom")
    assert s.executions[-1]["outcome"] == "FAILED"
    assert s.execution_ref == execution_ref(DR, "servicebus:send", "orders", "FAILED", TS)


def test_receive_passthrough_unbound():
    fake = FakeSender()
    s = keystone_sender(fake, decision_ref=DR, clock_ms=lambda: TS)
    assert s.receive_messages() == []
    assert s.executions == []   # receives are not bound


def test_rejects_bad_decision():
    with pytest.raises(KeystoneServiceBusError):
        keystone_sender(FakeSender(), decision_ref="")


def test_on_execution_callback():
    seen = []
    s = keystone_sender(FakeSender(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    s.send_messages("m1")
    assert seen[0]["execution_ref"] == s.execution_ref


def test_tamper_detected():
    ref = servicebus_execution_ref(decision_ref=DR, entity="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, entity="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, entity="refunds", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, entity="orders", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
