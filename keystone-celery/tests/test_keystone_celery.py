"""Tests for algovoi-keystone-celery (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_celery import (
    EXECUTION_OUTCOMES, KeystoneCeleryError, keystone_celery_app, keystone_task,
    task_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000


class FakeTask:
    """Celery task double; name attribute + apply_async/delay. Raises if kwargs mark 'boom'."""
    name = "billing.charge"

    def __init__(self):
        self.dispatched = []

    def apply_async(self, args=None, kwargs=None, **opts):
        if kwargs and kwargs.get("boom"):
            raise RuntimeError("broker down")
        self.dispatched.append((args, kwargs))
        return object()   # AsyncResult stand-in

    def delay(self, *args, **kwargs):
        self.dispatched.append((args, kwargs))
        return object()

    def run(self, *a, **k):
        return "ran"   # calling the task body directly is not a dispatch -> unbound


class FakeApp:
    """Celery app double; send_task(name, ...)."""
    def send_task(self, name, args=None, kwargs=None, **opts):
        return object()


def test_task_execution_ref_matches_primitive():
    ref = task_execution_ref(decision_ref=DR, task="billing.charge", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "celery:dispatch", "billing.charge", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = task_execution_ref(decision_ref=DR, task="billing.charge", outcome="COMMITTED", executed_at_ms=TS)
    assert base != task_execution_ref(decision_ref=DR, task="billing.refund", outcome="COMMITTED", executed_at_ms=TS)
    assert base != task_execution_ref(decision_ref=DR, task="billing.charge", outcome="FAILED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystoneCeleryError):
        task_execution_ref(decision_ref=DR, task="t", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneCeleryError):
        task_execution_ref(decision_ref=DR, task="", outcome="COMMITTED", executed_at_ms=TS)


def test_apply_async_binds():
    t = keystone_task(FakeTask(), decision_ref=DR, clock_ms=lambda: TS)
    t.apply_async(args=[1, 2])
    assert t.execution_ref == execution_ref(DR, "celery:dispatch", "billing.charge", "COMMITTED", TS)
    assert t.executions[0]["outcome"] == "COMMITTED"


def test_delay_binds():
    t = keystone_task(FakeTask(), decision_ref=DR, clock_ms=lambda: TS)
    t.delay(1, 2)
    assert t.execution_ref == execution_ref(DR, "celery:dispatch", "billing.charge", "COMMITTED", TS)


def test_send_task_binds():
    a = keystone_celery_app(FakeApp(), decision_ref=DR, clock_ms=lambda: TS)
    a.send_task("billing.charge", args=[1])
    assert a.execution_ref == execution_ref(DR, "celery:dispatch", "billing.charge", "COMMITTED", TS)


def test_failed_dispatch_records_failed_and_reraises():
    t = keystone_task(FakeTask(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        t.apply_async(kwargs={"boom": True})
    assert t.executions[-1]["outcome"] == "FAILED"
    assert t.execution_ref == execution_ref(DR, "celery:dispatch", "billing.charge", "FAILED", TS)


def test_passthrough_unbound():
    t = keystone_task(FakeTask(), decision_ref=DR, clock_ms=lambda: TS)
    assert t.run() == "ran"      # calling the body is not a dispatch
    assert t.executions == []


def test_rejects_bad_decision():
    with pytest.raises(KeystoneCeleryError):
        keystone_task(FakeTask(), decision_ref="")


def test_on_execution_callback():
    seen = []
    t = keystone_task(FakeTask(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    t.delay(1)
    assert seen[0]["execution_ref"] == t.execution_ref


def test_tamper_detected():
    ref = task_execution_ref(decision_ref=DR, task="billing.charge", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, task="billing.charge", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, task="billing.refund", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, task="billing.charge", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
