"""algovoi-keystone-sns -- bind AWS SNS publishes to the AlgoVoi keystone (boto3).

Keystone-only edition (open, Apache-2.0). Each message published to an SNS topic is bound to the
keystone decision that authorised it, producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``sns:publish`` and ``scope`` is the topic (the final segment of the
TopicArn, or the target / phone number). ``sns_execution_ref`` is the standalone helper;
``keystone_sns_client`` wraps any boto3 SNS client so ``publish`` emits an ``execution_ref``, with
every other call passing through. No boto3 dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneSNSError",
    "EXECUTION_OUTCOMES",
    "sns_execution_ref",
    "KeystoneSNSClient",
    "keystone_sns_client",
    "tamper_detected",
]


class KeystoneSNSError(ValueError):
    """Raised on invalid binding inputs."""


def _topic_name(target: Any) -> str:
    s = str(target or "")
    return s.split(":")[-1] if ":" in s else s


def sns_execution_ref(*, decision_ref: str, topic: str, outcome: str,
                      executed_at_ms: int) -> str:
    """Bind one SNS publish to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneSNSError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not (isinstance(topic, str) and topic):
        raise KeystoneSNSError("topic must be a non-empty string")
    return execution_ref(decision_ref, "sns:publish", topic, outcome, executed_at_ms)


class KeystoneSNSClient:
    """Wrap a boto3 SNS client so each ``publish`` emits a keystone execution_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. The topic is taken
    from the call's ``TopicArn`` (or ``TargetArn`` / ``PhoneNumber``). ``execution_ref`` holds the
    last binding; ``executions`` the full list. A publish that raises is recorded ``FAILED`` and the
    error re-raised.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneSNSError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _publish(self, *args: Any, **kwargs: Any) -> Any:
        target = kwargs.get("TopicArn") or kwargs.get("TargetArn") or kwargs.get("PhoneNumber")
        topic = _topic_name(target)
        ts = self._clock()
        fn = getattr(self._c, "publish")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._record(topic, "FAILED", ts)
            raise
        self._record(topic, "COMMITTED", ts)
        return result

    def publish(self, *args: Any, **kwargs: Any) -> Any:
        return self._publish(*args, **kwargs)

    def _record(self, topic: str, outcome: str, ts: int) -> None:
        ref = sns_execution_ref(decision_ref=self._decision_ref, topic=topic,
                                outcome=outcome, executed_at_ms=ts)
        self.execution_ref = ref
        rec = {"decision_ref": self._decision_ref, "scope": topic,
               "topic": topic, "action_type": "sns:publish",
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._c, name)


def keystone_sns_client(client: Any, *, decision_ref: str,
                        clock_ms: Callable[[], int] | None = None,
                        on_execution: Callable[[dict], None] | None = None) -> KeystoneSNSClient:
    """Wrap a boto3 SNS client so each publish is bound to ``decision_ref``."""
    return KeystoneSNSClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                             on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, topic: str, outcome: str,
                    executed_at_ms: int) -> bool:
    """Return True if the publish's fields do not recompute to the claimed execution_ref."""
    try:
        return sns_execution_ref(decision_ref=decision_ref, topic=topic, outcome=outcome,
                                 executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True
