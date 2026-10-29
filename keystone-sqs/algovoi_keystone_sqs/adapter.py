"""algovoi-keystone-sqs -- bind AWS SQS message sends to the AlgoVoi keystone (boto3).

Keystone-only edition (open, Apache-2.0). The AWS counterpart to the Kafka, AMQP, NATS and Azure
Service Bus connectors: each message sent to a queue is bound to the keystone decision that
authorised it, producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``sqs:send`` and ``scope`` is the queue name (the final segment of the
QueueUrl). ``sqs_execution_ref`` is the standalone helper; ``keystone_sqs_client`` wraps any boto3
SQS client so ``send_message`` / ``send_message_batch`` each emit an ``execution_ref``, with
receives and deletes passing through. No boto3 dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneSQSError",
    "EXECUTION_OUTCOMES",
    "sqs_execution_ref",
    "KeystoneSQSClient",
    "keystone_sqs_client",
    "tamper_detected",
]

_SEND_METHODS = ("send_message", "send_message_batch")


class KeystoneSQSError(ValueError):
    """Raised on invalid binding inputs."""


def _queue_name(queue_url: Any) -> str:
    s = str(queue_url or "")
    return s.rstrip("/").split("/")[-1] if s else ""


def sqs_execution_ref(*, decision_ref: str, queue: str, outcome: str,
                      executed_at_ms: int) -> str:
    """Bind one SQS send to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneSQSError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not (isinstance(queue, str) and queue):
        raise KeystoneSQSError("queue must be a non-empty string")
    return execution_ref(decision_ref, "sqs:send", queue, outcome, executed_at_ms)


class KeystoneSQSClient:
    """Wrap a boto3 SQS client so each send emits a keystone execution_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. The queue is the final
    segment of the call's QueueUrl. ``execution_ref`` holds the last binding; ``executions`` the full
    list. A send that raises is recorded ``FAILED`` and the error re-raised.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneSQSError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _wrap(self, name: str) -> Callable:
        fn = getattr(self._c, name)

        def caller(*args: Any, **kwargs: Any) -> Any:
            queue = _queue_name(kwargs.get("QueueUrl"))
            ts = self._clock()
            try:
                result = fn(*args, **kwargs)
            except Exception:
                self._record(queue, "FAILED", ts)
                raise
            self._record(queue, "COMMITTED", ts)
            return result

        return caller

    def _record(self, queue: str, outcome: str, ts: int) -> None:
        ref = sqs_execution_ref(decision_ref=self._decision_ref, queue=queue,
                                outcome=outcome, executed_at_ms=ts)
        self.execution_ref = ref
        rec = {"decision_ref": self._decision_ref, "scope": queue,
               "queue": queue, "action_type": "sqs:send",
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        if name in _SEND_METHODS:
            return self._wrap(name)
        return getattr(self._c, name)


def keystone_sqs_client(client: Any, *, decision_ref: str,
                        clock_ms: Callable[[], int] | None = None,
                        on_execution: Callable[[dict], None] | None = None) -> KeystoneSQSClient:
    """Wrap a boto3 SQS client so each send is bound to ``decision_ref``."""
    return KeystoneSQSClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                             on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, queue: str, outcome: str,
                    executed_at_ms: int) -> bool:
    """Return True if the send's fields do not recompute to the claimed execution_ref."""
    try:
        return sqs_execution_ref(decision_ref=decision_ref, queue=queue, outcome=outcome,
                                 executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True
