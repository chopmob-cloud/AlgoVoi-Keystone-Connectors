"""algovoi-keystone-azure-servicebus -- bind Azure Service Bus sends to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). The Azure cloud counterpart to the Kafka, AMQP and NATS
connectors: each message sent to a queue or topic is bound to the keystone decision that
authorised it, producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``servicebus:send`` and ``scope`` is the entity (queue or topic name).
``servicebus_execution_ref`` is the standalone helper; ``keystone_sender`` wraps any
azure-servicebus ``ServiceBusSender`` so ``send_messages`` / ``schedule_messages`` each emit an
``execution_ref``, with receivers and management calls passing through. No azure-servicebus
dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneServiceBusError",
    "EXECUTION_OUTCOMES",
    "servicebus_execution_ref",
    "KeystoneServiceBusSender",
    "keystone_sender",
    "tamper_detected",
]

_SEND_METHODS = ("send_messages", "schedule_messages")


class KeystoneServiceBusError(ValueError):
    """Raised on invalid binding inputs."""


def servicebus_execution_ref(*, decision_ref: str, entity: str, outcome: str,
                             executed_at_ms: int) -> str:
    """Bind one Service Bus send to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneServiceBusError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not (isinstance(entity, str) and entity):
        raise KeystoneServiceBusError("entity must be a non-empty string")
    return execution_ref(decision_ref, "servicebus:send", entity, outcome, executed_at_ms)


class KeystoneServiceBusSender:
    """Wrap an Azure ``ServiceBusSender`` so each send emits a keystone execution_ref.

    Attribute access falls through to the wrapped sender, so it is a drop-in. The entity (queue or
    topic) is taken from ``entity=`` or the sender's ``entity_name`` attribute. ``execution_ref``
    holds the last binding; ``executions`` the full list. A send that raises is recorded ``FAILED``
    and the error re-raised.
    """

    def __init__(self, sender: Any, *, decision_ref: str, entity: str | None = None,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneServiceBusError("decision_ref must be a non-empty string")
        self._s = sender
        self._decision_ref = decision_ref
        self._entity = (entity or getattr(sender, "entity_name", None)
                        or getattr(sender, "_entity_name", "") or "")
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _wrap(self, name: str) -> Callable:
        fn = getattr(self._s, name)

        def caller(*args: Any, **kwargs: Any) -> Any:
            ts = self._clock()
            try:
                result = fn(*args, **kwargs)
            except Exception:
                self._record("FAILED", ts)
                raise
            self._record("COMMITTED", ts)
            return result

        return caller

    def _record(self, outcome: str, ts: int) -> None:
        entity = str(self._entity or "")
        ref = servicebus_execution_ref(decision_ref=self._decision_ref, entity=entity,
                                       outcome=outcome, executed_at_ms=ts)
        self.execution_ref = ref
        rec = {"decision_ref": self._decision_ref, "scope": entity,
               "entity": entity, "action_type": "servicebus:send",
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        if name in _SEND_METHODS:
            return self._wrap(name)
        return getattr(self._s, name)


def keystone_sender(sender: Any, *, decision_ref: str, entity: str | None = None,
                    clock_ms: Callable[[], int] | None = None,
                    on_execution: Callable[[dict], None] | None = None) -> KeystoneServiceBusSender:
    """Wrap a ``ServiceBusSender`` so each send is bound to ``decision_ref``."""
    return KeystoneServiceBusSender(sender, decision_ref=decision_ref, entity=entity,
                                    clock_ms=clock_ms, on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, entity: str, outcome: str,
                    executed_at_ms: int) -> bool:
    """Return True if the send's fields do not recompute to the claimed execution_ref."""
    try:
        return servicebus_execution_ref(decision_ref=decision_ref, entity=entity, outcome=outcome,
                                        executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True
