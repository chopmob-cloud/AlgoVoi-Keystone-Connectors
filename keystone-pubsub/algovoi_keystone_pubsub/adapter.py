"""algovoi-keystone-pubsub -- bind Google Cloud Pub/Sub publishes to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). The GCP counterpart to the Kafka, AMQP, NATS, Azure
Service Bus and AWS SQS/SNS connectors: each message published to a topic is bound to the keystone
decision that authorised it, producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``pubsub:publish`` and ``scope`` is the topic (the final segment of the
``projects/<p>/topics/<t>`` path). ``pubsub_execution_ref`` is the standalone helper;
``keystone_publisher`` wraps any google-cloud-pubsub ``PublisherClient`` so ``publish`` emits an
``execution_ref``, with every other call passing through. No google-cloud-pubsub dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystonePubSubError",
    "EXECUTION_OUTCOMES",
    "pubsub_execution_ref",
    "KeystonePublisherClient",
    "keystone_publisher",
    "tamper_detected",
]


class KeystonePubSubError(ValueError):
    """Raised on invalid binding inputs."""


def _topic_name(topic: Any) -> str:
    s = str(topic or "")
    return s.rstrip("/").split("/")[-1] if s else ""


def pubsub_execution_ref(*, decision_ref: str, topic: str, outcome: str,
                         executed_at_ms: int) -> str:
    """Bind one Pub/Sub publish to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystonePubSubError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not (isinstance(topic, str) and topic):
        raise KeystonePubSubError("topic must be a non-empty string")
    return execution_ref(decision_ref, "pubsub:publish", topic, outcome, executed_at_ms)


class KeystonePublisherClient:
    """Wrap a google-cloud-pubsub ``PublisherClient`` so each ``publish`` emits an execution_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. The topic is the final
    segment of the topic path passed to ``publish``. ``execution_ref`` holds the last binding;
    ``executions`` the full list. A publish that raises is recorded ``FAILED`` and the error
    re-raised.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystonePubSubError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def publish(self, *args: Any, **kwargs: Any) -> Any:
        topic = _topic_name(args[0] if args else kwargs.get("topic"))
        ts = self._clock()
        fn = getattr(self._c, "publish")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._record(topic, "FAILED", ts)
            raise
        self._record(topic, "COMMITTED", ts)
        return result

    def _record(self, topic: str, outcome: str, ts: int) -> None:
        ref = pubsub_execution_ref(decision_ref=self._decision_ref, topic=topic,
                                   outcome=outcome, executed_at_ms=ts)
        self.execution_ref = ref
        rec = {"decision_ref": self._decision_ref, "scope": topic,
               "topic": topic, "action_type": "pubsub:publish",
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._c, name)


def keystone_publisher(client: Any, *, decision_ref: str,
                       clock_ms: Callable[[], int] | None = None,
                       on_execution: Callable[[dict], None] | None = None) -> KeystonePublisherClient:
    """Wrap a Pub/Sub ``PublisherClient`` so each publish is bound to ``decision_ref``."""
    return KeystonePublisherClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                                   on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, topic: str, outcome: str,
                    executed_at_ms: int) -> bool:
    """Return True if the publish's fields do not recompute to the claimed execution_ref."""
    try:
        return pubsub_execution_ref(decision_ref=decision_ref, topic=topic, outcome=outcome,
                                    executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True
