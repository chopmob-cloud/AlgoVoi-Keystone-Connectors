"""algovoi-keystone-pinecone -- bind Pinecone vector writes to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). Agent memory is a data plane: when a keystone decision
authorises an agent to write to its vector store, this binds each upsert / delete / update to that
decision, producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``pinecone:upsert`` / ``:delete`` / ``:update`` and ``scope`` is
``index/namespace``. ``pinecone_execution_ref`` is the standalone helper; ``keystone_index`` wraps
any Pinecone ``Index`` so each write emits an ``execution_ref``, with reads (query, fetch, ...)
passing through. No pinecone dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystonePineconeError",
    "EXECUTION_OUTCOMES",
    "pinecone_execution_ref",
    "KeystoneIndex",
    "keystone_index",
    "tamper_detected",
]

_ACTIONS = ("upsert", "delete", "update")


class KeystonePineconeError(ValueError):
    """Raised on invalid binding inputs."""


def pinecone_execution_ref(*, decision_ref: str, index: str, namespace: str, outcome: str,
                           executed_at_ms: int, action: str = "upsert") -> str:
    """Bind one Pinecone vector write to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystonePineconeError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if action not in _ACTIONS:
        raise KeystonePineconeError(f"action must be one of {_ACTIONS}")
    if not (isinstance(index, str) and index):
        raise KeystonePineconeError("index must be a non-empty string")
    if not isinstance(namespace, str):
        raise KeystonePineconeError("namespace must be a string")
    return execution_ref(decision_ref, f"pinecone:{action}", f"{index}/{namespace}", outcome,
                         executed_at_ms)


class KeystoneIndex:
    """Wrap a Pinecone ``Index`` so each vector write emits a keystone execution_ref.

    Attribute access falls through to the wrapped index, so it is a drop-in. The index name is
    taken from ``index_name=`` or the index's ``name`` attribute; the namespace from each call's
    ``namespace`` argument (default ``""``). ``execution_ref`` holds the last binding; ``executions``
    the full list. A write that raises is recorded ``FAILED`` and the error re-raised.
    """

    def __init__(self, index: Any, *, decision_ref: str, index_name: str | None = None,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystonePineconeError("decision_ref must be a non-empty string")
        self._idx = index
        self._decision_ref = decision_ref
        self._index_name = index_name or getattr(index, "name", "") or ""
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _wrap(self, action: str) -> Callable:
        fn = getattr(self._idx, action)

        def caller(*args: Any, **kwargs: Any) -> Any:
            namespace = kwargs.get("namespace", "") or ""
            ts = self._clock()
            try:
                result = fn(*args, **kwargs)
            except Exception:
                self._record(action, namespace, "FAILED", ts)
                raise
            self._record(action, namespace, "COMMITTED", ts)
            return result

        return caller

    def _record(self, action: str, namespace: str, outcome: str, ts: int) -> None:
        index = str(self._index_name or "")
        ref = pinecone_execution_ref(decision_ref=self._decision_ref, index=index,
                                     namespace=namespace, outcome=outcome, executed_at_ms=ts,
                                     action=action)
        rec = {"decision_ref": self._decision_ref, "scope": f"{index}/{namespace}",
               "action_type": f"pinecone:{action}", "index": index, "namespace": namespace,
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        if name in _ACTIONS:
            return self._wrap(name)
        return getattr(self._idx, name)


def keystone_index(index: Any, *, decision_ref: str, index_name: str | None = None,
                   clock_ms: Callable[[], int] | None = None,
                   on_execution: Callable[[dict], None] | None = None) -> KeystoneIndex:
    """Wrap a Pinecone ``Index`` so each vector write is bound to ``decision_ref``."""
    return KeystoneIndex(index, decision_ref=decision_ref, index_name=index_name,
                         clock_ms=clock_ms, on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, index: str, namespace: str,
                    outcome: str, executed_at_ms: int, action: str = "upsert") -> bool:
    """Return True if the write's fields do not recompute to the claimed execution_ref."""
    try:
        return pinecone_execution_ref(decision_ref=decision_ref, index=index, namespace=namespace,
                                      outcome=outcome, executed_at_ms=executed_at_ms,
                                      action=action) != claimed_ref
    except Exception:
        return True
