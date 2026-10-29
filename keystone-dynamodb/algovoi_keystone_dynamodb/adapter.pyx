"""algovoi-keystone-dynamodb -- bind AWS DynamoDB writes to the AlgoVoi keystone (boto3).

Keystone-only edition (open, Apache-2.0). AWS's dominant NoSQL store, the cloud-NoSQL counterpart
to the MongoDB connector: each item write is bound to the keystone decision that authorised it,
producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``dynamodb:put`` / ``:update`` / ``:delete`` / ``:batch_write`` and
``scope`` is the table name. ``dynamodb_execution_ref`` is the standalone helper;
``keystone_dynamodb_client`` wraps any boto3 DynamoDB client so ``put_item`` / ``update_item`` /
``delete_item`` / ``batch_write_item`` each emit an ``execution_ref``, with reads (get_item, query,
scan, ...) passing through. No boto3 dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneDynamoDBError",
    "EXECUTION_OUTCOMES",
    "dynamodb_execution_ref",
    "KeystoneDynamoDBClient",
    "keystone_dynamodb_client",
    "tamper_detected",
]

_ACTIONS = ("put", "update", "delete", "batch_write")


def _batch_tables(kwargs: dict) -> str:
    items = kwargs.get("RequestItems") or {}
    try:
        return ",".join(sorted(items.keys()))
    except Exception:
        return ""


# method name -> (action, extractor(args, kwargs) -> table scope)
_WRITE_METHODS: dict = {
    "put_item": ("put", lambda a, k: k.get("TableName")),
    "update_item": ("update", lambda a, k: k.get("TableName")),
    "delete_item": ("delete", lambda a, k: k.get("TableName")),
    "batch_write_item": ("batch_write", lambda a, k: _batch_tables(k)),
}


class KeystoneDynamoDBError(ValueError):
    """Raised on invalid binding inputs."""


def dynamodb_execution_ref(*, decision_ref: str, table: str, outcome: str,
                           executed_at_ms: int, action: str = "put") -> str:
    """Bind one DynamoDB write to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneDynamoDBError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if action not in _ACTIONS:
        raise KeystoneDynamoDBError(f"action must be one of {_ACTIONS}")
    if not (isinstance(table, str) and table):
        raise KeystoneDynamoDBError("table must be a non-empty string")
    return execution_ref(decision_ref, f"dynamodb:{action}", table, outcome, executed_at_ms)


class KeystoneDynamoDBClient:
    """Wrap a boto3 DynamoDB client so each write emits a keystone execution_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. The scope is the
    ``TableName`` (or the comma-joined tables of a batch write). ``execution_ref`` holds the last
    binding; ``executions`` the full list. A write that raises is recorded ``FAILED`` and the error
    re-raised.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneDynamoDBError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _wrap(self, name: str, action: str, extract: Callable) -> Callable:
        fn = getattr(self._c, name)

        def caller(*args: Any, **kwargs: Any) -> Any:
            table = extract(args, kwargs)
            ts = self._clock()
            try:
                result = fn(*args, **kwargs)
            except Exception:
                self._record(action, table, "FAILED", ts)
                raise
            self._record(action, table, "COMMITTED", ts)
            return result

        return caller

    def _record(self, action: str, table: Any, outcome: str, ts: int) -> None:
        table_s = str(table or "")
        ref = dynamodb_execution_ref(decision_ref=self._decision_ref, table=table_s,
                                     outcome=outcome, executed_at_ms=ts, action=action)
        rec = {"decision_ref": self._decision_ref, "scope": table_s,
               "action_type": f"dynamodb:{action}", "table": table,
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        spec = _WRITE_METHODS.get(name)
        if spec is not None:
            return self._wrap(name, spec[0], spec[1])
        return getattr(self._c, name)


def keystone_dynamodb_client(client: Any, *, decision_ref: str,
                             clock_ms: Callable[[], int] | None = None,
                             on_execution: Callable[[dict], None] | None = None
                             ) -> KeystoneDynamoDBClient:
    """Wrap a boto3 DynamoDB client so each write is bound to ``decision_ref``."""
    return KeystoneDynamoDBClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                                  on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, table: str, outcome: str,
                    executed_at_ms: int, action: str = "put") -> bool:
    """Return True if the write's fields do not recompute to the claimed execution_ref."""
    try:
        return dynamodb_execution_ref(decision_ref=decision_ref, table=table, outcome=outcome,
                                      executed_at_ms=executed_at_ms, action=action) != claimed_ref
    except Exception:
        return True
