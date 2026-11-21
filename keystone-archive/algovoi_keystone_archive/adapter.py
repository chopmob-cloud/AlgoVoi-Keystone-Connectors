"""algovoi-keystone-archive -- bind post-quantum archive writes to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). The keystone journal binds a commerce *decision* to an
*execution*; this connector carries that binding onward into durable, post-quantum-signed storage.
Wrap any archive client exposing an async ``notarize`` (the reference implementation is
``algovoi_doc_archive.Archive``) and every sealed document produces two content-addressed refs:

    execution_ref  = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                              executed_at_ms }))
    binding_ref    = "sha256:" + SHA-256(JCS({ action_type: "archive:bind", execution_ref,
                                              doc_hash, entry_id, seq }))

``action_type`` is ``archive:notarize`` and ``scope`` is the content address of the notarized bytes
(``sha256:<hex>``), so the execution is bound to the exact document, not merely to a call. The
adapter snapshots the bytes it hashes and forwards that snapshot, and it refuses to bind unless the
archive attests a ``doc_hash`` equal to ``scope`` -- a substituted or missing document address is
recorded ``FAILED`` with no binding. The ``binding_ref`` ties that execution to the archive entry
(``doc_hash``, ``entry_id``, ``seq``), joining the commerce level to the storage level. Both refs
recompute offline with a stock RFC 8785 implementation and SHA-256; ``verify_binding`` /
``tamper_detected`` recompute and compare. Full verification of a record is ``tamper_detected`` on
the execution preimage AND ``verify_binding`` on the storage identity AND ``outcome == COMMITTED``.
The refs are content addresses, not signatures: authenticity of the sealed entry comes from the
archive's own post-quantum signature, which the binding points at. Reads (``retrieve``,
``verify_chain``) pass through unbound. A notarize that raises or is cancelled is recorded ``FAILED``
with no storage binding, and the error re-raised. No archive dependency -- the client is duck-typed.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref
from algovoi_keystone_connect import keystone_ref

__all__ = [
    "KeystoneArchiveError",
    "EXECUTION_OUTCOMES",
    "archive_execution_ref",
    "archive_binding_ref",
    "verify_binding",
    "KeystoneArchiveClient",
    "keystone_archive_client",
    "tamper_detected",
]

ACTION_TYPE = "archive:notarize"
BIND_ACTION_TYPE = "archive:bind"
_HEX = "0123456789abcdef"


class KeystoneArchiveError(ValueError):
    """Raised on invalid binding inputs or an archive response that cannot be bound."""


def _content_hash(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _normalise_hash(h: Any) -> str:
    """Canonical ``sha256:<64 hex>``; reject anything that is not a SHA-256 digest."""
    if not isinstance(h, str):
        raise KeystoneArchiveError("doc_hash must be a string")
    low = h.strip().lower()
    hexpart = low[7:] if low.startswith("sha256:") else low
    if len(hexpart) != 64 or any(c not in _HEX for c in hexpart):
        raise KeystoneArchiveError("doc_hash must be a sha256 hex digest (64 hex characters)")
    return "sha256:" + hexpart


def archive_execution_ref(*, decision_ref: str, scope: str, outcome: str,
                          executed_at_ms: int) -> str:
    """Bind one archive notarize to a keystone decision and return its execution_ref."""
    if not (isinstance(decision_ref, str) and decision_ref):
        raise KeystoneArchiveError("decision_ref must be a non-empty string")
    if not (isinstance(scope, str) and scope):
        raise KeystoneArchiveError("scope must be a non-empty string")
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneArchiveError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not isinstance(executed_at_ms, int) or isinstance(executed_at_ms, bool):
        raise KeystoneArchiveError("executed_at_ms must be an epoch-millisecond integer")
    return execution_ref(decision_ref, ACTION_TYPE, scope, outcome, executed_at_ms)


def archive_binding_ref(*, execution_ref: str, doc_hash: str, entry_id: str, seq: int) -> str:
    """Tie an execution to its sealed archive entry; content-addressed and offline-recomputable."""
    if not (isinstance(execution_ref, str) and execution_ref):
        raise KeystoneArchiveError("execution_ref must be a non-empty string")
    if not (isinstance(doc_hash, str) and doc_hash):
        raise KeystoneArchiveError("doc_hash must be a non-empty string")
    if not (isinstance(entry_id, str) and entry_id):
        raise KeystoneArchiveError("entry_id must be a non-empty string")
    if not isinstance(seq, int) or isinstance(seq, bool):
        raise KeystoneArchiveError("seq must be an int")
    return keystone_ref({"action_type": BIND_ACTION_TYPE, "execution_ref": execution_ref,
                         "doc_hash": doc_hash, "entry_id": entry_id, "seq": seq})


def verify_binding(claimed_ref: str, *, execution_ref: str, doc_hash: str,
                   entry_id: str, seq: int) -> bool:
    """True when the claimed binding_ref recomputes from the given storage identity.

    This verifies the storage half only; pair it with ``tamper_detected`` on the execution
    preimage and an ``outcome == COMMITTED`` check to verify a whole record.
    """
    try:
        return archive_binding_ref(execution_ref=execution_ref, doc_hash=doc_hash,
                                   entry_id=entry_id, seq=seq) == claimed_ref
    except Exception:
        return False


def tamper_detected(claimed_ref: str, *, decision_ref: str, scope: str, outcome: str,
                    executed_at_ms: int) -> bool:
    """True when the claimed execution_ref does NOT recompute from the public preimage."""
    try:
        return archive_execution_ref(decision_ref=decision_ref, scope=scope, outcome=outcome,
                                     executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True


class KeystoneArchiveClient:
    """Wrap an async archive client so each ``notarize`` emits an execution_ref + binding_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. ``execution_ref``
    holds the last execution binding; ``executions`` the full list of records. Each successful
    record carries the descriptive storage identity (``doc_hash``, ``entry_id``, ``seq``) and a
    ``binding_ref``; a failed, cancelled or unbindable notarize is recorded ``FAILED`` with no
    binding and re-raises.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneArchiveError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    async def notarize(self, *args: Any, **kwargs: Any) -> Any:
        data = kwargs.get("data")
        if data is None and args:
            data = args[0]
        doc_hash = kwargs.get("doc_hash")
        if data is None and doc_hash is None:
            raise KeystoneArchiveError("notarize requires data or doc_hash")

        if data is not None:
            if not isinstance(data, (bytes, bytearray)):
                raise KeystoneArchiveError("data must be bytes")
            payload = bytes(data)  # snapshot: a live bytearray cannot change what is sealed
            scope = _content_hash(payload)
            if doc_hash is not None and _normalise_hash(doc_hash) != scope:
                self._record(scope, "FAILED", int(self._clock()), None)
                raise KeystoneArchiveError("supplied doc_hash does not match the data")
            # forward the snapshot, never the caller's live buffer
            if "data" in kwargs:
                kwargs = {**kwargs, "data": payload}
            elif args:
                args = (payload,) + tuple(args[1:])
        else:
            scope = _normalise_hash(doc_hash)

        ts = int(self._clock())
        # Validate the complete evidence preimage before any side effect.
        archive_execution_ref(decision_ref=self._decision_ref, scope=scope,
                              outcome="COMMITTED", executed_at_ms=ts)
        fn = getattr(self._c, "notarize")
        try:
            result = await fn(*args, **kwargs)
        except asyncio.CancelledError:
            self._record(scope, "FAILED", ts, None)
            raise
        except Exception:
            self._record(scope, "FAILED", ts, None)
            raise

        # The archive must attest the EXACT document we scoped, with a usable entry identity.
        if not isinstance(result, dict) or "doc_hash" not in result:
            self._record(scope, "FAILED", ts, result)
            raise KeystoneArchiveError("archive response carried no doc_hash; cannot bind the document")
        try:
            attested = _normalise_hash(result["doc_hash"])
        except KeystoneArchiveError:
            self._record(scope, "FAILED", ts, result)
            raise
        if attested != scope:
            self._record(scope, "FAILED", ts, result)
            raise KeystoneArchiveError(
                "archive doc_hash does not match the scoped document; refusing to bind")
        entry_id, seq = result.get("entry_id"), result.get("seq")
        if not (isinstance(entry_id, str) and entry_id
                and isinstance(seq, int) and not isinstance(seq, bool)):
            self._record(scope, "FAILED", ts, result)
            raise KeystoneArchiveError("archive response carried no valid entry_id/seq; cannot bind")

        self._record(scope, "COMMITTED", ts, result)
        return result

    def _record(self, scope: str, outcome: str, ts: int, result: Any) -> None:
        ref = archive_execution_ref(decision_ref=self._decision_ref, scope=scope,
                                    outcome=outcome, executed_at_ms=ts)
        rec = {"decision_ref": self._decision_ref, "scope": scope, "action_type": ACTION_TYPE,
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        if outcome == "COMMITTED" and isinstance(result, dict):
            # scope is the attested content address (validated equal to result["doc_hash"]).
            rec["doc_hash"] = scope
            rec["entry_id"] = result["entry_id"]
            rec["seq"] = result["seq"]
            rec["binding_ref"] = archive_binding_ref(
                execution_ref=ref, doc_hash=scope,
                entry_id=result["entry_id"], seq=result["seq"])
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(dict(rec))

    def __getattr__(self, name: str) -> Any:
        client = self.__dict__.get("_c")
        if client is None:
            raise AttributeError(name)
        return getattr(client, name)


def keystone_archive_client(client: Any, *, decision_ref: str,
                            clock_ms: Callable[[], int] | None = None,
                            on_execution: Callable[[dict], None] | None = None) -> KeystoneArchiveClient:
    """Factory: wrap an async archive client, binding every notarize to ``decision_ref``."""
    return KeystoneArchiveClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                                 on_execution=on_execution)
