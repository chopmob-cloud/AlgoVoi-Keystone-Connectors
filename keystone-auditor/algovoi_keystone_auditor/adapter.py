"""algovoi-keystone-auditor -- bind archive audit reports to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). The keystone journal binds a commerce *decision* to an
*execution*; ``algovoi-keystone-archive`` carries that binding into sealed post-quantum storage,
and this connector carries it onward into *reporting*. Wrap the read-only Archive Auditor
(``algovoi_archive_auditor.Auditor``) and every signed audit report, integrity attestation and
evidence pack produces two content-addressed refs:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                              executed_at_ms }))
    binding_ref   = "sha256:" + SHA-256(JCS({ action_type: "auditor:bind", execution_ref,
                                              archive_kid, head_receipt_hash, entries,
                                              chain_valid }))

``action_type`` is ``auditor:report`` / ``auditor:attest`` / ``auditor:evidence_pack`` and ``scope``
is the content address of the signed report payload (the inner ``report`` / ``attestation`` /
``manifest`` object, RFC 8785 canonicalised), so the execution is bound to the exact report
produced, not merely to a call. The ``binding_ref`` ties that report to the sealed archive state it
covers (``archive_kid``, ``head_receipt_hash``, ``entries``, ``chain_valid``), joining the storage
level to the reporting level. The adapter refuses to bind unless the report attests a valid
``head_receipt_hash`` and ``chain_valid`` is true: a report over a broken or unidentifiable chain is
recorded ``FAILED`` with no binding and the error re-raised. Both refs recompute offline with a stock
RFC 8785 implementation and SHA-256; ``verify_binding`` / ``tamper_detected`` recompute and compare.
Full verification of a record is ``tamper_detected`` on the execution preimage AND ``verify_binding``
on the archive anchor AND ``outcome == COMMITTED``. The refs are content addresses, not signatures:
authenticity of the report comes from the auditor's own Falcon-1024 envelope, which the binding
points at. Reads (``verify``, ``inventory``, ``retention_report``, ``siem_events``, ``metrics_inputs``)
pass through unbound. A call that raises or is cancelled is recorded ``FAILED`` with no binding and
the error re-raised. No auditor dependency -- the client is duck-typed.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref
from algovoi_keystone_connect import keystone_ref

__all__ = [
    "KeystoneAuditorError",
    "EXECUTION_OUTCOMES",
    "ACTION_TYPES",
    "BIND_ACTION_TYPE",
    "auditor_execution_ref",
    "auditor_binding_ref",
    "verify_binding",
    "KeystoneAuditorClient",
    "keystone_auditor_client",
    "tamper_detected",
]

ACTION_TYPES = {
    "audit_report": "auditor:report",
    "attest": "auditor:attest",
    "evidence_pack": "auditor:evidence_pack",
}
BIND_ACTION_TYPE = "auditor:bind"
# The inner signed payload key, and the entry-count field, differ per wrapped method.
_PAYLOAD_KEY = {"audit_report": "report", "attest": "attestation", "evidence_pack": "manifest"}
_ENTRIES_KEY = {"audit_report": "entries", "attest": "entries", "evidence_pack": "entry_count"}
_HEX = "0123456789abcdef"
# Content address of empty content: the scope of a FAILED call that produced no report at all.
_EMPTY_SCOPE = "sha256:" + hashlib.sha256(b"").hexdigest()


class KeystoneAuditorError(ValueError):
    """Raised on invalid binding inputs or an audit report that cannot be bound."""


def _normalise_hash(h: Any) -> str:
    """Canonical ``sha256:<64 hex>``; reject anything that is not a SHA-256 digest."""
    if not isinstance(h, str):
        raise KeystoneAuditorError("head_receipt_hash must be a string")
    low = h.strip().lower()
    hexpart = low[7:] if low.startswith("sha256:") else low
    if len(hexpart) != 64 or any(c not in _HEX for c in hexpart):
        raise KeystoneAuditorError("head_receipt_hash must be a sha256 hex digest (64 hex characters)")
    return "sha256:" + hexpart


def auditor_execution_ref(*, decision_ref: str, action_type: str, scope: str,
                          outcome: str, executed_at_ms: int) -> str:
    """Bind one audit report to a keystone decision and return its execution_ref."""
    if not (isinstance(decision_ref, str) and decision_ref):
        raise KeystoneAuditorError("decision_ref must be a non-empty string")
    if action_type not in ACTION_TYPES.values():
        raise KeystoneAuditorError(f"action_type must be one of {sorted(ACTION_TYPES.values())}")
    if not (isinstance(scope, str) and scope):
        raise KeystoneAuditorError("scope must be a non-empty string")
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneAuditorError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not isinstance(executed_at_ms, int) or isinstance(executed_at_ms, bool):
        raise KeystoneAuditorError("executed_at_ms must be an epoch-millisecond integer")
    return execution_ref(decision_ref, action_type, scope, outcome, executed_at_ms)


def auditor_binding_ref(*, execution_ref: str, archive_kid: str, head_receipt_hash: str,
                        entries: int, chain_valid: bool) -> str:
    """Tie a report execution to the sealed archive state it covers; offline-recomputable."""
    if not (isinstance(execution_ref, str) and execution_ref):
        raise KeystoneAuditorError("execution_ref must be a non-empty string")
    if not (isinstance(archive_kid, str) and archive_kid):
        raise KeystoneAuditorError("archive_kid must be a non-empty string")
    head = _normalise_hash(head_receipt_hash)
    if not isinstance(entries, int) or isinstance(entries, bool):
        raise KeystoneAuditorError("entries must be an int")
    if not isinstance(chain_valid, bool):
        raise KeystoneAuditorError("chain_valid must be a bool")
    return keystone_ref({"action_type": BIND_ACTION_TYPE, "execution_ref": execution_ref,
                         "archive_kid": archive_kid, "head_receipt_hash": head,
                         "entries": entries, "chain_valid": chain_valid})


def verify_binding(claimed_ref: str, *, execution_ref: str, archive_kid: str,
                   head_receipt_hash: str, entries: int, chain_valid: bool) -> bool:
    """True when the claimed binding_ref recomputes from the given archive anchor.

    This verifies the reporting half only; pair it with ``tamper_detected`` on the execution
    preimage and an ``outcome == COMMITTED`` check to verify a whole record.
    """
    try:
        return auditor_binding_ref(execution_ref=execution_ref, archive_kid=archive_kid,
                                   head_receipt_hash=head_receipt_hash, entries=entries,
                                   chain_valid=chain_valid) == claimed_ref
    except Exception:
        return False


def tamper_detected(claimed_ref: str, *, decision_ref: str, action_type: str, scope: str,
                    outcome: str, executed_at_ms: int) -> bool:
    """True when the claimed execution_ref does NOT recompute from the public preimage."""
    try:
        return auditor_execution_ref(decision_ref=decision_ref, action_type=action_type,
                                     scope=scope, outcome=outcome,
                                     executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True


class KeystoneAuditorClient:
    """Wrap an async Archive Auditor so each signed report emits an execution_ref + binding_ref.

    Attribute access falls through to the wrapped auditor, so it is a drop-in. ``execution_ref``
    holds the last execution binding; ``executions`` the full list of records. Each successful
    record carries the archive anchor (``archive_kid``, ``head_receipt_hash``, ``entries``,
    ``chain_valid``) and a ``binding_ref``; a report over a broken chain, a missing or invalid
    anchor, or a call that raises or is cancelled is recorded ``FAILED`` with no binding and
    re-raises.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneAuditorError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    async def _bound(self, method: str, *args: Any, **kwargs: Any) -> Any:
        action_type = ACTION_TYPES[method]
        ts = int(self._clock())
        fn = getattr(self._c, method)
        try:
            result = await fn(*args, **kwargs)
        except asyncio.CancelledError:
            self._record(action_type, None, "FAILED", ts, None)
            raise
        except Exception:
            self._record(action_type, None, "FAILED", ts, None)
            raise

        payload_key = _PAYLOAD_KEY[method]
        if not isinstance(result, dict) or not isinstance(result.get(payload_key), dict):
            self._record(action_type, None, "FAILED", ts, result)
            raise KeystoneAuditorError(
                f"auditor response carried no {payload_key}; cannot bind the report")
        payload = result[payload_key]
        scope = keystone_ref(payload)  # content address of the exact report produced

        archive_kid = payload.get("archive_kid")
        entries = payload.get(_ENTRIES_KEY[method])
        chain_valid = payload.get("chain_valid")
        if not (isinstance(archive_kid, str) and archive_kid):
            self._record(action_type, scope, "FAILED", ts, result)
            raise KeystoneAuditorError("report carried no archive_kid; cannot bind to an archive")
        try:
            head = _normalise_hash(payload.get("head_receipt_hash"))
        except KeystoneAuditorError:
            self._record(action_type, scope, "FAILED", ts, result)
            raise
        if not isinstance(entries, int) or isinstance(entries, bool):
            self._record(action_type, scope, "FAILED", ts, result)
            raise KeystoneAuditorError("report carried no valid entry count; cannot bind")
        if not isinstance(chain_valid, bool):
            self._record(action_type, scope, "FAILED", ts, result)
            raise KeystoneAuditorError("report carried no chain_valid flag; cannot bind")
        if not chain_valid:
            self._record(action_type, scope, "FAILED", ts, result)
            raise KeystoneAuditorError(
                "report attests a broken chain; refusing to bind an integrity report")

        self._record(action_type, scope, "COMMITTED", ts, result,
                     anchor={"archive_kid": archive_kid, "head_receipt_hash": head,
                             "entries": entries, "chain_valid": chain_valid})
        return result

    async def audit_report(self, *args: Any, **kwargs: Any) -> Any:
        return await self._bound("audit_report", *args, **kwargs)

    async def attest(self, *args: Any, **kwargs: Any) -> Any:
        return await self._bound("attest", *args, **kwargs)

    async def evidence_pack(self, *args: Any, **kwargs: Any) -> Any:
        return await self._bound("evidence_pack", *args, **kwargs)

    def _record(self, action_type: str, scope: str | None, outcome: str, ts: int,
                result: Any, anchor: dict | None = None) -> None:
        # A FAILED call that produced no report has no content to address; use the empty-content
        # address so the execution is still addressable and honest about having produced nothing.
        eff_scope = scope if scope is not None else _EMPTY_SCOPE
        ref = auditor_execution_ref(decision_ref=self._decision_ref, action_type=action_type,
                                    scope=eff_scope, outcome=outcome, executed_at_ms=ts)
        rec = {"decision_ref": self._decision_ref, "scope": eff_scope, "action_type": action_type,
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        if outcome == "COMMITTED" and anchor is not None:
            rec.update(anchor)
            rec["binding_ref"] = auditor_binding_ref(
                execution_ref=ref, archive_kid=anchor["archive_kid"],
                head_receipt_hash=anchor["head_receipt_hash"], entries=anchor["entries"],
                chain_valid=anchor["chain_valid"])
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(dict(rec))

    def __getattr__(self, name: str) -> Any:
        client = self.__dict__.get("_c")
        if client is None:
            raise AttributeError(name)
        return getattr(client, name)


def keystone_auditor_client(client: Any, *, decision_ref: str,
                            clock_ms: Callable[[], int] | None = None,
                            on_execution: Callable[[dict], None] | None = None) -> KeystoneAuditorClient:
    """Factory: wrap an async Archive Auditor, binding every signed report to ``decision_ref``."""
    return KeystoneAuditorClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                                 on_execution=on_execution)
