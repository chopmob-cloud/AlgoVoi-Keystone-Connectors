"""Unit tests for algovoi-keystone-auditor (fake auditor; no AlgoVoi auditor dependency)."""
from __future__ import annotations

import asyncio
import hashlib

import pytest

from algovoi_keystone_auditor import (
    ACTION_TYPES,
    BIND_ACTION_TYPE,
    KeystoneAuditorError,
    auditor_binding_ref,
    auditor_execution_ref,
    keystone_auditor_client,
    tamper_detected,
    verify_binding,
)
from algovoi_keystone_connect import keystone_ref

HEAD = "sha256:" + hashlib.sha256(b"archive-head").hexdigest()
KID = "kid:archive:test"
DREF = "sha256:" + hashlib.sha256(b"decision").hexdigest()


def _report(**over):
    r = {"type": "archive_audit_report", "version": "1", "archive_kid": KID,
         "entries": 3, "chain_valid": True, "broken_at": None, "reason": None,
         "head_receipt_hash": HEAD, "auditor_did": "did:web:auditor.example.com",
         "generated_at": "2026-11-21T00:00:00Z"}
    r.update(over)
    return r


def _attestation(**over):
    a = {"type": "archive_integrity_attestation", "version": "1", "archive_kid": KID,
         "entries": 3, "chain_valid": True, "broken_at": None, "head_receipt_hash": HEAD,
         "prev_attestation_hash": None, "auditor_did": "did:web:auditor.example.com",
         "attested_at": "2026-11-21T00:00:00Z"}
    a.update(over)
    return a


def _manifest(**over):
    m = {"type": "archive_evidence_pack_manifest", "version": "1", "archive_kid": KID,
         "entry_count": 2, "entry_seqs": [1, 2], "entry_hashes": ["a", "b"],
         "head_receipt_hash": HEAD, "chain_valid": True,
         "auditor_did": "did:web:auditor.example.com", "generated_at": "2026-11-21T00:00:00Z"}
    m.update(over)
    return m


class FakeAuditor:
    """Duck-typed stand-in for algovoi_archive_auditor.Auditor."""

    def __init__(self, *, report=None, attestation=None, manifest=None, raise_exc=None):
        self._report = report if report is not None else _report()
        self._attestation = attestation if attestation is not None else _attestation()
        self._manifest = manifest if manifest is not None else _manifest()
        self._raise = raise_exc
        self.calls = []

    async def audit_report(self, *, sign=True):
        self.calls.append("audit_report")
        if self._raise:
            raise self._raise
        return {"report": self._report, "report_envelope": "env"}

    async def attest(self, *, prev_attestation_hash=None, sign=True):
        self.calls.append("attest")
        if self._raise:
            raise self._raise
        return {"attestation": self._attestation, "attestation_envelope": "env",
                "attestation_hash": "h"}

    async def evidence_pack(self, *, seqs=None, sign=True):
        self.calls.append("evidence_pack")
        if self._raise:
            raise self._raise
        return {"manifest": self._manifest, "manifest_envelope": "env", "entries": ["r1", "r2"],
                "archive_kid": KID}

    async def verify(self):
        self.calls.append("verify")
        return {"valid": True}

    async def inventory(self):
        self.calls.append("inventory")
        return [{"seq": 1}]


def _client(fake, **kw):
    return keystone_auditor_client(fake, decision_ref=DREF, clock_ms=lambda: 1700000000000, **kw)


# --- happy path: each signed output binds COMMITTED with both refs -------------------------

async def test_audit_report_binds_committed():
    c = _client(FakeAuditor())
    out = await c.audit_report()
    assert out["report"]["chain_valid"] is True
    rec = c.executions[-1]
    assert rec["outcome"] == "COMMITTED"
    assert rec["action_type"] == "auditor:report"
    assert rec["archive_kid"] == KID
    assert rec["head_receipt_hash"] == HEAD
    assert rec["entries"] == 3
    assert rec["chain_valid"] is True
    assert rec["scope"] == keystone_ref(out["report"])
    assert "binding_ref" in rec and "execution_ref" in rec


async def test_attest_binds_committed():
    c = _client(FakeAuditor())
    out = await c.attest()
    rec = c.executions[-1]
    assert rec["outcome"] == "COMMITTED"
    assert rec["action_type"] == "auditor:attest"
    assert rec["scope"] == keystone_ref(out["attestation"])
    assert rec["entries"] == 3
    assert "binding_ref" in rec


async def test_evidence_pack_binds_committed_entry_count():
    c = _client(FakeAuditor())
    out = await c.evidence_pack()
    rec = c.executions[-1]
    assert rec["outcome"] == "COMMITTED"
    assert rec["action_type"] == "auditor:evidence_pack"
    assert rec["entries"] == 2  # from manifest entry_count
    assert rec["scope"] == keystone_ref(out["manifest"])
    assert "binding_ref" in rec


# --- offline recomputation -----------------------------------------------------------------

async def test_execution_ref_recomputes_offline():
    c = _client(FakeAuditor())
    await c.audit_report()
    rec = c.executions[-1]
    assert tamper_detected(rec["execution_ref"], decision_ref=DREF,
                           action_type=rec["action_type"], scope=rec["scope"],
                           outcome=rec["outcome"], executed_at_ms=rec["executed_at_ms"]) is False


async def test_binding_ref_recomputes_offline():
    c = _client(FakeAuditor())
    await c.audit_report()
    rec = c.executions[-1]
    assert verify_binding(rec["binding_ref"], execution_ref=rec["execution_ref"],
                          archive_kid=KID, head_receipt_hash=HEAD, entries=3,
                          chain_valid=True) is True


async def test_tamper_detected_on_wrong_scope():
    c = _client(FakeAuditor())
    await c.audit_report()
    rec = c.executions[-1]
    assert tamper_detected(rec["execution_ref"], decision_ref=DREF,
                           action_type=rec["action_type"], scope="sha256:" + "0" * 64,
                           outcome=rec["outcome"], executed_at_ms=rec["executed_at_ms"]) is True


async def test_verify_binding_false_on_wrong_head():
    c = _client(FakeAuditor())
    await c.audit_report()
    rec = c.executions[-1]
    other = "sha256:" + hashlib.sha256(b"different-head").hexdigest()
    assert verify_binding(rec["binding_ref"], execution_ref=rec["execution_ref"],
                          archive_kid=KID, head_receipt_hash=other, entries=3,
                          chain_valid=True) is False


# --- fail-closed guards --------------------------------------------------------------------

async def test_broken_chain_records_failed_and_raises():
    c = _client(FakeAuditor(report=_report(chain_valid=False, broken_at=2)))
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    rec = c.executions[-1]
    assert rec["outcome"] == "FAILED"
    assert "binding_ref" not in rec


async def test_missing_head_receipt_hash_failed():
    c = _client(FakeAuditor(report=_report(head_receipt_hash=None)))
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"
    assert "binding_ref" not in c.executions[-1]


@pytest.mark.parametrize("bad", [None, 123, "zz", "sha256:nothex", "sha256:" + "g" * 64])
async def test_malformed_head_receipt_hash_failed(bad):
    c = _client(FakeAuditor(report=_report(head_receipt_hash=bad)))
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"
    assert "binding_ref" not in c.executions[-1]


async def test_missing_archive_kid_failed():
    c = _client(FakeAuditor(report=_report(archive_kid="")))
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"


async def test_missing_payload_key_failed():
    class NoReport:
        async def audit_report(self, *, sign=True):
            return {"report_envelope": "env"}
    c = _client(NoReport())
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"


async def test_non_dict_result_failed():
    class NotDict:
        async def audit_report(self, *, sign=True):
            return "not a dict"
    c = _client(NotDict())
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"


@pytest.mark.parametrize("bad", [None, "3", True])
async def test_invalid_entries_failed(bad):
    c = _client(FakeAuditor(report=_report(entries=bad)))
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"


@pytest.mark.parametrize("bad", ["true", 1, None])
async def test_chain_valid_non_bool_failed(bad):
    c = _client(FakeAuditor(report=_report(chain_valid=bad)))
    with pytest.raises(KeystoneAuditorError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"


async def test_call_raises_records_failed():
    c = _client(FakeAuditor(raise_exc=RuntimeError("auditor exploded")))
    with pytest.raises(RuntimeError):
        await c.audit_report()
    rec = c.executions[-1]
    assert rec["outcome"] == "FAILED"
    assert "binding_ref" not in rec


async def test_cancelled_records_failed():
    c = _client(FakeAuditor(raise_exc=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await c.audit_report()
    assert c.executions[-1]["outcome"] == "FAILED"


# --- pass-through, factory validation, accumulation ----------------------------------------

async def test_reads_pass_through_unbound():
    fake = FakeAuditor()
    c = _client(fake)
    assert await c.verify() == {"valid": True}
    assert await c.inventory() == [{"seq": 1}]
    assert c.executions == []  # reads are not bound
    assert fake.calls == ["verify", "inventory"]


def test_invalid_decision_ref_raises():
    with pytest.raises(KeystoneAuditorError):
        keystone_auditor_client(FakeAuditor(), decision_ref="")


def test_action_type_validation():
    with pytest.raises(KeystoneAuditorError):
        auditor_execution_ref(decision_ref=DREF, action_type="auditor:bogus", scope=HEAD,
                              outcome="COMMITTED", executed_at_ms=1)


def test_binding_ref_validates_anchor():
    with pytest.raises(KeystoneAuditorError):
        auditor_binding_ref(execution_ref="x", archive_kid=KID, head_receipt_hash="zz",
                            entries=1, chain_valid=True)
    with pytest.raises(KeystoneAuditorError):
        auditor_binding_ref(execution_ref="x", archive_kid=KID, head_receipt_hash=HEAD,
                            entries=1, chain_valid="yes")


async def test_on_execution_callback():
    seen = []
    c = _client(FakeAuditor(), on_execution=seen.append)
    await c.audit_report()
    assert len(seen) == 1
    assert seen[0]["outcome"] == "COMMITTED"


async def test_executions_accumulate_and_binding_only_on_committed():
    c = _client(FakeAuditor())
    await c.audit_report()
    await c.attest()
    await c.evidence_pack()
    assert len(c.executions) == 3
    assert all("binding_ref" in r for r in c.executions)
    assert c.execution_ref == c.executions[-1]["execution_ref"]


def test_action_type_constants():
    assert ACTION_TYPES == {"audit_report": "auditor:report", "attest": "auditor:attest",
                            "evidence_pack": "auditor:evidence_pack"}
    assert BIND_ACTION_TYPE == "auditor:bind"
