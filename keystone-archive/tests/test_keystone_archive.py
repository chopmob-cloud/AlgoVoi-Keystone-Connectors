"""Contract tests for algovoi-keystone-archive.

The behaviour under test: an archive notarize becomes a decision-bound execution_ref
AND a storage-binding ref that ties that execution to the sealed archive entry
(doc_hash, entry_id, seq). Both must recompute offline; every mutation must be caught.

A fake async archive stands in for algovoi-doc-archive's Archive so the unit contract
needs no PQC dependency. The real-archive integration is a separate gated test.
"""
from __future__ import annotations

import hashlib

import pytest

from algovoi_execution_ref import execution_ref
from algovoi_keystone_connect import synth_ref

from algovoi_keystone_archive import (
    KeystoneArchiveError,
    archive_binding_ref,
    archive_execution_ref,
    keystone_archive_client,
    tamper_detected,
    verify_binding,
)


def _h(b: bytes) -> str:
    return "sha256:" + hashlib.sha256(b).hexdigest()


class FakeArchive:
    """Mimics algovoi_doc_archive.Archive.notarize / retrieve / verify_chain."""

    def __init__(self, *, fail=False, mismatch=False):
        self.fail = fail
        self.mismatch = mismatch
        self.seq = 0
        self.store = {}

    async def notarize(self, *, data=None, doc_hash=None, title="", content_type=""):
        if self.fail:
            raise RuntimeError("archive backend down")
        if data is None and not doc_hash:
            raise ValueError("provide data or doc_hash")
        h = _h(data) if data is not None else doc_hash
        if data is not None and doc_hash and doc_hash != h:
            raise ValueError("doc_hash does not match data")
        self.seq += 1
        self.store[h] = data
        return {"entry_id": f"entry-{self.seq}", "seq": self.seq, "doc_hash": h,
                "stored": data is not None, "enc_alg": "ML-KEM-1024+AES-256-GCM",
                "title": title, "content_type": content_type}

    async def retrieve(self, *, doc_hash):
        return self.store[doc_hash]

    async def verify_chain(self):
        return type("R", (), {"valid": True})()


DECISION = synth_ref("decision")


@pytest.mark.asyncio
async def test_notarize_emits_decision_bound_execution_ref():
    arc = FakeArchive()
    client = keystone_archive_client(arc, decision_ref=DECISION, clock_ms=lambda: 1700000000000)
    data = b"payment receipt bytes"
    result = await client.notarize(data=data, title="receipt")
    assert result["doc_hash"] == _h(data)
    assert len(client.executions) == 1
    rec = client.executions[0]
    assert rec["action_type"] == "archive:notarize"
    assert rec["outcome"] == "COMMITTED"
    assert rec["decision_ref"] == DECISION
    assert rec["scope"] == _h(data)
    assert rec["executed_at_ms"] == 1700000000000
    # the execution_ref recomputes from the public preimage
    assert rec["execution_ref"] == execution_ref(
        DECISION, "archive:notarize", _h(data), "COMMITTED", 1700000000000)
    assert client.execution_ref == rec["execution_ref"]


@pytest.mark.asyncio
async def test_binding_ref_ties_execution_to_archive_entry():
    arc = FakeArchive()
    client = keystone_archive_client(arc, decision_ref=DECISION)
    data = b"sealed contract"
    result = await client.notarize(data=data)
    rec = client.executions[0]
    assert rec["doc_hash"] == result["doc_hash"]
    assert rec["entry_id"] == result["entry_id"]
    assert rec["seq"] == result["seq"]
    expected = archive_binding_ref(execution_ref=rec["execution_ref"],
                                   doc_hash=result["doc_hash"],
                                   entry_id=result["entry_id"], seq=result["seq"])
    assert rec["binding_ref"] == expected
    assert verify_binding(rec["binding_ref"], execution_ref=rec["execution_ref"],
                          doc_hash=result["doc_hash"], entry_id=result["entry_id"],
                          seq=result["seq"]) is True


@pytest.mark.asyncio
async def test_scope_from_doc_hash_only_notarize():
    arc = FakeArchive()
    client = keystone_archive_client(arc, decision_ref=DECISION)
    h = _h(b"already stored elsewhere")
    result = await client.notarize(doc_hash=h)
    assert result["stored"] is False
    rec = client.executions[0]
    assert rec["scope"] == h
    assert rec["outcome"] == "COMMITTED"


@pytest.mark.asyncio
async def test_failed_notarize_records_failed_and_reraises():
    arc = FakeArchive(fail=True)
    client = keystone_archive_client(arc, decision_ref=DECISION)
    with pytest.raises(RuntimeError):
        await client.notarize(data=b"x")
    rec = client.executions[0]
    assert rec["outcome"] == "FAILED"
    assert "binding_ref" not in rec  # no storage binding on a failed write


@pytest.mark.asyncio
async def test_data_doc_hash_mismatch_is_failed():
    arc = FakeArchive()
    client = keystone_archive_client(arc, decision_ref=DECISION)
    with pytest.raises(ValueError):
        await client.notarize(data=b"x", doc_hash=_h(b"different"))
    assert client.executions[0]["outcome"] == "FAILED"


@pytest.mark.asyncio
async def test_reads_pass_through_unbound():
    arc = FakeArchive()
    client = keystone_archive_client(arc, decision_ref=DECISION)
    data = b"read me"
    await client.notarize(data=data)
    got = await client.retrieve(doc_hash=_h(data))
    assert got == data
    assert (await client.verify_chain()).valid is True
    assert len(client.executions) == 1  # reads emitted no binding


def test_tamper_detected_catches_mutations():
    scope = _h(b"content")
    ref = archive_execution_ref(decision_ref=DECISION, scope=scope,
                                outcome="COMMITTED", executed_at_ms=1700000000000)
    assert tamper_detected(ref, decision_ref=DECISION, scope=scope,
                           outcome="COMMITTED", executed_at_ms=1700000000000) is False
    assert tamper_detected(ref, decision_ref=synth_ref("other"), scope=scope,
                           outcome="COMMITTED", executed_at_ms=1700000000000) is True
    assert tamper_detected(ref, decision_ref=DECISION, scope=_h(b"tampered"),
                           outcome="COMMITTED", executed_at_ms=1700000000000) is True
    assert tamper_detected(ref, decision_ref=DECISION, scope=scope,
                           outcome="FAILED", executed_at_ms=1700000000000) is True
    assert tamper_detected(ref, decision_ref=DECISION, scope=scope,
                           outcome="COMMITTED", executed_at_ms=1700000000001) is True


def test_verify_binding_rejects_mutations():
    eref = execution_ref(DECISION, "archive:notarize", _h(b"c"), "COMMITTED", 1)
    good = archive_binding_ref(execution_ref=eref, doc_hash=_h(b"c"), entry_id="e1", seq=1)
    assert verify_binding(good, execution_ref=eref, doc_hash=_h(b"c"), entry_id="e1", seq=1)
    assert not verify_binding(good, execution_ref=eref, doc_hash=_h(b"other"), entry_id="e1", seq=1)
    assert not verify_binding(good, execution_ref=eref, doc_hash=_h(b"c"), entry_id="e2", seq=1)
    assert not verify_binding(good, execution_ref=eref, doc_hash=_h(b"c"), entry_id="e1", seq=2)
    assert not verify_binding(good, execution_ref=synth_ref("other"), doc_hash=_h(b"c"), entry_id="e1", seq=1)


def test_invalid_inputs_raise():
    with pytest.raises(KeystoneArchiveError):
        keystone_archive_client(FakeArchive(), decision_ref="")
    with pytest.raises(KeystoneArchiveError):
        archive_execution_ref(decision_ref=DECISION, scope="", outcome="COMMITTED", executed_at_ms=1)
    with pytest.raises(KeystoneArchiveError):
        archive_execution_ref(decision_ref=DECISION, scope=_h(b"c"), outcome="BOGUS", executed_at_ms=1)


@pytest.mark.asyncio
async def test_on_execution_callback_fires():
    seen = []
    arc = FakeArchive()
    client = keystone_archive_client(arc, decision_ref=DECISION, on_execution=seen.append)
    await client.notarize(data=b"cb")
    assert len(seen) == 1
    assert seen[0]["binding_ref"] == client.executions[0]["binding_ref"]


# --- the archive must attest the EXACT document, not a substituted one -------

class LyingArchive(FakeArchive):
    """Returns a doc_hash for different bytes than it was handed."""

    def __init__(self, fake_hash):
        super().__init__()
        self._fake = fake_hash

    async def notarize(self, *, data=None, doc_hash=None, title=None, content_type=None):
        res = await super().notarize(data=data, doc_hash=doc_hash, title=title, content_type=content_type)
        res["doc_hash"] = self._fake  # substitute a different document's address
        return res


@pytest.mark.asyncio
async def test_substituted_doc_hash_is_failed_and_unbound():
    arc = LyingArchive(_h(b"a completely different document"))
    client = keystone_archive_client(arc, decision_ref=DECISION, clock_ms=lambda: 1000)
    with pytest.raises(KeystoneArchiveError):
        await client.notarize(data=b"the real document")
    rec = client.executions[0]
    assert rec["outcome"] == "FAILED"
    assert "binding_ref" not in rec  # no storage binding for a substituted document


@pytest.mark.asyncio
async def test_missing_doc_hash_is_failed_and_unbound():
    class NoHashArchive(FakeArchive):
        async def notarize(self, **kw):
            res = await super().notarize(**kw)
            del res["doc_hash"]
            return res

    client = keystone_archive_client(NoHashArchive(), decision_ref=DECISION)
    with pytest.raises(KeystoneArchiveError):
        await client.notarize(data=b"doc")
    rec = client.executions[0]
    assert rec["outcome"] == "FAILED"
    assert "binding_ref" not in rec


@pytest.mark.asyncio
async def test_data_and_doc_hash_both_supplied_matched_ok():
    client = keystone_archive_client(FakeArchive(), decision_ref=DECISION)
    res = await client.notarize(data=b"doc", doc_hash=_h(b"doc"))
    rec = client.executions[0]
    assert rec["outcome"] == "COMMITTED"
    assert rec["scope"] == _h(b"doc")
    assert "binding_ref" in rec


@pytest.mark.asyncio
async def test_adapter_guards_mismatch_even_if_archive_does_not():
    class PermissiveArchive(FakeArchive):
        async def notarize(self, *, data=None, doc_hash=None, title="", content_type=""):
            self.seq += 1
            h = _h(data) if data is not None else doc_hash
            self.store[h] = data
            # deliberately does NOT check doc_hash against data, and echoes the supplied doc_hash
            return {"entry_id": f"entry-{self.seq}", "seq": self.seq,
                    "doc_hash": doc_hash or h, "stored": data is not None}

    client = keystone_archive_client(PermissiveArchive(), decision_ref=DECISION)
    with pytest.raises(ValueError):
        await client.notarize(data=b"x", doc_hash=_h(b"different"))
    rec = client.executions[0]
    assert rec["outcome"] == "FAILED"
    assert "binding_ref" not in rec


@pytest.mark.asyncio
async def test_bytearray_is_snapshotted():
    arc = FakeArchive()
    client = keystone_archive_client(arc, decision_ref=DECISION)
    buf = bytearray(b"original bytes")
    res = await client.notarize(data=buf)
    buf[:] = b"mutated after the call"  # a live bytearray must not change what was sealed
    assert await arc.retrieve(doc_hash=res["doc_hash"]) == b"original bytes"
    assert client.executions[0]["scope"] == _h(b"original bytes")


def test_executed_at_ms_rejects_bool():
    with pytest.raises(KeystoneArchiveError):
        archive_execution_ref(decision_ref=DECISION, scope=_h(b"x"), outcome="COMMITTED", executed_at_ms=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, 123, "zz", "sha256:nothex"])
async def test_malformed_archive_doc_hash_records_failed(bad):
    class BadHashArchive(FakeArchive):
        async def notarize(self, **kw):
            res = await super().notarize(**kw)
            res["doc_hash"] = bad
            return res

    client = keystone_archive_client(BadHashArchive(), decision_ref=DECISION)
    with pytest.raises(KeystoneArchiveError):
        await client.notarize(data=b"doc")
    rec = client.executions[0]
    assert rec["outcome"] == "FAILED"  # the write happened, so a FAILED record MUST exist
    assert "binding_ref" not in rec
