"""Live integration against the real algovoi-doc-archive (Falcon-1024 + ML-KEM-1024).

Skipped when the optional archive extra is absent, so the unit contract stays dependency-free.
This proves the duck-typed connector matches the real Archive API and that the commerce->storage
chain verifies offline end to end: notarize -> execution_ref + binding_ref -> archive chain valid
-> decrypt round-trip -> journaled record recomputes.
"""
from __future__ import annotations

import hashlib

import pytest

da = pytest.importorskip("algovoi_doc_archive")

from algovoi_doc_archive import Archive, HybridPqcEncryptor, LocalFsBackend, signing
from algovoi_doc_archive.encryption import generate_mlkem_keypair
from algovoi_keystone_connect import synth_ref
from algovoi_keystone_runtime import Journal, verify_record

from algovoi_keystone_archive import keystone_archive_client, tamper_detected, verify_binding


def _h(b):
    return "sha256:" + hashlib.sha256(b).hexdigest()


@pytest.fixture()
def archive(tmp_path):
    pk, sk = signing.generate_keypair()
    kem_pk, kem_sk = generate_mlkem_keypair()
    return Archive(secret_key=sk, public_key=pk,
                   storage=LocalFsBackend(str(tmp_path / "archive")),
                   encryptor=HybridPqcEncryptor(kem_pk, kem_sk))


@pytest.mark.asyncio
async def test_real_notarize_binding_and_roundtrip(archive, tmp_path):
    decision = synth_ref("decision")
    client = keystone_archive_client(archive, decision_ref=decision)
    data = b"real settlement receipt bytes"
    result = await client.notarize(data=data, title="receipt",
                                   content_type="application/octet-stream")
    rec = client.executions[-1]
    assert rec["outcome"] == "COMMITTED"
    assert rec["action_type"] == "archive:notarize"
    assert rec["doc_hash"] == result["doc_hash"] == _h(data)
    assert rec["scope"] == _h(data)
    # storage binding recomputes against the REAL archive entry identity
    assert verify_binding(rec["binding_ref"], execution_ref=rec["execution_ref"],
                          doc_hash=result["doc_hash"], entry_id=result["entry_id"],
                          seq=result["seq"])
    # the archive's own Falcon-signed chain verifies
    assert (await archive.verify_chain()).valid
    # decrypt round-trip with integrity check
    assert await archive.retrieve(doc_hash=result["doc_hash"]) == data
    # journal the binding record; it recomputes and the chain verifies
    j = Journal(str(tmp_path / "keystone.db"))
    head = j.append(rec)
    assert verify_record(rec)
    assert j.verify(expect_head=head, expect_count=j.count()).ok


@pytest.mark.asyncio
async def test_real_binding_tamper_is_caught(archive):
    decision = synth_ref("decision")
    client = keystone_archive_client(archive, decision_ref=decision)
    result = await client.notarize(data=b"tamper target")
    rec = client.executions[-1]
    assert not verify_binding(rec["binding_ref"], execution_ref=rec["execution_ref"],
                              doc_hash=result["doc_hash"], entry_id=result["entry_id"],
                              seq=result["seq"] + 1)
    assert tamper_detected(rec["execution_ref"], decision_ref=decision, scope=_h(b"other"),
                           outcome="COMMITTED", executed_at_ms=rec["executed_at_ms"])


@pytest.mark.asyncio
async def test_real_mismatch_notarize_is_failed(archive):
    decision = synth_ref("decision")
    client = keystone_archive_client(archive, decision_ref=decision)
    with pytest.raises(ValueError):
        await client.notarize(data=b"x", doc_hash=_h(b"different"))
    rec = client.executions[-1]
    assert rec["outcome"] == "FAILED"
    assert "binding_ref" not in rec
