"""Tests for algovoi-keystone-azure-blob (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_azure_blob import (
    EXECUTION_OUTCOMES, KeystoneAzureBlobError, blob_execution_ref,
    keystone_container_client, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000


class FakeContainer:
    """azure-storage-blob ContainerClient double; raises if blob == 'boom'."""
    def __init__(self, container_name="receipts"):
        self.container_name = container_name
        self.calls = []

    def upload_blob(self, name, data=None, **k):
        if name == "boom":
            raise RuntimeError("auth failed")
        self.calls.append(("upload_blob", name))
        return {"etag": "x"}

    def delete_blob(self, blob, **k):
        self.calls.append(("delete_blob", blob))
        return None

    def download_blob(self, blob, **k):
        return b"data"   # read path, must pass through unbound


def test_blob_execution_ref_matches_primitive():
    ref = blob_execution_ref(decision_ref=DR, container="receipts", blob="2026/r1.json", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "azureblob:upload", "receipts/2026/r1.json", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = blob_execution_ref(decision_ref=DR, container="receipts", blob="r1", outcome="COMMITTED", executed_at_ms=TS)
    assert base != blob_execution_ref(decision_ref=DR, container="receipts", blob="r2", outcome="COMMITTED", executed_at_ms=TS)
    assert base != blob_execution_ref(decision_ref=DR, container="other", blob="r1", outcome="COMMITTED", executed_at_ms=TS)
    assert base != blob_execution_ref(decision_ref=DR, container="receipts", blob="r1", outcome="COMMITTED", executed_at_ms=TS, action="delete")
    assert base != blob_execution_ref(decision_ref=DR, container="receipts", blob="r1", outcome="FAILED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystoneAzureBlobError):
        blob_execution_ref(decision_ref=DR, container="c", blob="b", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneAzureBlobError):
        blob_execution_ref(decision_ref=DR, container="", blob="b", outcome="COMMITTED", executed_at_ms=TS)
    with pytest.raises(KeystoneAzureBlobError):
        blob_execution_ref(decision_ref=DR, container="c", blob="b", outcome="COMMITTED", executed_at_ms=TS, action="copy")


def test_upload_blob_binds():
    c = keystone_container_client(FakeContainer(), decision_ref=DR, clock_ms=lambda: TS)
    c.upload_blob(name="2026/r1.json", data=b"{}")
    assert c.execution_ref == execution_ref(DR, "azureblob:upload", "receipts/2026/r1.json", "COMMITTED", TS)
    assert c.executions[0]["outcome"] == "COMMITTED"


def test_upload_blob_positional_binds():
    c = keystone_container_client(FakeContainer(), decision_ref=DR, clock_ms=lambda: TS)
    c.upload_blob("r1", b"{}")
    assert c.execution_ref == execution_ref(DR, "azureblob:upload", "receipts/r1", "COMMITTED", TS)


def test_delete_blob_binds():
    c = keystone_container_client(FakeContainer(), decision_ref=DR, clock_ms=lambda: TS)
    c.delete_blob("r1")
    assert c.execution_ref == execution_ref(DR, "azureblob:delete", "receipts/r1", "COMMITTED", TS)


def test_container_override():
    c = keystone_container_client(FakeContainer(container_name=""), decision_ref=DR, container="evidence", clock_ms=lambda: TS)
    c.upload_blob(name="r1", data=b"{}")
    assert c.execution_ref == execution_ref(DR, "azureblob:upload", "evidence/r1", "COMMITTED", TS)


def test_failed_upload_records_failed_and_reraises():
    c = keystone_container_client(FakeContainer(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        c.upload_blob(name="boom", data=b"{}")
    assert c.executions[-1]["outcome"] == "FAILED"
    assert c.execution_ref == execution_ref(DR, "azureblob:upload", "receipts/boom", "FAILED", TS)


def test_read_passthrough_unbound():
    fake = FakeContainer()
    c = keystone_container_client(fake, decision_ref=DR, clock_ms=lambda: TS)
    assert c.download_blob("r1") == b"data"
    assert c.executions == []   # reads are not bound


def test_rejects_bad_decision():
    with pytest.raises(KeystoneAzureBlobError):
        keystone_container_client(FakeContainer(), decision_ref="")


def test_on_execution_callback():
    seen = []
    c = keystone_container_client(FakeContainer(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    c.upload_blob(name="r1", data=b"{}")
    assert seen[0]["execution_ref"] == c.execution_ref


def test_tamper_detected():
    ref = blob_execution_ref(decision_ref=DR, container="receipts", blob="r1", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, container="receipts", blob="r1", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, container="receipts", blob="r2", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, container="receipts", blob="r1", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
