"""Tests for algovoi-keystone-qdrant (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_qdrant import (
    EXECUTION_OUTCOMES, KeystoneQdrantError, keystone_qdrant_client, qdrant_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000


class FakeQdrant:
    def __init__(self):
        self.writes = []

    def upsert(self, collection_name=None, points=None, **k):
        if collection_name == "boom":
            raise RuntimeError("collection not found")
        self.writes.append(("upsert", collection_name))
        return {"status": "completed"}

    def delete(self, collection_name=None, **k):
        self.writes.append(("delete", collection_name))
        return {"status": "completed"}

    def set_payload(self, collection_name=None, **k):
        self.writes.append(("set_payload", collection_name))
        return {"status": "completed"}

    def search(self, **k):
        return []   # read path, must pass through unbound


def test_qdrant_execution_ref_matches_primitive():
    ref = qdrant_execution_ref(decision_ref=DR, collection="agent-memory", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "qdrant:upsert", "agent-memory", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = qdrant_execution_ref(decision_ref=DR, collection="agent-memory", outcome="COMMITTED", executed_at_ms=TS)
    assert base != qdrant_execution_ref(decision_ref=DR, collection="other", outcome="COMMITTED", executed_at_ms=TS)
    assert base != qdrant_execution_ref(decision_ref=DR, collection="agent-memory", outcome="COMMITTED", executed_at_ms=TS, action="delete")
    assert base != qdrant_execution_ref(decision_ref=DR, collection="agent-memory", outcome="FAILED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystoneQdrantError):
        qdrant_execution_ref(decision_ref=DR, collection="c", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneQdrantError):
        qdrant_execution_ref(decision_ref=DR, collection="", outcome="COMMITTED", executed_at_ms=TS)
    with pytest.raises(KeystoneQdrantError):
        qdrant_execution_ref(decision_ref=DR, collection="c", outcome="COMMITTED", executed_at_ms=TS, action="search")


def test_upsert_binds():
    c = keystone_qdrant_client(FakeQdrant(), decision_ref=DR, clock_ms=lambda: TS)
    c.upsert(collection_name="agent-memory", points=[{"id": 1}])
    assert c.execution_ref == execution_ref(DR, "qdrant:upsert", "agent-memory", "COMMITTED", TS)
    assert c.executions[0]["outcome"] == "COMMITTED"


def test_set_payload_binds():
    c = keystone_qdrant_client(FakeQdrant(), decision_ref=DR, clock_ms=lambda: TS)
    c.set_payload(collection_name="agent-memory", payload={"k": "v"})
    assert c.execution_ref == execution_ref(DR, "qdrant:set_payload", "agent-memory", "COMMITTED", TS)


def test_delete_binds():
    c = keystone_qdrant_client(FakeQdrant(), decision_ref=DR, clock_ms=lambda: TS)
    c.delete(collection_name="agent-memory")
    assert c.execution_ref == execution_ref(DR, "qdrant:delete", "agent-memory", "COMMITTED", TS)


def test_failed_upsert_records_failed_and_reraises():
    c = keystone_qdrant_client(FakeQdrant(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        c.upsert(collection_name="boom", points=[])
    assert c.executions[-1]["outcome"] == "FAILED"
    assert c.execution_ref == execution_ref(DR, "qdrant:upsert", "boom", "FAILED", TS)


def test_search_passthrough_unbound():
    c = keystone_qdrant_client(FakeQdrant(), decision_ref=DR, clock_ms=lambda: TS)
    assert c.search(collection_name="agent-memory", query_vector=[0.1]) == []
    assert c.executions == []


def test_rejects_bad_decision():
    with pytest.raises(KeystoneQdrantError):
        keystone_qdrant_client(FakeQdrant(), decision_ref="")


def test_on_execution_callback():
    seen = []
    c = keystone_qdrant_client(FakeQdrant(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    c.upsert(collection_name="agent-memory", points=[])
    assert seen[0]["execution_ref"] == c.execution_ref


def test_tamper_detected():
    ref = qdrant_execution_ref(decision_ref=DR, collection="agent-memory", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, collection="agent-memory", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, collection="other", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, collection="agent-memory", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
