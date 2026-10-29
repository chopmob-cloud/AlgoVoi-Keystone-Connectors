"""Tests for algovoi-keystone-pinecone (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_pinecone import (
    EXECUTION_OUTCOMES, KeystonePineconeError, keystone_index, pinecone_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000


class FakeIndex:
    name = "agent-memory"

    def __init__(self):
        self.writes = []

    def upsert(self, vectors=None, namespace="", **k):
        if namespace == "boom":
            raise RuntimeError("quota exceeded")
        self.writes.append(("upsert", namespace))
        return {"upserted_count": len(vectors or [])}

    def delete(self, ids=None, namespace="", **k):
        self.writes.append(("delete", namespace))
        return {}

    def query(self, **k):
        return {"matches": []}   # read path, must pass through unbound


def test_pinecone_execution_ref_matches_primitive():
    ref = pinecone_execution_ref(decision_ref=DR, index="agent-memory", namespace="tenant-1", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "pinecone:upsert", "agent-memory/tenant-1", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = pinecone_execution_ref(decision_ref=DR, index="agent-memory", namespace="t1", outcome="COMMITTED", executed_at_ms=TS)
    assert base != pinecone_execution_ref(decision_ref=DR, index="agent-memory", namespace="t2", outcome="COMMITTED", executed_at_ms=TS)
    assert base != pinecone_execution_ref(decision_ref=DR, index="other", namespace="t1", outcome="COMMITTED", executed_at_ms=TS)
    assert base != pinecone_execution_ref(decision_ref=DR, index="agent-memory", namespace="t1", outcome="COMMITTED", executed_at_ms=TS, action="delete")


def test_rejects_bad():
    with pytest.raises(KeystonePineconeError):
        pinecone_execution_ref(decision_ref=DR, index="i", namespace="n", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystonePineconeError):
        pinecone_execution_ref(decision_ref=DR, index="", namespace="n", outcome="COMMITTED", executed_at_ms=TS)
    with pytest.raises(KeystonePineconeError):
        pinecone_execution_ref(decision_ref=DR, index="i", namespace="n", outcome="COMMITTED", executed_at_ms=TS, action="query")


def test_upsert_binds():
    idx = keystone_index(FakeIndex(), decision_ref=DR, clock_ms=lambda: TS)
    idx.upsert(vectors=[{"id": "1", "values": [0.1]}], namespace="tenant-1")
    assert idx.execution_ref == execution_ref(DR, "pinecone:upsert", "agent-memory/tenant-1", "COMMITTED", TS)
    assert idx.executions[0]["outcome"] == "COMMITTED"


def test_delete_binds():
    idx = keystone_index(FakeIndex(), decision_ref=DR, clock_ms=lambda: TS)
    idx.delete(ids=["1"], namespace="tenant-1")
    assert idx.execution_ref == execution_ref(DR, "pinecone:delete", "agent-memory/tenant-1", "COMMITTED", TS)


def test_default_namespace():
    idx = keystone_index(FakeIndex(), decision_ref=DR, clock_ms=lambda: TS)
    idx.upsert(vectors=[])
    assert idx.execution_ref == execution_ref(DR, "pinecone:upsert", "agent-memory/", "COMMITTED", TS)


def test_index_name_override():
    idx = keystone_index(FakeIndex(), decision_ref=DR, index_name="explicit-idx", clock_ms=lambda: TS)
    idx.upsert(vectors=[], namespace="n")
    assert idx.execution_ref == execution_ref(DR, "pinecone:upsert", "explicit-idx/n", "COMMITTED", TS)


def test_failed_upsert_records_failed_and_reraises():
    idx = keystone_index(FakeIndex(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        idx.upsert(vectors=[], namespace="boom")
    assert idx.executions[-1]["outcome"] == "FAILED"
    assert idx.execution_ref == execution_ref(DR, "pinecone:upsert", "agent-memory/boom", "FAILED", TS)


def test_query_passthrough_unbound():
    idx = keystone_index(FakeIndex(), decision_ref=DR, clock_ms=lambda: TS)
    assert idx.query(vector=[0.1]) == {"matches": []}
    assert idx.executions == []


def test_rejects_bad_decision():
    with pytest.raises(KeystonePineconeError):
        keystone_index(FakeIndex(), decision_ref="")


def test_on_execution_callback():
    seen = []
    idx = keystone_index(FakeIndex(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    idx.upsert(vectors=[], namespace="n")
    assert seen[0]["execution_ref"] == idx.execution_ref


def test_tamper_detected():
    ref = pinecone_execution_ref(decision_ref=DR, index="agent-memory", namespace="t1", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, index="agent-memory", namespace="t1", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, index="agent-memory", namespace="t2", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, index="agent-memory", namespace="t1", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
