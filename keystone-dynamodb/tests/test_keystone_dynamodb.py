"""Tests for algovoi-keystone-dynamodb (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_dynamodb import (
    EXECUTION_OUTCOMES, KeystoneDynamoDBError, dynamodb_execution_ref,
    keystone_dynamodb_client, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000


class FakeDynamo:
    def __init__(self):
        self.writes = []

    def put_item(self, **k):
        if (k.get("Item") or {}).get("boom"):
            raise RuntimeError("conditional check failed")
        self.writes.append(("put", k))
        return {}

    def update_item(self, **k):
        self.writes.append(("update", k))
        return {}

    def delete_item(self, **k):
        self.writes.append(("delete", k))
        return {}

    def batch_write_item(self, **k):
        self.writes.append(("batch", k))
        return {"UnprocessedItems": {}}

    def get_item(self, **k):
        return {"Item": {}}   # read path, must pass through unbound


def test_dynamodb_execution_ref_matches_primitive():
    ref = dynamodb_execution_ref(decision_ref=DR, table="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "dynamodb:put", "orders", "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_fields_load_bearing():
    base = dynamodb_execution_ref(decision_ref=DR, table="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert base != dynamodb_execution_ref(decision_ref=DR, table="refunds", outcome="COMMITTED", executed_at_ms=TS)
    assert base != dynamodb_execution_ref(decision_ref=DR, table="orders", outcome="COMMITTED", executed_at_ms=TS, action="delete")
    assert base != dynamodb_execution_ref(decision_ref=DR, table="orders", outcome="FAILED", executed_at_ms=TS)


def test_rejects_bad():
    with pytest.raises(KeystoneDynamoDBError):
        dynamodb_execution_ref(decision_ref=DR, table="t", outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneDynamoDBError):
        dynamodb_execution_ref(decision_ref=DR, table="", outcome="COMMITTED", executed_at_ms=TS)
    with pytest.raises(KeystoneDynamoDBError):
        dynamodb_execution_ref(decision_ref=DR, table="t", outcome="COMMITTED", executed_at_ms=TS, action="scan")


def test_put_item_binds():
    c = keystone_dynamodb_client(FakeDynamo(), decision_ref=DR, clock_ms=lambda: TS)
    c.put_item(TableName="orders", Item={"id": {"S": "1"}})
    assert c.execution_ref == execution_ref(DR, "dynamodb:put", "orders", "COMMITTED", TS)
    assert c.executions[0]["outcome"] == "COMMITTED"


def test_delete_item_binds():
    c = keystone_dynamodb_client(FakeDynamo(), decision_ref=DR, clock_ms=lambda: TS)
    c.delete_item(TableName="orders", Key={"id": {"S": "1"}})
    assert c.execution_ref == execution_ref(DR, "dynamodb:delete", "orders", "COMMITTED", TS)


def test_batch_write_binds_joined_tables():
    c = keystone_dynamodb_client(FakeDynamo(), decision_ref=DR, clock_ms=lambda: TS)
    c.batch_write_item(RequestItems={"orders": [], "audit": []})
    assert c.execution_ref == execution_ref(DR, "dynamodb:batch_write", "audit,orders", "COMMITTED", TS)


def test_failed_put_records_failed_and_reraises():
    c = keystone_dynamodb_client(FakeDynamo(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        c.put_item(TableName="orders", Item={"boom": True})
    assert c.executions[-1]["outcome"] == "FAILED"
    assert c.execution_ref == execution_ref(DR, "dynamodb:put", "orders", "FAILED", TS)


def test_read_passthrough_unbound():
    c = keystone_dynamodb_client(FakeDynamo(), decision_ref=DR, clock_ms=lambda: TS)
    assert c.get_item(TableName="orders", Key={"id": {"S": "1"}}) == {"Item": {}}
    assert c.executions == []


def test_rejects_bad_decision():
    with pytest.raises(KeystoneDynamoDBError):
        keystone_dynamodb_client(FakeDynamo(), decision_ref="")


def test_on_execution_callback():
    seen = []
    c = keystone_dynamodb_client(FakeDynamo(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    c.put_item(TableName="orders", Item={"id": {"S": "1"}})
    assert seen[0]["execution_ref"] == c.execution_ref


def test_tamper_detected():
    ref = dynamodb_execution_ref(decision_ref=DR, table="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, table="orders", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, table="refunds", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, table="orders", outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
