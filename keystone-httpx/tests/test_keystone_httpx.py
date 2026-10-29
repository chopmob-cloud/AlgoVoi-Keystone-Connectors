"""Tests for algovoi-keystone-httpx (keystone-only edition)."""
import pytest
from algovoi_execution_ref import execution_ref
from algovoi_keystone_httpx import (
    EXECUTION_OUTCOMES, KeystoneHTTPError, keystone_http_client, outcome_for_status,
    request_execution_ref, tamper_detected,
)

DR = "sha256:" + "a" * 64
TS = 1716460800000
URL = "https://api.partner.com/v1/pay?token=secret"
SCOPE = "api.partner.com/v1/pay"


class Resp:
    def __init__(self, status_code):
        self.status_code = status_code


class FakeClient:
    """httpx.Client / requests.Session double. Returns a status set per URL; raises on 'boom'."""
    def __init__(self):
        self.calls = []

    def post(self, url, *a, **k):
        self.calls.append(("post", url))
        if url == "boom":
            raise RuntimeError("connection reset")
        return Resp(201)

    def put(self, url, *a, **k):
        self.calls.append(("put", url))
        return Resp(200)

    def delete(self, url, *a, **k):
        self.calls.append(("delete", url))
        return Resp(404)

    def get(self, url, *a, **k):
        self.calls.append(("get", url))
        return Resp(200)   # read path, must pass through unbound

    def request(self, method, url, *a, **k):
        self.calls.append(("request", method, url))
        return Resp(200)


def test_request_execution_ref_matches_primitive():
    ref = request_execution_ref(decision_ref=DR, method="POST", url=URL, outcome="COMMITTED", executed_at_ms=TS)
    assert ref == execution_ref(DR, "http:post", SCOPE, "COMMITTED", TS)
    assert ref.startswith("sha256:") and len(ref) == 71


def test_scope_drops_query():
    a = request_execution_ref(decision_ref=DR, method="POST", url="https://h/p?x=1", outcome="COMMITTED", executed_at_ms=TS)
    b = request_execution_ref(decision_ref=DR, method="POST", url="https://h/p?x=2", outcome="COMMITTED", executed_at_ms=TS)
    assert a == b   # query string is not part of scope


def test_outcome_for_status():
    assert outcome_for_status(201) == "COMMITTED"
    assert outcome_for_status(302) == "COMMITTED"
    assert outcome_for_status(404) == "SKIPPED"
    assert outcome_for_status(500) == "FAILED"
    assert outcome_for_status(0) == "FAILED"


def test_rejects_bad():
    with pytest.raises(KeystoneHTTPError):
        request_execution_ref(decision_ref=DR, method="POST", url=URL, outcome="DONE", executed_at_ms=TS)
    with pytest.raises(KeystoneHTTPError):
        request_execution_ref(decision_ref=DR, method="", url=URL, outcome="COMMITTED", executed_at_ms=TS)


def test_post_binds_committed():
    c = keystone_http_client(FakeClient(), decision_ref=DR, clock_ms=lambda: TS)
    c.post(URL, json={})
    assert c.execution_ref == execution_ref(DR, "http:post", SCOPE, "COMMITTED", TS)
    assert c.executions[0]["outcome"] == "COMMITTED"


def test_delete_4xx_is_skipped():
    c = keystone_http_client(FakeClient(), decision_ref=DR, clock_ms=lambda: TS)
    c.delete("https://api.partner.com/v1/pay")
    assert c.execution_ref == execution_ref(DR, "http:delete", SCOPE, "SKIPPED", TS)


def test_request_write_binds():
    c = keystone_http_client(FakeClient(), decision_ref=DR, clock_ms=lambda: TS)
    c.request("PUT", "https://api.partner.com/v1/pay")
    assert c.execution_ref == execution_ref(DR, "http:put", SCOPE, "COMMITTED", TS)


def test_request_read_passthrough_unbound():
    c = keystone_http_client(FakeClient(), decision_ref=DR, clock_ms=lambda: TS)
    r = c.request("GET", "https://api.partner.com/v1/pay")
    assert r.status_code == 200
    assert c.executions == []   # GET via request() is not bound


def test_get_passthrough_unbound():
    c = keystone_http_client(FakeClient(), decision_ref=DR, clock_ms=lambda: TS)
    c.get("https://api.partner.com/v1/pay")
    assert c.executions == []   # reads are not bound


def test_failed_post_records_failed_and_reraises():
    c = keystone_http_client(FakeClient(), decision_ref=DR, clock_ms=lambda: TS)
    with pytest.raises(RuntimeError):
        c.post("boom")
    assert c.executions[-1]["outcome"] == "FAILED"
    assert c.execution_ref == execution_ref(DR, "http:post", "boom", "FAILED", TS)


def test_rejects_bad_decision():
    with pytest.raises(KeystoneHTTPError):
        keystone_http_client(FakeClient(), decision_ref="")


def test_on_execution_callback():
    seen = []
    c = keystone_http_client(FakeClient(), decision_ref=DR, clock_ms=lambda: TS, on_execution=seen.append)
    c.post(URL)
    assert seen[0]["execution_ref"] == c.execution_ref


def test_tamper_detected():
    ref = request_execution_ref(decision_ref=DR, method="POST", url=URL, outcome="COMMITTED", executed_at_ms=TS)
    assert not tamper_detected(ref, decision_ref=DR, method="POST", url=URL, outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected(ref, decision_ref=DR, method="POST", url="https://api.partner.com/v1/refund", outcome="COMMITTED", executed_at_ms=TS)
    assert tamper_detected("not-a-ref", decision_ref=DR, method="POST", url=URL, outcome="COMMITTED", executed_at_ms=TS)


def test_outcomes_constant():
    assert EXECUTION_OUTCOMES == ("COMMITTED", "SKIPPED", "FAILED", "REVERSED")
