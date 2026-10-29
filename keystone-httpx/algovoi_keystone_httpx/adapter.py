"""algovoi-keystone-httpx -- bind outbound HTTP calls to the AlgoVoi keystone (httpx / requests).

Keystone-only edition (open, Apache-2.0). The ``asgi`` connector binds *inbound* requests a service
receives; this one binds the *outbound* calls a service makes. When a keystone decision authorises
an external call (a partner push, a webhook, a settlement), the response is bound to that decision,
producing a content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``http:<method>`` (lowercased) and ``scope`` is the destination
``host/path`` (query string dropped for stability). ``request_execution_ref`` is the standalone
helper; ``keystone_http_client`` wraps any sync ``httpx.Client`` or ``requests.Session`` so each
state-changing call (POST / PUT / PATCH / DELETE) is bound, with reads (GET / HEAD / OPTIONS)
passing through unbound. Outcome is derived from the response status: 2xx/3xx COMMITTED, 4xx
SKIPPED, 5xx / exception FAILED. No httpx or requests dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable
from urllib.parse import urlsplit

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneHTTPError",
    "EXECUTION_OUTCOMES",
    "WRITE_METHODS",
    "request_execution_ref",
    "outcome_for_status",
    "KeystoneHTTPClient",
    "keystone_http_client",
    "tamper_detected",
]

WRITE_METHODS = ("POST", "PUT", "PATCH", "DELETE")


class KeystoneHTTPError(ValueError):
    """Raised on invalid binding inputs."""


def _scope(url: Any) -> str:
    """Normalise a request URL to a stable scope: ``host/path`` (query and fragment dropped)."""
    s = str(url)
    parts = urlsplit(s)
    if parts.netloc:
        return parts.netloc + (parts.path or "/")
    return parts.path or s


def outcome_for_status(status_code: int) -> str:
    """Map an HTTP response status to a keystone outcome (0 / no response -> FAILED)."""
    if 200 <= status_code < 400:
        return "COMMITTED"
    if 400 <= status_code < 500:
        return "SKIPPED"
    return "FAILED"


def request_execution_ref(*, decision_ref: str, method: str, url: Any, outcome: str,
                          executed_at_ms: int) -> str:
    """Bind one outbound HTTP call to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneHTTPError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not (isinstance(method, str) and method):
        raise KeystoneHTTPError("method must be a non-empty string")
    scope = _scope(url)
    if not scope:
        raise KeystoneHTTPError("url must yield a non-empty scope")
    return execution_ref(decision_ref, f"http:{method.lower()}", scope, outcome, executed_at_ms)


class KeystoneHTTPClient:
    """Wrap a sync ``httpx.Client`` / ``requests.Session`` so each write call emits an execution_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. ``execution_ref``
    holds the last binding; ``executions`` the full list. A call that raises is recorded ``FAILED``
    and the error re-raised; otherwise the outcome is derived from the response status. ``request``
    is bound only when its method is state-changing; ``get`` / ``head`` / ``options`` pass through.
    """

    def __init__(self, client: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneHTTPError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _run(self, fn: Callable, method: str, url: Any, args: tuple, kwargs: dict) -> Any:
        ts = self._clock()
        try:
            resp = fn(*args, **kwargs)
        except Exception:
            self._record(method, url, "FAILED", ts)
            raise
        status = getattr(resp, "status_code", 0) or 0
        self._record(method, url, outcome_for_status(int(status)), ts)
        return resp

    def _wrap_verb(self, method: str) -> Callable:
        fn = getattr(self._c, method.lower())

        def caller(*args: Any, **kwargs: Any) -> Any:
            url = args[0] if args else kwargs.get("url")
            return self._run(fn, method, url, args, kwargs)

        return caller

    def _wrap_request(self) -> Callable:
        fn = getattr(self._c, "request")

        def caller(*args: Any, **kwargs: Any) -> Any:
            method = (args[0] if args else kwargs.get("method")) or ""
            url = (args[1] if len(args) > 1 else kwargs.get("url"))
            if str(method).upper() not in WRITE_METHODS:
                return fn(*args, **kwargs)   # read verb -> pass through unbound
            return self._run(fn, str(method).upper(), url, args, kwargs)

        return caller

    def _record(self, method: str, url: Any, outcome: str, ts: int) -> None:
        scope = _scope(url)
        ref = request_execution_ref(decision_ref=self._decision_ref, method=method, url=url,
                                    outcome=outcome, executed_at_ms=ts)
        rec = {"decision_ref": self._decision_ref, "scope": scope,
               "method": method, "url": str(url), "action_type": f"http:{method.lower()}",
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        if name in ("post", "put", "patch", "delete"):
            return self._wrap_verb(name.upper())
        if name == "request":
            return self._wrap_request()
        return getattr(self._c, name)


def keystone_http_client(client: Any, *, decision_ref: str,
                         clock_ms: Callable[[], int] | None = None,
                         on_execution: Callable[[dict], None] | None = None) -> KeystoneHTTPClient:
    """Wrap a sync HTTP client so each state-changing call is bound to ``decision_ref``."""
    return KeystoneHTTPClient(client, decision_ref=decision_ref, clock_ms=clock_ms,
                              on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, method: str, url: Any, outcome: str,
                    executed_at_ms: int) -> bool:
    """Return True if the call's fields do not recompute to the claimed execution_ref."""
    try:
        return request_execution_ref(decision_ref=decision_ref, method=method, url=url,
                                     outcome=outcome, executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True
