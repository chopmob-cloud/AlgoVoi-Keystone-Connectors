"""algovoi-keystone-celery -- bind Celery task dispatch to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). When a keystone decision authorises asynchronous work,
this binds the dispatch of the Celery task to that decision, producing a content-addressed
``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``celery:dispatch`` and ``scope`` is the task name.
``task_execution_ref`` is the standalone helper; ``keystone_task`` wraps a single task so
``apply_async`` / ``delay`` are bound, and ``keystone_celery_app`` wraps an app so ``send_task`` is
bound. A dispatch that raises is recorded ``FAILED``; otherwise ``COMMITTED``. No Celery dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneCeleryError",
    "EXECUTION_OUTCOMES",
    "task_execution_ref",
    "KeystoneTask",
    "KeystoneCeleryApp",
    "keystone_task",
    "keystone_celery_app",
    "tamper_detected",
]


class KeystoneCeleryError(ValueError):
    """Raised on invalid binding inputs."""


def task_execution_ref(*, decision_ref: str, task: str, outcome: str,
                       executed_at_ms: int) -> str:
    """Bind one task dispatch to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneCeleryError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if not (isinstance(task, str) and task):
        raise KeystoneCeleryError("task must be a non-empty string")
    return execution_ref(decision_ref, "celery:dispatch", task, outcome, executed_at_ms)


class _Dispatcher:
    def __init__(self, decision_ref: str, clock_ms: Callable[[], int] | None,
                 on_execution: Callable[[dict], None] | None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneCeleryError("decision_ref must be a non-empty string")
        self._decision_ref = decision_ref
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _run(self, fn: Callable, task: str, args: tuple, kwargs: dict) -> Any:
        ts = self._clock()
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self._record(task, "FAILED", ts)
            raise
        self._record(task, "COMMITTED", ts)
        return result

    def _record(self, task: str, outcome: str, ts: int) -> None:
        task_s = str(task or "")
        ref = task_execution_ref(decision_ref=self._decision_ref, task=task_s,
                                 outcome=outcome, executed_at_ms=ts)
        rec = {"decision_ref": self._decision_ref, "scope": task_s,
               "task": task_s, "action_type": "celery:dispatch",
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(rec)


class KeystoneTask(_Dispatcher):
    """Wrap a single Celery task so each ``apply_async`` / ``delay`` emits a keystone execution_ref.

    Attribute access falls through to the wrapped task, so it is a drop-in. The scope is the task's
    ``name`` attribute.
    """

    def __init__(self, task: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        super().__init__(decision_ref, clock_ms, on_execution)
        self._t = task
        self._name = getattr(task, "name", None) or str(task)

    def __getattr__(self, name: str) -> Any:
        if name in ("apply_async", "delay"):
            fn = getattr(self._t, name)
            return lambda *a, **k: self._run(fn, self._name, a, k)
        return getattr(self._t, name)


class KeystoneCeleryApp(_Dispatcher):
    """Wrap a Celery app so each ``send_task(name, ...)`` emits a keystone execution_ref.

    Attribute access falls through to the wrapped app, so it is a drop-in. The scope is the task
    name passed to ``send_task``.
    """

    def __init__(self, app: Any, *, decision_ref: str,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        super().__init__(decision_ref, clock_ms, on_execution)
        self._app = app

    def __getattr__(self, name: str) -> Any:
        if name == "send_task":
            fn = getattr(self._app, "send_task")

            def caller(*args: Any, **kwargs: Any) -> Any:
                task = (args[0] if args else kwargs.get("name")) or ""
                return self._run(fn, str(task), args, kwargs)

            return caller
        return getattr(self._app, name)


def keystone_task(task: Any, *, decision_ref: str, clock_ms: Callable[[], int] | None = None,
                  on_execution: Callable[[dict], None] | None = None) -> KeystoneTask:
    """Wrap a Celery task so each dispatch is bound to ``decision_ref``."""
    return KeystoneTask(task, decision_ref=decision_ref, clock_ms=clock_ms,
                        on_execution=on_execution)


def keystone_celery_app(app: Any, *, decision_ref: str, clock_ms: Callable[[], int] | None = None,
                        on_execution: Callable[[dict], None] | None = None) -> KeystoneCeleryApp:
    """Wrap a Celery app so each ``send_task`` is bound to ``decision_ref``."""
    return KeystoneCeleryApp(app, decision_ref=decision_ref, clock_ms=clock_ms,
                             on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, task: str, outcome: str,
                    executed_at_ms: int) -> bool:
    """Return True if the dispatch's fields do not recompute to the claimed execution_ref."""
    try:
        return task_execution_ref(decision_ref=decision_ref, task=task, outcome=outcome,
                                  executed_at_ms=executed_at_ms) != claimed_ref
    except Exception:
        return True
