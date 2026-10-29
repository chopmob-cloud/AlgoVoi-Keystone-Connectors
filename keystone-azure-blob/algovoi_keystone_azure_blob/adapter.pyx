"""algovoi-keystone-azure-blob -- bind Azure Blob Storage writes to the AlgoVoi keystone.

Keystone-only edition (open, Apache-2.0). The Azure counterpart to the S3 and GCS connectors:
each blob write is bound to the keystone decision that authorised it, producing a
content-addressed ``execution_ref``:

    execution_ref = "sha256:" + SHA-256(JCS({ decision_ref, action_type, scope, outcome,
                                             executed_at_ms }))

where ``action_type`` is ``azureblob:upload`` / ``azureblob:delete`` and ``scope`` is
``container/blob``. ``blob_execution_ref`` is the standalone helper; ``keystone_container_client``
wraps any azure-storage-blob ``ContainerClient`` so ``upload_blob`` / ``delete_blob`` each emit an
``execution_ref``, with every read (download_blob, list_blobs, ...) passing through. No
azure-storage-blob dependency.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from algovoi_execution_ref import EXECUTION_OUTCOMES, execution_ref

__all__ = [
    "KeystoneAzureBlobError",
    "EXECUTION_OUTCOMES",
    "blob_execution_ref",
    "KeystoneContainerClient",
    "keystone_container_client",
    "tamper_detected",
]

# method name -> (action, extractor(args, kwargs) -> blob name)
#   ContainerClient.upload_blob(name, data, ...); delete_blob(blob, ...) -- both positional-first.
_WRITE_METHODS: dict = {
    "upload_blob": ("upload", lambda a, k: k.get("name") or (a[0] if a else None)),
    "delete_blob": ("delete", lambda a, k: k.get("blob") or (a[0] if a else None)),
}


class KeystoneAzureBlobError(ValueError):
    """Raised on invalid binding inputs."""


def blob_execution_ref(*, decision_ref: str, container: str, blob: str, outcome: str,
                       executed_at_ms: int, action: str = "upload") -> str:
    """Bind one Azure blob write to a keystone decision and return its execution_ref."""
    if outcome not in EXECUTION_OUTCOMES:
        raise KeystoneAzureBlobError(f"outcome must be one of {EXECUTION_OUTCOMES}")
    if action not in ("upload", "delete"):
        raise KeystoneAzureBlobError("action must be 'upload' or 'delete'")
    if not (isinstance(container, str) and container):
        raise KeystoneAzureBlobError("container must be a non-empty string")
    if not (isinstance(blob, str) and blob):
        raise KeystoneAzureBlobError("blob must be a non-empty string")
    return execution_ref(decision_ref, f"azureblob:{action}", f"{container}/{blob}", outcome,
                         executed_at_ms)


class KeystoneContainerClient:
    """Wrap an azure-storage-blob ``ContainerClient`` so each write emits a keystone execution_ref.

    Attribute access falls through to the wrapped client, so it is a drop-in. The container name is
    taken from ``container=`` or the client's ``container_name`` attribute. ``execution_ref`` holds
    the last binding; ``executions`` the full list. A write that raises is recorded ``FAILED`` and
    the error re-raised.
    """

    def __init__(self, client: Any, *, decision_ref: str, container: str | None = None,
                 clock_ms: Callable[[], int] | None = None,
                 on_execution: Callable[[dict], None] | None = None) -> None:
        if not (isinstance(decision_ref, str) and decision_ref):
            raise KeystoneAzureBlobError("decision_ref must be a non-empty string")
        self._c = client
        self._decision_ref = decision_ref
        self._container = container or getattr(client, "container_name", "") or ""
        self._clock = clock_ms or (lambda: int(time.time() * 1000))
        self._on = on_execution
        self.executions: list[dict] = []
        self.execution_ref: str | None = None

    def _wrap(self, name: str, action: str, extract: Callable) -> Callable:
        fn = getattr(self._c, name)

        def caller(*args: Any, **kwargs: Any) -> Any:
            blob = extract(args, kwargs)
            ts = self._clock()
            try:
                result = fn(*args, **kwargs)
            except Exception:
                self._record(action, blob, "FAILED", ts)
                raise
            self._record(action, blob, "COMMITTED", ts)
            return result

        return caller

    def _record(self, action: str, blob: Any, outcome: str, ts: int) -> None:
        container = str(self._container or "")
        blob_s = str(blob or "")
        ref = blob_execution_ref(decision_ref=self._decision_ref, container=container,
                                 blob=blob_s, outcome=outcome, executed_at_ms=ts, action=action)
        rec = {"decision_ref": self._decision_ref,
               "scope": f"{container}/{blob_s}",
               "action_type": f"azureblob:{action}", "container": self._container, "blob": blob,
               "outcome": outcome, "executed_at_ms": ts, "execution_ref": ref}
        self.execution_ref = ref
        self.executions.append(rec)
        if self._on:
            self._on(rec)

    def __getattr__(self, name: str) -> Any:
        spec = _WRITE_METHODS.get(name)
        if spec is not None:
            return self._wrap(name, spec[0], spec[1])
        return getattr(self._c, name)


def keystone_container_client(client: Any, *, decision_ref: str, container: str | None = None,
                             clock_ms: Callable[[], int] | None = None,
                             on_execution: Callable[[dict], None] | None = None
                             ) -> KeystoneContainerClient:
    """Wrap an Azure ``ContainerClient`` so each blob write is bound to ``decision_ref``."""
    return KeystoneContainerClient(client, decision_ref=decision_ref, container=container,
                                   clock_ms=clock_ms, on_execution=on_execution)


def tamper_detected(claimed_ref: str, *, decision_ref: str, container: str, blob: str,
                    outcome: str, executed_at_ms: int, action: str = "upload") -> bool:
    """Return True if the write's fields do not recompute to the claimed execution_ref."""
    try:
        return blob_execution_ref(decision_ref=decision_ref, container=container, blob=blob,
                                  outcome=outcome, executed_at_ms=executed_at_ms,
                                  action=action) != claimed_ref
    except Exception:
        return True
