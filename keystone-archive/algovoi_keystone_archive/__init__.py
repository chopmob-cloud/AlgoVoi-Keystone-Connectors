"""algovoi-keystone-archive -- bind post-quantum archive writes to the AlgoVoi keystone."""
from .adapter import (
    ACTION_TYPE,
    BIND_ACTION_TYPE,
    EXECUTION_OUTCOMES,
    KeystoneArchiveClient,
    KeystoneArchiveError,
    archive_binding_ref,
    archive_execution_ref,
    keystone_archive_client,
    tamper_detected,
    verify_binding,
)

__all__ = [
    "ACTION_TYPE",
    "BIND_ACTION_TYPE",
    "EXECUTION_OUTCOMES",
    "KeystoneArchiveClient",
    "KeystoneArchiveError",
    "archive_binding_ref",
    "archive_execution_ref",
    "keystone_archive_client",
    "tamper_detected",
    "verify_binding",
]
__version__ = "0.1.0"
