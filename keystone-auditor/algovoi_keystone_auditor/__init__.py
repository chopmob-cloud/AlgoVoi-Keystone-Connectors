"""algovoi-keystone-auditor -- bind archive audit reports to the AlgoVoi keystone."""
from .adapter import (
    ACTION_TYPES,
    BIND_ACTION_TYPE,
    EXECUTION_OUTCOMES,
    KeystoneAuditorClient,
    KeystoneAuditorError,
    auditor_binding_ref,
    auditor_execution_ref,
    keystone_auditor_client,
    tamper_detected,
    verify_binding,
)

__all__ = [
    "ACTION_TYPES",
    "BIND_ACTION_TYPE",
    "EXECUTION_OUTCOMES",
    "KeystoneAuditorClient",
    "KeystoneAuditorError",
    "auditor_binding_ref",
    "auditor_execution_ref",
    "keystone_auditor_client",
    "tamper_detected",
    "verify_binding",
]
__version__ = "0.1.0"
