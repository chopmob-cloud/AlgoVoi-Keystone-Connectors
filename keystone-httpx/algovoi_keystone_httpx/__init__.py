"""algovoi-keystone-httpx -- bind outbound HTTP calls to the AlgoVoi keystone (httpx / requests)."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneHTTPClient,
    KeystoneHTTPError,
    WRITE_METHODS,
    keystone_http_client,
    outcome_for_status,
    request_execution_ref,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneHTTPClient",
    "KeystoneHTTPError",
    "WRITE_METHODS",
    "keystone_http_client",
    "outcome_for_status",
    "request_execution_ref",
    "tamper_detected",
]
__version__ = "0.5.0"
