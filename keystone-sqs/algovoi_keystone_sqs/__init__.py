"""algovoi-keystone-sqs -- keystone data-plane connector (Apache-2.0)."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneSQSClient,
    KeystoneSQSError,
    keystone_sqs_client,
    sqs_execution_ref,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneSQSClient",
    "KeystoneSQSError",
    "keystone_sqs_client",
    "sqs_execution_ref",
    "tamper_detected",
]
__version__ = "0.5.0"
