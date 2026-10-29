"""algovoi-keystone-sns -- keystone data-plane connector (Apache-2.0)."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneSNSClient,
    KeystoneSNSError,
    keystone_sns_client,
    sns_execution_ref,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneSNSClient",
    "KeystoneSNSError",
    "keystone_sns_client",
    "sns_execution_ref",
    "tamper_detected",
]
__version__ = "0.5.0"
