"""algovoi-keystone-qdrant -- keystone data-plane connector (Apache-2.0)."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneQdrantClient,
    KeystoneQdrantError,
    keystone_qdrant_client,
    qdrant_execution_ref,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneQdrantClient",
    "KeystoneQdrantError",
    "keystone_qdrant_client",
    "qdrant_execution_ref",
    "tamper_detected",
]
__version__ = "0.5.0"
