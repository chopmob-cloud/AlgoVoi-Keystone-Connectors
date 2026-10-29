"""algovoi-keystone-azure-blob -- bind Azure Blob Storage writes to the AlgoVoi keystone."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneAzureBlobError,
    KeystoneContainerClient,
    blob_execution_ref,
    keystone_container_client,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneAzureBlobError",
    "KeystoneContainerClient",
    "blob_execution_ref",
    "keystone_container_client",
    "tamper_detected",
]
__version__ = "0.5.0"
