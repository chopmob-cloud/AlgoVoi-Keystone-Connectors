"""algovoi-keystone-dynamodb -- keystone data-plane connector (Apache-2.0)."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneDynamoDBClient,
    KeystoneDynamoDBError,
    dynamodb_execution_ref,
    keystone_dynamodb_client,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneDynamoDBClient",
    "KeystoneDynamoDBError",
    "dynamodb_execution_ref",
    "keystone_dynamodb_client",
    "tamper_detected",
]
__version__ = "0.1.0"
