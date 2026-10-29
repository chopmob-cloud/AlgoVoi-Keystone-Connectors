"""algovoi-keystone-azure-servicebus -- bind Azure Service Bus sends to the AlgoVoi keystone."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneServiceBusError,
    KeystoneServiceBusSender,
    keystone_sender,
    servicebus_execution_ref,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneServiceBusError",
    "KeystoneServiceBusSender",
    "keystone_sender",
    "servicebus_execution_ref",
    "tamper_detected",
]
__version__ = "0.5.0"
