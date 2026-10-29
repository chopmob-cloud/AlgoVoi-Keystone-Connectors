"""algovoi-keystone-celery -- bind Celery task dispatch to the AlgoVoi keystone."""
from .adapter import (
    EXECUTION_OUTCOMES,
    KeystoneCeleryApp,
    KeystoneCeleryError,
    KeystoneTask,
    keystone_celery_app,
    keystone_task,
    task_execution_ref,
    tamper_detected,
)

__all__ = [
    "EXECUTION_OUTCOMES",
    "KeystoneCeleryApp",
    "KeystoneCeleryError",
    "KeystoneTask",
    "keystone_celery_app",
    "keystone_task",
    "task_execution_ref",
    "tamper_detected",
]
__version__ = "0.1.0"
