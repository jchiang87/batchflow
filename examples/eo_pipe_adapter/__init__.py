"""
eo_pipe_adapter — thin bridge from eo_pipe legacy config to batchflow.
"""
from .adapter import (
    load_eo_workflow,
    check_env_vars,
    register_lsst_patterns,
    LSST_TRANSIENT_PATTERNS,
    LSST_FATAL_PATTERNS,
    REQUIRED_ENV_VARS,
)

__all__ = [
    "load_eo_workflow",
    "check_env_vars",
    "register_lsst_patterns",
    "LSST_TRANSIENT_PATTERNS",
    "LSST_FATAL_PATTERNS",
    "REQUIRED_ENV_VARS",
]
