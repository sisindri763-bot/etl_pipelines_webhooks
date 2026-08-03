"""
adapters/__init__.py
---------------------
Top-level adapter registry — imports all three sub-registries.
The app imports from here so it never needs to know about sub-packages.

Usage:
    from adapters import LOG_ADAPTERS, SOURCE_ADAPTERS, TARGET_ADAPTERS
"""

from adapters.log    import LOG_ADAPTERS     # noqa: F401
from adapters.source import SOURCE_ADAPTERS  # noqa: F401
from adapters.target import TARGET_ADAPTERS  # noqa: F401

__all__ = ["LOG_ADAPTERS", "SOURCE_ADAPTERS", "TARGET_ADAPTERS"]
