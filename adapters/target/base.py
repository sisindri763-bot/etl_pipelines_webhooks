"""
adapters/target/base.py
-----------------------
Same abstract base as adapters/source/base.py — re-exported here
so target adapters can import from their own package.
"""

from adapters.source.base import DataAdapter  # noqa: F401  (re-export)

__all__ = ["DataAdapter"]
