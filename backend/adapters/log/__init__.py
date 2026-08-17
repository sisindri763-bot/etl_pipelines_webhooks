"""
adapters/log/__init__.py
-------------------------
Registry: maps tool type strings -> log adapter instances.
"""

from adapters.log.dbt import DbtCloudLogAdapter

LOG_ADAPTERS = {
    "dbt": DbtCloudLogAdapter(),
}

__all__ = ["LOG_ADAPTERS"]
