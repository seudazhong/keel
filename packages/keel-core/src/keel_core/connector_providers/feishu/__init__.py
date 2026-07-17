"""Discoverable Feishu provider facade.

Only this package facade exports a manifest and factory. Underscore-prefixed helper modules remain
provider-local and cannot create duplicate connector registry entries.
"""

from ._provider import FeishuProvider, factory, manifest

__all__ = ["FeishuProvider", "factory", "manifest"]
