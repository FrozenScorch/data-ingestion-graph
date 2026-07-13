"""
Node registry: auto-discovery and registration of node types.
"""

import importlib
import pkgutil
from typing import Any, Optional

from app.nodes.base import BaseNode, PortDataType

# Global node registry
_registry: dict[str, BaseNode] = {}


def register_node(node: BaseNode) -> None:
    """Register a node instance in the global registry."""
    if node.node_type in _registry:
        raise ValueError(f"Node type '{node.node_type}' is already registered")
    _registry[node.node_type] = node


def get_node(node_type: str) -> Optional[BaseNode]:
    """Get a registered node by type."""
    return _registry.get(node_type)


def get_all_nodes() -> dict[str, BaseNode]:
    """Get all registered nodes."""
    return dict(_registry)


def get_nodes_by_category(category: str) -> list[BaseNode]:
    """Get all nodes in a given category."""
    return [n for n in _registry.values() if n.category == category]


def discover_nodes() -> None:
    """
    Auto-discover and register all node modules in app.nodes package.
    Modules with a 'register' function will be called to register their nodes.
    """
    import app.nodes as nodes_pkg

    for importer, modname, ispkg in pkgutil.iter_modules(nodes_pkg.__path__):
        if modname in ("base", "registry"):
            continue
        try:
            module = importlib.import_module(f"app.nodes.{modname}")
            # If module has a register function, call it
            if hasattr(module, "register"):
                module.register()
        except Exception as e:
            # Log but don't crash on import errors
            import logging

            logging.getLogger(__name__).warning(f"Failed to import node module {modname}: {e}")
    validate_registry_contracts()


def validate_registry_contracts() -> None:
    """Materialize every node contract so schema drift fails during startup."""
    for node_type, node in _registry.items():
        try:
            definition = node.to_dict()
        except Exception as exc:
            raise ValueError(
                f"Node type {node_type!r} has an invalid registry contract: {exc}"
            ) from exc
        if definition.get("type") != node_type:
            raise ValueError(f"Node type {node_type!r} serialized with a different type")


def get_registry_summary() -> list[dict[str, Any]]:
    """Get a summary of all registered nodes for the API."""
    nodes = []
    for node in _registry.values():
        nodes.append(node.to_dict())
    return nodes
