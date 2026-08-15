from .base import AdapterRegistry, WorkflowAdapter
from .minimax_h3 import MiniMaxH3Adapter


def create_registry(conditioning_node: str = "MiniMaxH3ImageToVideo") -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(MiniMaxH3Adapter(conditioning_node=conditioning_node))
    return registry


__all__ = ["AdapterRegistry", "WorkflowAdapter", "MiniMaxH3Adapter", "create_registry"]
