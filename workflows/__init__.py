from .base import AdapterRegistry, WorkflowAdapter
from .minimax_h3 import MiniMaxH3Adapter
from .minimax_h3_turbo_480p import MiniMaxH3Turbo480pAdapter


def create_registry(conditioning_node: str = "MiniMaxH3ImageToVideo") -> AdapterRegistry:
    registry = AdapterRegistry()
    registry.register(MiniMaxH3Adapter(conditioning_node=conditioning_node))
    registry.register(MiniMaxH3Turbo480pAdapter(conditioning_node=conditioning_node))
    return registry


__all__ = [
    "AdapterRegistry",
    "WorkflowAdapter",
    "MiniMaxH3Adapter",
    "MiniMaxH3Turbo480pAdapter",
    "create_registry",
]
