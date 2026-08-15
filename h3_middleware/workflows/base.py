from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class WorkflowAdapter(ABC):
    name: str

    @abstractmethod
    def normalize(self, raw: dict[str, Any], assets: dict[str, list[dict[str, Any]]], force_mode: str | None = None) -> dict[str, Any]:
        """Validate public inputs and return resolved generation parameters."""

    @abstractmethod
    def build(
        self,
        params: dict[str, Any],
        uploaded_assets: dict[str, list[str]],
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a ComfyUI API-format prompt."""

    @abstractmethod
    def required_nodes(
        self,
        mode: str,
        options: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        assets: dict[str, list[dict[str, Any]]] | None = None,
    ) -> set[str]:
        """Return node class names required by one mode."""

    @abstractmethod
    def schema(self) -> dict[str, Any]:
        """Return the adapter's public mode and parameter schema."""


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, WorkflowAdapter] = {}

    def register(self, adapter: WorkflowAdapter) -> None:
        if adapter.name in self._adapters:
            raise ValueError(f"workflow adapter already registered: {adapter.name}")
        self._adapters[adapter.name] = adapter

    def get(self, name: str) -> WorkflowAdapter:
        try:
            return self._adapters[name]
        except KeyError as exc:
            raise ValueError(f"unknown workflow adapter: {name}") from exc

    def list(self) -> list[WorkflowAdapter]:
        return list(self._adapters.values())
