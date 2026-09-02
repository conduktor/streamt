"""Secret-free primary artifact projections for compiled models."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

from streamt.core.models import MaterializedType

ProcessKind = Literal["flink", "gateway", "connect"]
OutputKind = Literal["kafka", "gateway"]


@dataclass(frozen=True)
class CompiledModelView:
    """One model's secret-free primary process and output projection."""

    model_name: str
    materialized: MaterializedType
    process_kind: ProcessKind | None
    output_kind: OutputKind | None
    output_name: str | None
    gateway_physical_input: str | None
    connector_inputs: tuple[str, ...]

    def __post_init__(self) -> None:
        """Reject structurally inconsistent projections."""
        if not isinstance(self.model_name, str) or not self.model_name.strip():
            raise ValueError("Compiled model name must be non-blank")
        if not isinstance(self.materialized, MaterializedType):
            raise ValueError("Compiled model materialization must be a MaterializedType")
        if self.process_kind not in (None, "flink", "gateway", "connect"):
            raise ValueError("Compiled model process kind is unsupported")
        if self.output_kind not in (None, "kafka", "gateway"):
            raise ValueError("Compiled model output kind is unsupported")
        if not isinstance(self.connector_inputs, tuple) or not all(
            isinstance(topic, str) for topic in self.connector_inputs
        ):
            raise ValueError("Compiled connector inputs must be a tuple of strings")
        if self.output_kind is None:
            if self.output_name is not None:
                raise ValueError("A model without an output kind cannot have an output name")
        elif not isinstance(self.output_name, str) or not self.output_name.strip():
            raise ValueError("A compiled output name must be non-blank")

        expected_shapes: dict[
            MaterializedType,
            tuple[set[ProcessKind | None], OutputKind | None],
        ] = {
            MaterializedType.TOPIC: ({None, "flink"}, "kafka"),
            MaterializedType.FLINK: ({"flink"}, "kafka"),
            MaterializedType.VIRTUAL_TOPIC: ({"gateway"}, "gateway"),
            MaterializedType.SINK: ({"connect"}, None),
        }
        process_kinds, output_kind = expected_shapes[self.materialized]
        if self.process_kind not in process_kinds or self.output_kind != output_kind:
            raise ValueError(
                f"Compiled model '{self.model_name}' has an inconsistent materialization shape"
            )

        if self.process_kind == "gateway":
            if (
                not isinstance(self.gateway_physical_input, str)
                or not self.gateway_physical_input.strip()
            ):
                raise ValueError("A Gateway process requires one physical input")
        elif self.gateway_physical_input is not None:
            raise ValueError("Only a Gateway process can have a physical Gateway input")

        if self.process_kind == "connect":
            if not self.connector_inputs or any(
                not topic.strip() for topic in self.connector_inputs
            ):
                raise ValueError("A Connect process requires non-blank connector inputs")
        elif self.connector_inputs:
            raise ValueError("Only a Connect process can have connector inputs")


CompiledModels = Mapping[str, CompiledModelView]


def empty_compiled_models() -> CompiledModels:
    """Return an immutable empty compiled-model projection."""
    return MappingProxyType({})


def freeze_compiled_models(
    views: Iterable[CompiledModelView],
    *,
    expected_model_names: Iterable[str],
) -> CompiledModels:
    """Validate complete one-to-one coverage and return a sorted read-only mapping."""
    expected = set(expected_model_names)
    compiled: dict[str, CompiledModelView] = {}
    for view in views:
        if view.model_name in compiled:
            raise ValueError(f"Duplicate compiled model projection '{view.model_name}'")
        compiled[view.model_name] = view

    actual = set(compiled)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        details: list[str] = []
        if missing:
            details.append(f"missing {missing}")
        if unexpected:
            details.append(f"unexpected {unexpected}")
        raise ValueError(
            "Compiled model projection does not match resolved models: "
            + "; ".join(details)
        )

    return MappingProxyType(dict(sorted(compiled.items())))
