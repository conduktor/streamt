"""DAG builder for streamt projects."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from streamt.core.models import Exposure, Model, StreamtProject
from streamt.core.parser import ProjectParser


class NodeType(str, Enum):
    """Type of node in the DAG."""

    SOURCE = "source"
    MODEL = "model"
    EXPOSURE = "exposure"


@dataclass
class DAGNode:
    """A node in the DAG."""

    name: str
    type: NodeType
    materialized: Optional[str] = None  # For models
    upstream: set[str] = field(default_factory=set)
    downstream: set[str] = field(default_factory=set)

    def __hash__(self) -> int:
        return hash((self.name, self.type))


@dataclass
class DAG:
    """Directed Acyclic Graph of the project."""

    nodes: dict[str, DAGNode] = field(default_factory=dict)

    def add_node(self, node: DAGNode) -> None:
        """Add a node to the DAG."""
        self.nodes[node.name] = node

    def add_edge(self, from_node: str, to_node: str) -> None:
        """Add an edge from from_node to to_node."""
        if from_node in self.nodes and to_node in self.nodes:
            self.nodes[from_node].downstream.add(to_node)
            self.nodes[to_node].upstream.add(from_node)

    def get_node(self, name: str) -> Optional[DAGNode]:
        """Get a node by name."""
        return self.nodes.get(name)

    def topological_sort(self) -> list[str]:
        """Return nodes in topological order (dependencies first)."""
        visited: set[str] = set()
        result: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            visited.add(name)
            node = self.nodes.get(name)
            if node:
                for upstream in node.upstream:
                    visit(upstream)
            result.append(name)

        for name in self.nodes:
            visit(name)

        return result

    def get_upstream(self, name: str, recursive: bool = True) -> set[str]:
        """Get upstream nodes (dependencies)."""
        result: set[str] = set()
        node = self.nodes.get(name)
        if not node:
            return result

        for upstream in node.upstream:
            result.add(upstream)
            if recursive:
                result.update(self.get_upstream(upstream, recursive=True))

        return result

    def get_downstream(self, name: str, recursive: bool = True) -> set[str]:
        """Get downstream nodes (dependents)."""
        result: set[str] = set()
        node = self.nodes.get(name)
        if not node:
            return result

        for downstream in node.downstream:
            result.add(downstream)
            if recursive:
                result.update(self.get_downstream(downstream, recursive=True))

        return result

    def get_models_only(self) -> list[str]:
        """Get model nodes in topological order."""
        sorted_nodes = self.topological_sort()
        return [name for name in sorted_nodes if self.nodes[name].type == NodeType.MODEL]

    def render_ascii(self, focus: Optional[str] = None) -> str:
        """Render the DAG as ASCII art."""
        lines: list[str] = []

        # Get nodes to display
        if focus:
            upstream = self.get_upstream(focus)
            downstream = self.get_downstream(focus)
            nodes_to_show = upstream | downstream | {focus}
        else:
            nodes_to_show = set(self.nodes.keys())

        # Get topological order
        sorted_nodes = self.topological_sort()
        sorted_nodes = [n for n in sorted_nodes if n in nodes_to_show]

        # Group by level (distance from sources)
        levels: dict[str, int] = {}
        for name in sorted_nodes:
            node = self.nodes[name]
            if not node.upstream:
                levels[name] = 0
            else:
                levels[name] = max(levels.get(u, 0) for u in node.upstream) + 1

        # Render each node
        for name in sorted_nodes:
            node = self.nodes[name]
            indent = "    " * levels.get(name, 0)
            type_str = f"({node.type.value}"
            if node.materialized:
                type_str += f":{node.materialized}"
            type_str += ")"

            if node.downstream & nodes_to_show:
                lines.append(f"{indent}{name} {type_str}")
                lines.append(f"{indent}    │")
                lines.append(f"{indent}    ▼")
            else:
                lines.append(f"{indent}{name} {type_str}")

        return "\n".join(lines)

    def render_mermaid(self, focus: Optional[str] = None) -> str:
        """Render the DAG as a Mermaid flowchart."""
        lines = ["graph LR"]

        if focus:
            upstream = self.get_upstream(focus)
            downstream = self.get_downstream(focus)
            nodes_to_show = upstream | downstream | {focus}
        else:
            nodes_to_show = set(self.nodes.keys())

        sorted_nodes = [n for n in self.topological_sort() if n in nodes_to_show]

        # Declare nodes with shapes based on type
        for name in sorted_nodes:
            node = self.nodes[name]
            safe = name.replace("-", "_").replace(".", "_")
            if node.type == NodeType.SOURCE:
                lines.append(f'    {safe}[("{name}")]')
            elif node.type == NodeType.EXPOSURE:
                lines.append(f'    {safe}>{{"{name}"}}]')
            else:
                lines.append(f'    {safe}["{name}"]')

        # Declare edges
        for name in sorted_nodes:
            node = self.nodes[name]
            safe_from = name.replace("-", "_").replace(".", "_")
            for ds in sorted(node.downstream & nodes_to_show):
                safe_to = ds.replace("-", "_").replace(".", "_")
                lines.append(f"    {safe_from} --> {safe_to}")

        # Style source nodes
        source_ids = [
            name.replace("-", "_").replace(".", "_")
            for name in sorted_nodes
            if self.nodes[name].type == NodeType.SOURCE
        ]
        if source_ids:
            lines.append("    classDef source fill:#e1f5fe,stroke:#0288d1")
            lines.append(f"    class {','.join(source_ids)} source")

        return "\n".join(lines)

    def render_dot(self, focus: Optional[str] = None) -> str:
        """Render the DAG as a Graphviz DOT graph."""
        lines = [
            "digraph streamt {",
            "    rankdir=LR;",
            '    node [shape=box, style=filled, fillcolor="#f5f5f5"];',
        ]

        if focus:
            upstream = self.get_upstream(focus)
            downstream = self.get_downstream(focus)
            nodes_to_show = upstream | downstream | {focus}
        else:
            nodes_to_show = set(self.nodes.keys())

        sorted_nodes = [n for n in self.topological_sort() if n in nodes_to_show]

        for name in sorted_nodes:
            node = self.nodes[name]
            attrs: list[str] = [f'label="{name}"']
            if node.type == NodeType.SOURCE:
                attrs.extend(["shape=cylinder", 'fillcolor="#e1f5fe"'])
            elif node.type == NodeType.EXPOSURE:
                attrs.extend(["shape=hexagon", 'fillcolor="#fff3e0"'])
            else:
                attrs.append('fillcolor="#e8f5e9"')
            lines.append(f'    "{name}" [{", ".join(attrs)}];')

        for name in sorted_nodes:
            node = self.nodes[name]
            for ds in sorted(node.downstream & nodes_to_show):
                lines.append(f'    "{name}" -> "{ds}";')

        lines.append("}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        """Convert DAG to dictionary for JSON serialization."""
        return {
            "nodes": [
                {
                    "name": node.name,
                    "type": node.type.value,
                    "materialized": node.materialized,
                    "upstream": list(node.upstream),
                    "downstream": list(node.downstream),
                }
                for node in self.nodes.values()
            ],
            "edges": [
                {"from": name, "to": downstream}
                for name, node in self.nodes.items()
                for downstream in node.downstream
            ],
        }


@dataclass
class ColumnLineage:
    """Column-level lineage entry."""

    model: str
    column: str
    upstream: list[tuple[str, str]]  # (source_or_model_name, column_name)


class ColumnLineageBuilder:
    """Builds column-level lineage by tracing SELECT column origins."""

    def __init__(self, project: StreamtProject) -> None:
        self.project = project

    def build(self, model_name: str) -> list[ColumnLineage]:
        """Trace column origins for a model."""
        import re

        model = self.project.get_model(model_name)
        if not model or not model.sql:
            return []

        # Parse refs from SQL
        sources = re.findall(r"\{\{\s*source\s*\(\s*[\"'](\w+)[\"']\s*\)\s*\}\}", model.sql)
        refs = re.findall(r"\{\{\s*ref\s*\(\s*[\"'](\w+)[\"']\s*\)\s*\}\}", model.sql)

        # Build available column map from upstream
        upstream_cols: dict[str, list[str]] = {}
        for src_name in sources:
            src = self.project.get_source(src_name)
            if src:
                upstream_cols[src_name] = [c.name for c in src.columns]
        for ref_name in refs:
            ref_model = self.project.get_model(ref_name)
            if ref_model and ref_model.columns:
                upstream_cols[ref_name] = [c.name for c in ref_model.columns]
            elif ref_model and ref_model.contract and ref_model.contract.columns:
                upstream_cols[ref_name] = [c.name for c in ref_model.contract.columns]

        # Extract SELECT columns via simple regex (handles aliased and direct columns)
        clean_sql = re.sub(
            r"\{\{\s*(?:source|ref)\s*\(\s*[\"'](\w+)[\"']\s*\)\s*\}\}", r"\1", model.sql
        )
        select_match = re.match(r"(?i)\s*SELECT\s+(.*?)\s+FROM\s+", clean_sql, re.DOTALL)
        if not select_match:
            return []

        cols_part = select_match.group(1)
        if cols_part.strip() == "*":
            # SELECT * → all upstream columns
            results = []
            for up_name, up_cols in upstream_cols.items():
                for col in up_cols:
                    results.append(
                        ColumnLineage(model=model_name, column=col, upstream=[(up_name, col)])
                    )
            return results

        results = []
        for expr in cols_part.split(","):
            expr = expr.strip()
            # Handle "expr AS alias" or plain column
            alias_match = re.search(r"\bAS\s+(\w+)\s*$", expr, re.IGNORECASE)
            output_col = alias_match.group(1) if alias_match else expr.split(".")[-1].strip()
            # Trace to upstream: look for table.col or bare col
            col_ref_match = re.match(r"(\w+)\.(\w+)", expr)
            upstream_refs: list[tuple[str, str]] = []
            if col_ref_match:
                upstream_refs.append((col_ref_match.group(1), col_ref_match.group(2)))
            else:
                bare_col = (
                    expr.split()[-1]
                    if not alias_match
                    else re.sub(r"\s+AS\s+\w+$", "", expr, flags=re.IGNORECASE).strip()
                )
                for up_name, up_cols in upstream_cols.items():
                    if bare_col in up_cols:
                        upstream_refs.append((up_name, bare_col))
            if upstream_refs:
                results.append(
                    ColumnLineage(model=model_name, column=output_col, upstream=upstream_refs)
                )
        return results


class DAGBuilder:
    """Builder for DAG from streamt project."""

    def __init__(self, project: StreamtProject) -> None:
        """Initialize builder with project."""
        self.project = project
        self.parser = ProjectParser(project.project_path) if project.project_path else None

    def build(self) -> DAG:
        """Build the DAG from the project."""
        dag = DAG()

        # Add source nodes
        for source in self.project.sources:
            dag.add_node(
                DAGNode(
                    name=source.name,
                    type=NodeType.SOURCE,
                )
            )

        # Add model nodes
        for model in self.project.models:
            dag.add_node(
                DAGNode(
                    name=model.name,
                    type=NodeType.MODEL,
                    materialized=model.get_materialized().value,
                )
            )

        # Add exposure nodes
        for exposure in self.project.exposures:
            dag.add_node(
                DAGNode(
                    name=exposure.name,
                    type=NodeType.EXPOSURE,
                )
            )

        # Build edges for models
        for model in self.project.models:
            self._build_model_edges(dag, model)

        # Build edges for exposures
        for exposure in self.project.exposures:
            self._build_exposure_edges(dag, exposure)

        return dag

    def _build_model_edges(self, dag: DAG, model: Model) -> None:
        """Build edges for a model."""
        # Get dependencies from SQL
        if model.sql and self.parser:
            sources, refs = self.parser.extract_refs_from_sql(model.sql)

            for source_name in sources:
                dag.add_edge(source_name, model.name)

            for ref_name in refs:
                dag.add_edge(ref_name, model.name)

        # Also consider explicit 'from' if SQL is not present
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    dag.add_edge(from_ref.source, model.name)
                if from_ref.ref:
                    dag.add_edge(from_ref.ref, model.name)

    def _build_exposure_edges(self, dag: DAG, exposure: Exposure) -> None:
        """Build edges for an exposure."""
        # Consumer exposures depend on models they consume
        for ref in exposure.consumes:
            if ref.ref:
                dag.add_edge(ref.ref, exposure.name)

        # depends_on
        for ref in exposure.depends_on:
            if ref.ref:
                dag.add_edge(ref.ref, exposure.name)
            if ref.source:
                dag.add_edge(ref.source, exposure.name)

        # Producer exposures are upstream of sources they produce
        for ref in exposure.produces:
            if ref.source:
                dag.add_edge(exposure.name, ref.source)
