"""Source/install parity and dependency-isolation gate for DataHub export."""

from __future__ import annotations

import ast
import http.client
import importlib
import importlib.metadata
import importlib.util
import json
import os
import re
import socket
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from collections import Counter
from email.parser import BytesParser
from pathlib import Path
from typing import Any, cast

import requests
import yaml
from click import Command
from click.testing import CliRunner, Result

_CATALOG_ID = "payments-wheel"
_FABRIC = "PROD"
_KAFKA_INSTANCE = "main"
_GATEWAY_PLATFORM = "conduktor-gateway"
_GATEWAY_INSTANCE = "edge,west"

_SECRET_VALUES = (
    "broker-private-token.invalid:19092",
    "broker-private-user",
    "broker-private-password",
    "gateway-private-token.invalid:16969",
    "warehouse-private-password",
    "connector-private-token",
    "snowflake-sink",
    "compiled-sql-private-literal",
    "source-column-private-description",
    "contract-column-private-description",
    "exposure-private-description",
    "dashboard-private-token.invalid",
    "exposure-private-consumer-group",
    "preexisting-output-private-sentinel",
    "source-secret-owner",
    "source-secret-tag",
    "topic-secret-tag",
    "flink-secret-owner",
    "flink-secret-tag",
    "gateway-secret-owner",
    "gateway-secret-tag",
    "sink-secret-owner",
    "sink-secret-tag",
)
_WARNING_COUNTS = {
    "W115_DATAHUB_SINK_OUTPUT_OMITTED": 1,
    "W116_DATAHUB_EXPOSURE_OMITTED": 2,
    "W117_DATAHUB_TAGS_OMITTED": 5,
    "W118_DATAHUB_OWNER_OMITTED": 4,
}

_RUNTIME_PROVIDER_TYPES = (
    ("streamt.deployer.kafka", "KafkaDeployer"),
    ("streamt.deployer.gateway", "GatewayDeployer"),
    ("streamt.deployer.flink", "FlinkDeployer"),
    ("streamt.deployer.connect", "ConnectDeployer"),
    ("streamt.deployer.state_backend", "DeploymentStateService"),
)


def _forbidden_external_access(*_args: object, **_kwargs: object) -> Any:
    raise AssertionError("DataHub export attempted provider, network, or subprocess access")


_WARNING_MESSAGES = {
    "W115_DATAHUB_SINK_OUTPUT_OMITTED": (
        "Connector destination metadata is omitted from DataHub export"
    ),
    "W116_DATAHUB_EXPOSURE_OMITTED": "Exposure metadata is omitted from DataHub export",
    "W117_DATAHUB_TAGS_OMITTED": "Declared tags are omitted from DataHub export",
    "W118_DATAHUB_OWNER_OMITTED": "Declared owner is omitted from DataHub export",
}
_WARNING_SEQUENCE = (
    ("W116_DATAHUB_EXPOSURE_OMITTED", "exposures/0"),
    ("W116_DATAHUB_EXPOSURE_OMITTED", "exposures/1"),
    ("W118_DATAHUB_OWNER_OMITTED", "models/enriched_orders/owner"),
    ("W117_DATAHUB_TAGS_OMITTED", "models/enriched_orders/tags"),
    ("W117_DATAHUB_TAGS_OMITTED", "models/plain_topic/tags"),
    ("W118_DATAHUB_OWNER_OMITTED", "models/public_orders/owner"),
    ("W117_DATAHUB_TAGS_OMITTED", "models/public_orders/tags"),
    ("W115_DATAHUB_SINK_OUTPUT_OMITTED", "models/warehouse_sink"),
    ("W118_DATAHUB_OWNER_OMITTED", "models/warehouse_sink/owner"),
    ("W117_DATAHUB_TAGS_OMITTED", "models/warehouse_sink/tags"),
    ("W118_DATAHUB_OWNER_OMITTED", "sources/raw_orders/owner"),
    ("W117_DATAHUB_TAGS_OMITTED", "sources/raw_orders/tags"),
)


def _write_project(project_dir: Path) -> None:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {
            "name": "payments-wheel-catalog",
            "version": "1.0.0",
            "description": "Payments streaming catalog",
        },
        "runtime": {
            "kafka": {
                "bootstrap_servers": "broker-private-token.invalid:19092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "broker-private-user",
                "sasl_password": "broker-private-password",
            },
            "conduktor": {
                "gateway": {
                    "proxy_bootstrap": "gateway-private-token.invalid:16969",
                }
            },
        },
        "connections": {
            "warehouse": {
                "type": "snowflake",
                "config": {"password": "warehouse-private-password"},
            }
        },
        "sources": [
            {
                "name": "raw_orders",
                "description": "Raw order events",
                "topic": "Orders.Raw",
                "owner": "source-secret-owner",
                "tags": ["source-secret-tag"],
                "columns": [
                    {
                        "name": "private_source_column",
                        "type": "BIGINT",
                        "description": "source-column-private-description",
                    }
                ],
            }
        ],
        "models": [
            {
                "name": "plain_topic",
                "description": "Provisioned order topic",
                "materialized": "topic",
                "topic": {"name": "orders.plain"},
                "tags": ["topic-secret-tag"],
            },
            {
                "name": "enriched_orders",
                "description": "Enriched order events",
                "materialized": "flink",
                "sql": (
                    "SELECT id, 'compiled-sql-private-literal' AS private_marker "
                    'FROM {{ source("raw_orders") }} WHERE id > 0'
                ),
                "topic": {"name": "orders.enriched"},
                "owner": "flink-secret-owner",
                "tags": ["flink-secret-tag"],
                "contract": {
                    "enforced": True,
                    "columns": [
                        {
                            "name": "private_contract_column",
                            "description": "contract-column-private-description",
                        }
                    ],
                },
            },
            {
                "name": "public_orders",
                "description": "Public order projection",
                "materialized": "virtual_topic",
                "sql": 'SELECT * FROM {{ ref("enriched_orders") }} WHERE id IS NOT NULL',
                "gateway": {"virtual_topic": {"name": "orders(public)"}},
                "owner": "gateway-secret-owner",
                "tags": ["gateway-secret-tag"],
                "contract": {
                    "enforced": False,
                    "columns": [{"name": "private_contract_column"}],
                },
            },
            {
                "name": "warehouse_sink",
                "description": "Warehouse delivery",
                "materialized": "sink",
                "from": [{"ref": "public_orders"}],
                "sink": {
                    "connector": "snowflake-sink",
                    "connection": "warehouse",
                    "config": {"api.token": "connector-private-token"},
                },
                "owner": "sink-secret-owner",
                "tags": ["sink-secret-tag"],
            },
        ],
        "exposures": [
            {
                "name": "orders_dashboard",
                "type": "dashboard",
                "description": "exposure-private-description",
                "url": "https://dashboard-private-token.invalid/view",
                "consumer_group": "exposure-private-consumer-group",
                "consumes": [{"ref": "public_orders"}],
            },
            {
                "name": "orders_dashboard",
                "type": "application",
                "description": "exposure-private-description",
                "url": "https://dashboard-private-token.invalid/app",
                "consumer_group": "exposure-private-consumer-group",
                "consumes": [{"ref": "plain_topic"}],
            },
        ],
    }
    (project_dir / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _deny_external_access() -> None:
    socket.getaddrinfo = _forbidden_external_access
    socket.gethostbyname = _forbidden_external_access
    socket.gethostbyname_ex = _forbidden_external_access
    socket.create_connection = _forbidden_external_access
    socket.socket.connect = _forbidden_external_access  # type: ignore[method-assign]
    socket.socket.connect_ex = _forbidden_external_access  # type: ignore[method-assign]
    socket.socket.sendto = _forbidden_external_access  # type: ignore[method-assign]
    http.client.HTTPConnection.connect = _forbidden_external_access  # type: ignore[method-assign]
    http.client.HTTPConnection.request = _forbidden_external_access  # type: ignore[method-assign]
    urllib.request.urlopen = _forbidden_external_access
    requests.sessions.Session.request = _forbidden_external_access  # type: ignore[method-assign]
    subprocess.Popen = _forbidden_external_access  # type: ignore[assignment,misc]
    subprocess.run = _forbidden_external_access
    subprocess.call = _forbidden_external_access
    subprocess.check_call = _forbidden_external_access
    subprocess.check_output = _forbidden_external_access

    import confluent_kafka
    import confluent_kafka.admin

    confluent_kafka.AdminClient = _forbidden_external_access  # type: ignore[attr-defined]
    confluent_kafka.Consumer = _forbidden_external_access  # type: ignore[assignment,misc]
    confluent_kafka.Producer = _forbidden_external_access  # type: ignore[assignment,misc]
    confluent_kafka.admin.AdminClient = _forbidden_external_access  # type: ignore[assignment,misc]


def _deny_streamt_provider_use() -> None:
    """Make every runtime provider fail even before it reaches an I/O seam."""
    for module_name, type_name in _RUNTIME_PROVIDER_TYPES:
        module = importlib.import_module(module_name)
        provider_type = getattr(module, type_name)
        provider_type.__init__ = _forbidden_external_access
        for method_name, method in vars(provider_type).items():
            if method_name.startswith("__") or not callable(method):
                continue
            setattr(provider_type, method_name, _forbidden_external_access)
        setattr(module, type_name, _forbidden_external_access)

    state_module = importlib.import_module("streamt.deployer.state_backend")
    state_module.make_deployment_state_service = _forbidden_external_access


def _assert_datahub_sdk_absent() -> None:
    assert importlib.util.find_spec("datahub") is None
    try:
        importlib.metadata.version("acryl-datahub")
    except importlib.metadata.PackageNotFoundError:
        pass
    else:
        raise AssertionError("acryl-datahub must be absent from the streamt wheel environment")
    assert not any(name == "datahub" or name.startswith("datahub.") for name in sys.modules)


def _normalized_distribution_name(requirement: str) -> str:
    match = re.match(r"\s*([A-Za-z0-9_.-]+)", requirement)
    assert match is not None, "installed streamt has an invalid requirement"
    return re.sub(r"[-_.]+", "-", match.group(1)).lower()


def _assert_metadata_has_no_sdk(metadata: Any) -> None:
    assert metadata["Name"] == "streamt"
    requirements = metadata.get_all("Requires-Dist") or []
    assert "acryl-datahub" not in {
        _normalized_distribution_name(requirement) for requirement in requirements
    }
    extras = {
        re.sub(r"[-_.]+", "-", extra).lower()
        for extra in (metadata.get_all("Provides-Extra") or [])
    }
    assert "datahub" not in extras


def _assert_installed_distribution_contract() -> None:
    _assert_metadata_has_no_sdk(importlib.metadata.metadata("streamt"))


def _is_official_datahub_module(name: str) -> bool:
    return name == "datahub" or name.startswith("datahub.")


def _literal_import_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _literal_import_name(node.left)
        right = _literal_import_name(node.right)
        return left + right if left is not None and right is not None else None
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
                return None
            parts.append(value.value)
        return "".join(parts)
    return None


class _ProductionImportAudit(ast.NodeVisitor):
    def __init__(self, raw_name: str) -> None:
        self.raw_name = raw_name
        self.importlib_names = {"importlib"}
        self.import_module_names: set[str] = set()
        self.builtins_names = {"builtins"}
        self.dunder_import_names = {"__import__"}

    def _reject(self, module: str) -> None:
        assert not _is_official_datahub_module(module), self.raw_name

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._reject(alias.name)
            if alias.name == "importlib":
                self.importlib_names.add(alias.asname or alias.name)
            elif alias.name == "builtins":
                self.builtins_names.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.level == 0 and node.module is not None:
            self._reject(node.module)
            if node.module == "importlib":
                self.import_module_names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name == "import_module"
                )
            elif node.module == "builtins":
                self.dunder_import_names.update(
                    alias.asname or alias.name for alias in node.names if alias.name == "__import__"
                )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        target = node.func
        dynamic_import = (
            isinstance(target, ast.Name)
            and target.id in self.import_module_names | self.dunder_import_names
        ) or (
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and (
                (target.attr == "import_module" and target.value.id in self.importlib_names)
                or (target.attr == "__import__" and target.value.id in self.builtins_names)
            )
        )
        if dynamic_import and node.args:
            module = _literal_import_name(node.args[0])
            if module is not None:
                self._reject(module)
        self.generic_visit(node)


def _assert_no_official_datahub_imports(source: bytes, raw_name: str) -> None:
    try:
        tree = ast.parse(source, filename=raw_name)
    except (SyntaxError, UnicodeError) as error:
        raise AssertionError(f"production source is not parseable: {raw_name}") from error
    _ProductionImportAudit(raw_name).visit(tree)


def _is_top_level_datahub_namespace(parts: tuple[str, ...]) -> bool:
    namespace_names = {"datahub", "datahub.py"}
    if parts and parts[0] in namespace_names:
        return True
    if len(parts) >= 2 and parts[0] == "src" and parts[1] in namespace_names:
        return True
    return any(
        part in {"purelib", "platlib", "site-packages"}
        and position + 1 < len(parts)
        and parts[position + 1] in namespace_names
        for position, part in enumerate(parts)
    )


def _is_streamt_production_source(parts: tuple[str, ...], *, sdist: bool) -> bool:
    if sdist and parts[:2] == ("src", "streamt"):
        return True
    if not sdist and parts[:1] == ("streamt",):
        return True
    return any(
        part in {"purelib", "platlib", "site-packages"}
        and position + 1 < len(parts)
        and parts[position + 1] == "streamt"
        for position, part in enumerate(parts)
    )


def _assert_distribution_audit_guards() -> None:
    forbidden_sources = (
        b"import datahub\n",
        b"from datahub.metadata import urns\n",
        b'import importlib\nimportlib.import_module("datahub.emitter")\n',
        b'from importlib import import_module as load\nload("data" + "hub")\n',
        b'__import__(f"datahub.ingestion")\n',
        b'import builtins as runtime\nruntime.__import__("datahub")\n',
    )
    for source in forbidden_sources:
        try:
            _assert_no_official_datahub_imports(source, "streamt/forbidden.py")
        except AssertionError:
            pass
        else:
            raise AssertionError("production DataHub import audit did not reject a fixture")

    _assert_no_official_datahub_imports(
        b'import importlib\nimportlib.import_module("psycopg")\nfrom . import datahub\n',
        "streamt/safe.py",
    )
    for parts in (
        ("datahub", "__init__.py"),
        ("src", "datahub", "__init__.py"),
        ("streamt-0.1.data", "purelib", "datahub", "__init__.py"),
        ("streamt-0.1.data", "platlib", "datahub.py"),
    ):
        assert _is_top_level_datahub_namespace(parts), parts
    assert not _is_top_level_datahub_namespace(
        ("tests", "fixtures", "datahub", "v1.7.0", "identity-vectors.json")
    )


def _assert_archive_members_do_not_vendor_sdk(
    members: dict[str, bytes],
    *,
    sdist: bool,
) -> None:
    for raw_name in members:
        name = raw_name.replace("\\", "/")
        relative = name.split("/", 1)[1] if sdist and "/" in name else name
        parts = tuple(part.lower() for part in relative.split("/") if part)
        assert not _is_top_level_datahub_namespace(parts), raw_name
        assert not any(
            part.replace("-", "_").startswith("acryl_datahub_")
            and part.endswith((".dist-info", ".egg-info"))
            for part in parts
        ), raw_name

        is_streamt_source = _is_streamt_production_source(parts, sdist=sdist)
        if is_streamt_source:
            assert "datahub" not in parts[:-1], raw_name
        if is_streamt_source and relative.endswith(".py"):
            _assert_no_official_datahub_imports(members[raw_name], raw_name)


def _assert_built_distributions(distributions_dir: Path) -> None:
    wheels = tuple(distributions_dir.glob("*.whl"))
    sdists = tuple(distributions_dir.glob("*.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1

    with zipfile.ZipFile(wheels[0]) as archive:
        wheel_members = {
            name: archive.read(name) for name in archive.namelist() if not name.endswith("/")
        }
    metadata_names = [name for name in wheel_members if name.endswith(".dist-info/METADATA")]
    assert len(metadata_names) == 1
    _assert_metadata_has_no_sdk(BytesParser().parsebytes(wheel_members[metadata_names[0]]))
    _assert_archive_members_do_not_vendor_sdk(wheel_members, sdist=False)

    with tarfile.open(sdists[0], mode="r:gz") as archive:
        sdist_members: dict[str, bytes] = {}
        for member in archive.getmembers():
            if not member.isfile():
                continue
            extracted = archive.extractfile(member)
            assert extracted is not None
            sdist_members[member.name] = extracted.read()
    pkg_info_names = [
        name for name in sdist_members if name.count("/") == 1 and name.endswith("/PKG-INFO")
    ]
    assert len(pkg_info_names) == 1
    _assert_metadata_has_no_sdk(BytesParser().parsebytes(sdist_members[pkg_info_names[0]]))
    _assert_archive_members_do_not_vendor_sdk(sdist_members, sdist=True)


def _assert_source_checkout(streamt_module: Any, checkout: Path) -> None:
    imported_module = Path(streamt_module.__file__).resolve()
    assert (checkout / "src").resolve() in imported_module.parents
    assert checkout != Path.cwd()
    assert checkout not in Path.cwd().parents


def _assert_installed_wheel(streamt_module: Any, checkout: Path) -> None:
    checkout_source = (checkout / "src").resolve()
    installed_module = Path(streamt_module.__file__).resolve()
    import_roots = {Path(entry).resolve() for entry in sys.path if entry}
    assert sys.flags.isolated == 1
    assert "PYTHONPATH" not in os.environ
    assert checkout_source not in import_roots
    assert checkout not in installed_module.parents
    assert checkout != Path.cwd()
    assert checkout not in Path.cwd().parents
    assert Path(sys.executable).with_name("streamt").is_file()


def _load_cli() -> tuple[Any, Command]:
    _assert_datahub_sdk_absent()
    _deny_external_access()
    streamt_module = importlib.import_module("streamt")
    cli_module = importlib.import_module("streamt.cli")
    _deny_streamt_provider_use()

    _assert_datahub_sdk_absent()
    return streamt_module, cast(Command, cli_module.main)


def _common_arguments(project_dir: Path, *, kafka_instance: bool) -> list[str]:
    arguments = [
        "docs",
        "datahub",
        "--catalog-id",
        _CATALOG_ID,
        "--fabric",
        _FABRIC,
        "--gateway-platform-id",
        _GATEWAY_PLATFORM,
        "--gateway-platform-instance",
        _GATEWAY_INSTANCE,
        "--project-dir",
        str(project_dir),
    ]
    if kafka_instance:
        arguments[6:6] = ["--kafka-platform-instance", _KAFKA_INSTANCE]
    return arguments


def _invoke(main: Command, *arguments: str) -> Result:
    result = CliRunner().invoke(main, list(arguments), catch_exceptions=False)
    assert result.exit_code == 0, f"streamt docs datahub returned {result.exit_code}"
    return result


def _proposal_payload(result: Result) -> list[dict[str, Any]]:
    assert result.stdout.endswith("\n")
    assert not result.stdout.endswith("\n\n")
    assert "\r" not in result.stdout
    decoded = json.loads(result.stdout)
    assert isinstance(decoded, list)
    assert decoded
    assert result.stdout == _canonical(decoded)
    return cast(list[dict[str, Any]], decoded)


def _assert_warning_text(stderr: str) -> None:
    assert stderr.splitlines() == [
        f"WARNING: {_WARNING_MESSAGES[code]}" for code, _location in _WARNING_SEQUENCE
    ]
    for code, count in _WARNING_COUNTS.items():
        assert code not in stderr
        assert stderr.count(_WARNING_MESSAGES[code]) == count


def _assert_proposals(
    proposals: list[dict[str, Any]],
    *,
    kafka_instance: bool,
) -> None:
    expected_count = 15 if kafka_instance else 12
    assert len(proposals) == expected_count
    for proposal in proposals:
        assert set(proposal) == {
            "aspect",
            "aspectName",
            "changeType",
            "entityType",
            "entityUrn",
        }
        assert proposal["changeType"] == "UPSERT"
        assert set(proposal["aspect"]) == {"json"}

    assert (proposals[0]["entityType"], proposals[0]["aspectName"]) == (
        "dataFlow",
        "dataFlowInfo",
    )
    assert proposals[0]["aspect"]["json"] == {
        "name": "payments-wheel-catalog",
        "description": "Payments streaming catalog",
        "env": "PROD",
        "customProperties": {},
    }

    dataset_proposals = [item for item in proposals if item["entityType"] == "dataset"]
    job_proposals = [item for item in proposals if item["entityType"] == "dataJob"]
    assert proposals == [proposals[0], *dataset_proposals, *job_proposals]
    assert [item["entityUrn"] for item in dataset_proposals] == sorted(
        (item["entityUrn"] for item in dataset_proposals), key=lambda urn: urn.encode("utf-8")
    )
    assert [item["entityUrn"] for item in job_proposals] == sorted(
        (item["entityUrn"] for item in job_proposals), key=lambda urn: urn.encode("utf-8")
    )

    by_pair = {
        (item["entityUrn"], item["aspectName"]): item["aspect"]["json"] for item in proposals
    }
    dataset_urns = {
        payload["name"]: urn
        for (urn, aspect_name), payload in by_pair.items()
        if aspect_name == "datasetProperties"
    }
    assert set(dataset_urns) == {
        "raw_orders",
        "plain_topic",
        "enriched_orders",
        "public_orders",
    }
    assert by_pair[(dataset_urns["raw_orders"], "datasetProperties")]["customProperties"] == {}
    assert by_pair[(dataset_urns["plain_topic"], "datasetProperties")]["customProperties"] == {}
    assert by_pair[(dataset_urns["enriched_orders"], "datasetProperties")]["customProperties"] == {
        "streamt.contract.status": "enforced"
    }
    assert by_pair[(dataset_urns["public_orders"], "datasetProperties")]["customProperties"] == {
        "streamt.contract.status": "declared"
    }

    gateway_urn = dataset_urns["public_orders"]
    assert gateway_urn == (
        "urn:li:dataset:(urn:li:dataPlatform:conduktor-gateway,edge%2Cwest.orders%28public%29,PROD)"
    )
    assert by_pair[(gateway_urn, "dataPlatformInstance")] == {
        "platform": "urn:li:dataPlatform:conduktor-gateway",
        "instance": (
            "urn:li:dataPlatformInstance:(urn:li:dataPlatform:conduktor-gateway,edge%2Cwest)"
        ),
    }

    kafka_physical = {
        "raw_orders": "Orders.Raw",
        "plain_topic": "orders.plain",
        "enriched_orders": "orders.enriched",
    }
    for logical_name, physical_name in kafka_physical.items():
        prefix = f"{_KAFKA_INSTANCE}." if kafka_instance else ""
        expected_urn = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{prefix}{physical_name},PROD)"
        assert dataset_urns[logical_name] == expected_urn
        pair = (expected_urn, "dataPlatformInstance")
        if kafka_instance:
            assert by_pair[pair] == {
                "platform": "urn:li:dataPlatform:kafka",
                "instance": "urn:li:dataPlatformInstance:(urn:li:dataPlatform:kafka,main)",
            }
        else:
            assert pair not in by_pair

    job_infos = {
        payload["name"]: urn
        for (urn, aspect_name), payload in by_pair.items()
        if aspect_name == "dataJobInfo"
    }
    assert set(job_infos) == {"enriched_orders", "public_orders", "warehouse_sink"}
    expected_edges = {
        "enriched_orders": ([dataset_urns["raw_orders"]], [dataset_urns["enriched_orders"]]),
        "public_orders": (
            [dataset_urns["enriched_orders"]],
            [dataset_urns["public_orders"]],
        ),
        "warehouse_sink": ([dataset_urns["public_orders"]], []),
    }
    for logical_name, job_urn in job_infos.items():
        info = by_pair[(job_urn, "dataJobInfo")]
        assert info["type"] == {
            "string": {
                "enriched_orders": "flink",
                "public_orders": "gateway",
                "warehouse_sink": "connect",
            }[logical_name]
        }
        lineage = by_pair[(job_urn, "dataJobInputOutput")]
        assert set(lineage) == {
            "inputDatasets",
            "outputDatasets",
            "inputDatasetEdges",
            "outputDatasetEdges",
        }
        assert lineage["inputDatasets"] == []
        assert lineage["outputDatasets"] == []
        expected_inputs, expected_outputs = expected_edges[logical_name]
        assert lineage["inputDatasetEdges"] == [{"destinationUrn": urn} for urn in expected_inputs]
        assert lineage["outputDatasetEdges"] == [
            {"destinationUrn": urn} for urn in expected_outputs
        ]

    assert not any(
        item["entityType"] == "dataJob" and "plain_topic" in item["entityUrn"] for item in proposals
    )


def _assert_structured(
    result: Result,
    raw_proposals: list[dict[str, Any]],
    *,
    kafka_instance: bool,
) -> None:
    assert result.stderr == ""
    payload = json.loads(result.stdout)
    assert set(payload) == {"status", "command", "data", "errors", "warnings"}
    assert payload["status"] == "ok"
    assert payload["command"] == "docs datahub"
    assert payload["errors"] == []
    data = payload["data"]
    assert data["standard"] == "DataHub MCP"
    assert data["release"] == "1.7.0"
    assert data["api_version"] == "MetadataChangeProposal"
    assert data["proposals"] == raw_proposals
    assert data["output_file"] is None
    assert data["counts"] == {
        "proposals": 15 if kafka_instance else 12,
        "entities": {"dataFlow": 1, "dataset": 4, "dataJob": 3},
        "aspects": {
            "dataFlowInfo": 1,
            "datasetProperties": 4,
            "dataPlatformInstance": 4 if kafka_instance else 1,
            "dataJobInfo": 3,
            "dataJobInputOutput": 3,
        },
    }
    warnings = payload["warnings"]
    assert all(list(warning) == ["code", "message", "location"] for warning in warnings)
    assert warnings == [
        {
            "code": code,
            "message": _WARNING_MESSAGES[code],
            "location": location,
        }
        for code, location in _WARNING_SEQUENCE
    ]
    warning_counts = Counter(item["code"] for item in warnings)
    assert dict(warning_counts) == _WARNING_COUNTS


def _assert_secret_neutral(rendered: str, checkout: Path, *temporary_paths: Path) -> None:
    for sentinel in (*_SECRET_VALUES, str(checkout), *(str(path) for path in temporary_paths)):
        assert sentinel not in rendered, sentinel
    for forbidden in (
        "compiled-sql-private-literal",
        "bootstrap_servers",
        "sasl_password",
        "consumer_group",
        "api.token",
        "connections",
        "runtime",
    ):
        assert forbidden not in rendered, forbidden


def _render_raw(
    main: Command,
    project_dir: Path,
    *,
    kafka_instance: bool,
) -> tuple[Result, list[dict[str, Any]]]:
    common = _common_arguments(project_dir, kafka_instance=kafka_instance)
    first = _invoke(main, *common)
    second = _invoke(main, *common)
    assert first.stdout == second.stdout
    assert first.stderr == second.stderr
    proposals = _proposal_payload(first)
    _assert_warning_text(first.stderr)
    _assert_proposals(proposals, kafka_instance=kafka_instance)
    return first, proposals


def _write_source_baselines() -> None:
    checkout = Path(os.environ["STREAMT_CHECKOUT"]).resolve()
    baseline_dir = Path(os.environ["STREAMT_DATAHUB_BASELINE_DIR"]).resolve()
    distributions_dir = Path(os.environ["STREAMT_DISTRIBUTIONS_DIR"]).resolve()
    assert baseline_dir.is_dir()
    assert checkout not in baseline_dir.parents
    _assert_distribution_audit_guards()
    _assert_built_distributions(distributions_dir)
    streamt_module, main = _load_cli()
    _assert_source_checkout(streamt_module, checkout)
    with tempfile.TemporaryDirectory(prefix="streamt-datahub-source-") as raw_root:
        root = Path(raw_root)
        project_dir = root / "project"
        project_dir.mkdir()
        _write_project(project_dir)
        full, _ = _render_raw(main, project_dir, kafka_instance=True)
        no_instance, _ = _render_raw(main, project_dir, kafka_instance=False)
        rendered = "\n".join((full.stdout, full.stderr, no_instance.stdout, no_instance.stderr))
        _assert_secret_neutral(rendered, checkout, root, project_dir)
        (baseline_dir / "with-instance.json").write_bytes(full.stdout.encode("utf-8"))
        (baseline_dir / "without-kafka-instance.json").write_bytes(
            no_instance.stdout.encode("utf-8")
        )
    _assert_datahub_sdk_absent()


def _exercise_installed_wheel() -> None:
    checkout = Path(os.environ["STREAMT_CHECKOUT"]).resolve()
    baseline_dir = Path(os.environ["STREAMT_DATAHUB_BASELINE_DIR"]).resolve()
    artifact_dir = Path(os.environ["STREAMT_DATAHUB_ARTIFACT_DIR"]).resolve()
    assert checkout not in baseline_dir.parents
    assert checkout not in artifact_dir.parents
    assert baseline_dir.is_dir()
    assert artifact_dir.is_dir()
    _assert_installed_distribution_contract()
    streamt_module, main = _load_cli()
    _assert_installed_wheel(streamt_module, checkout)

    with tempfile.TemporaryDirectory(prefix="streamt-datahub-wheel-") as raw_root:
        root = Path(raw_root)
        project_dir = root / "project"
        project_dir.mkdir()
        output_file = root / "catalog.json"
        _write_project(project_dir)

        full, full_proposals = _render_raw(main, project_dir, kafka_instance=True)
        no_instance, no_instance_proposals = _render_raw(
            main,
            project_dir,
            kafka_instance=False,
        )
        assert full.stdout.encode("utf-8") == (baseline_dir / "with-instance.json").read_bytes()
        assert (
            no_instance.stdout.encode("utf-8")
            == (baseline_dir / "without-kafka-instance.json").read_bytes()
        )

        structured_full = _invoke(
            main,
            "-o",
            "json",
            *_common_arguments(project_dir, kafka_instance=True),
        )
        structured_no_instance = _invoke(
            main,
            "-o",
            "json",
            *_common_arguments(project_dir, kafka_instance=False),
        )
        _assert_structured(structured_full, full_proposals, kafka_instance=True)
        _assert_structured(
            structured_no_instance,
            no_instance_proposals,
            kafka_instance=False,
        )

        output_file.write_text("preexisting-output-private-sentinel", encoding="utf-8")
        file_result = _invoke(
            main,
            *_common_arguments(project_dir, kafka_instance=True),
            "--output-file",
            str(output_file),
        )
        assert file_result.stdout == ""
        _assert_warning_text(file_result.stderr)
        assert output_file.read_bytes() == full.stdout.encode("utf-8")

        quiet = _invoke(
            main,
            "--quiet",
            *_common_arguments(project_dir, kafka_instance=True),
        )
        assert quiet.stdout == ""
        assert quiet.stderr == ""
        (artifact_dir / "with-instance.json").write_bytes(full.stdout.encode("utf-8"))
        (artifact_dir / "without-kafka-instance.json").write_bytes(
            no_instance.stdout.encode("utf-8")
        )

        rendered = "\n".join(
            (
                full.stdout,
                full.stderr,
                no_instance.stdout,
                no_instance.stderr,
                structured_full.stdout,
                structured_no_instance.stdout,
                file_result.stdout,
                file_result.stderr,
                output_file.read_text(encoding="utf-8"),
            )
        )
        _assert_secret_neutral(rendered, checkout, root, project_dir, output_file)
    _assert_datahub_sdk_absent()


if __name__ == "__main__":
    if os.environ.get("STREAMT_DATAHUB_SOURCE_MODE") == "1":
        _write_source_baselines()
        print("source-checkout DataHub catalog baselines passed")
    else:
        _exercise_installed_wheel()
        print("installed-wheel DataHub catalog export passed")
