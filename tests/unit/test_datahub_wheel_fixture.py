"""Keep the installed-wheel DataHub fixture covered by the normal unit gate."""

from __future__ import annotations

from pathlib import Path

import pytest

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from tests.package import datahub_catalog_wheel_smoke as smoke


@pytest.mark.parametrize("kafka_instance", [True, False])
def test_datahub_wheel_fixture_compiles_and_exports_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kafka_instance: bool
) -> None:
    for target in (
        *(f"{module}.{name}" for module, name in smoke._RUNTIME_PROVIDER_TYPES),
        "streamt.deployer.schema_registry.SchemaRegistryDeployer",
        "socket.getaddrinfo",
        "socket.create_connection",
        "socket.socket.connect",
        "socket.socket.connect_ex",
        "requests.sessions.Session.request",
        "subprocess.Popen",
        "subprocess.run",
        "confluent_kafka.Consumer",
        "confluent_kafka.Producer",
        "confluent_kafka.admin.AdminClient",
    ):
        monkeypatch.setattr(target, smoke._forbidden_external_access)

    smoke._write_project(tmp_path)
    project = ProjectParser(tmp_path).parse()
    compiler = Compiler(project)
    compiler.compile(dry_run=True)
    assert {exposure.name for exposure in project.exposures} == {
        "orders_dashboard", "orders_application"
    }
    assert compiler.dag.nodes["orders_dashboard"].upstream == {"public_orders"}
    assert compiler.dag.nodes["orders_application"].upstream == {"plain_topic"}
    assert not (tmp_path / "generated").exists()

    raw, proposals = smoke._render_raw(main, tmp_path, kafka_instance=kafka_instance)
    structured = smoke._invoke(
        main, "-o", "json", *smoke._common_arguments(tmp_path, kafka_instance=kafka_instance)
    )
    smoke._assert_structured(structured, proposals, kafka_instance=kafka_instance)
    smoke._assert_secret_neutral(
        raw.stdout + raw.stderr + structured.stdout,
        Path(__file__).resolve().parents[2],
        tmp_path,
    )
