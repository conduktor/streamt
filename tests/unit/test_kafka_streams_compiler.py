"""Provider-free SQL, identity, schema, and manifest gates for the fixed runner."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from streamt.compiler import Compiler
from streamt.compiler.kafka_streams import KafkaStreamsPlanError, compile_plan, validate_plan
from streamt.compiler.manifest import (
    KafkaStreamsArtifactFormatError,
    Manifest,
    parse_compiled_kafka_streams_job_artifact,
)
from streamt.compiler.model_resolution import CompileError
from streamt.core.models import Executor, MaterializedType, Model, StreamtProject
from streamt.core.parser import ParseError, ProjectParser
from streamt.core.runtime import KafkaStreamsConfig

IMAGE = "sha256:" + "a" * 64
SOURCE_COLUMNS = [
    {"name": "id", "type": "STRING", "required": True},
    {"name": "amount", "type": "BIGINT", "required": True},
    {"name": "active", "type": "BOOLEAN"},
]
SQL = 'SELECT id AS order_id, amount FROM {{ source("orders") }} WHERE amount >= 50'


def test_plan_size_limit_matches_fixed_runner_before_any_provider():
    plan = compile_plan("SELECT id FROM orders WHERE id = 'x'", {
        "id": {"type": "STRING", "nullable": False},
    }, "orders", "orders.input", "orders.output")
    plan["predicates"][0]["value"] = "x" * 1_048_576
    with pytest.raises(KafkaStreamsPlanError, match="1 MiB"):
        validate_plan(plan)


def _config() -> dict:
    return {
        "project": {"name": "orders-project"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker-private.invalid:9092"},
            "kafka_streams": {"image": IMAGE},
        },
        "sources": [{"name": "orders", "topic": "orders.raw.v1", "columns": copy.deepcopy(SOURCE_COLUMNS)}],
        "models": [{"name": "valuable_orders", "executor": "kafka_streams", "sql": SQL}],
        "exposures": [{
            "name": "fraud_application", "type": "application", "tool": "java",
            "consumes": [{"ref": "valuable_orders"}],
        }],
    }


def _compile(config: dict | None = None, **project_fields: object) -> tuple[Compiler, Manifest]:
    project = StreamtProject.model_validate({**(config or _config()), **project_fields})
    compiler = Compiler(project)
    return compiler, compiler.compile(dry_run=True)


def test_imported_source_managed_output_and_custom_application_compile_without_providers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Compiler attempted provider, network, or process access")

    for target in (
        "streamt.deployer.kafka.KafkaDeployer", "streamt.deployer.flink.FlinkDeployer",
        "streamt.deployer.connect.ConnectDeployer", "streamt.deployer.gateway.GatewayDeployer",
        "streamt.deployer.schema_registry.SchemaRegistryDeployer", "socket.getaddrinfo",
        "socket.create_connection", "subprocess.run", "subprocess.Popen",
    ):
        monkeypatch.setattr(target, forbidden)
    compiler, manifest = _compile(project_path=tmp_path)
    job = manifest.artifacts["kafka_streams_jobs"][0]
    assert not manifest.artifacts["flink_jobs"]
    assert not manifest.artifacts["schemas"]
    assert job["name"] == "valuable_orders"
    assert job["initial_offset"] == "earliest"
    assert job["plan"]["projection"] == [
        {"column": "id", "as": "order_id"}, {"column": "amount", "as": "amount"},
    ]
    assert job["plan"]["schema"]["active"] == {"type": "BOOLEAN", "nullable": True}
    assert job["ownership"] == {
        "mode": "managed", "project": "orders-project", "type": "model", "name": "valuable_orders",
    }
    assert manifest.sources[0]["ownership"]["mode"] == "external"
    assert compiler.compiled_models["valuable_orders"].process_kind == "kafka_streams"
    assert compiler.compiled_models["valuable_orders"].materialized == MaterializedType.TOPIC
    assert compiler.dag.nodes["fraud_application"].upstream == {"valuable_orders"}
    assert "broker-private" not in json.dumps(job)
    assert not (tmp_path / "generated").exists()


def test_application_identity_is_environment_scoped_and_stable_under_supported_changes() -> None:
    _, original = _compile()
    changed = _config()
    changed["models"][0]["sql"] = SQL.replace(">= 50", ">= 100")
    changed["runtime"]["kafka_streams"] = {
        "image": "registry.example/runner@sha256:" + "b" * 64,
        "network": "other-network", "initial_offset": "latest",
    }
    _, evolved = _compile(changed)
    _, production = _compile(environment_name="production")
    original_job = original.artifacts["kafka_streams_jobs"][0]
    evolved_job = evolved.artifacts["kafka_streams_jobs"][0]
    assert original_job["application_id"] == evolved_job["application_id"]
    assert original_job["application_id"] != production.artifacts["kafka_streams_jobs"][0]["application_id"]
    assert original_job["plan"] != evolved_job["plan"]


def test_parser_carries_actual_environment_without_exposing_internal_field(tmp_path: Path) -> None:
    config = _config()
    runtime = config.pop("runtime")
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))
    environments = tmp_path / "environments"
    environments.mkdir()
    for name in ("dev", "production"):
        (environments / f"{name}.yml").write_text(yaml.safe_dump({
            "environment": {"name": name}, "runtime": runtime,
        }))
    jobs = []
    for name in ("dev", "production"):
        project = ProjectParser(tmp_path, environment=name).parse()
        assert project.environment_name == name
        assert "environment_name" not in project.model_dump()
        jobs.append(Compiler(project).compile(dry_run=True).artifacts["kafka_streams_jobs"][0])
    assert jobs[0]["application_id"] != jobs[1]["application_id"]
    config["environment_name"] = "spoofed"
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))
    with pytest.raises(ParseError, match="environment_name"):
        ProjectParser(tmp_path, environment="dev").parse()


@pytest.mark.parametrize("ownership", ["external", "managed", "adopted"])
def test_job_and_output_topic_preserve_declaration_ownership(ownership: str) -> None:
    config = _config()
    config["models"][0]["ownership"] = {"mode": ownership}
    _, manifest = _compile(config)
    assert manifest.artifacts["kafka_streams_jobs"][0]["ownership"]["mode"] == ownership
    assert manifest.artifacts["topics"][0]["ownership"]["mode"] == ownership


def test_ref_chain_uses_actual_inferred_output_without_mutating_declared_columns() -> None:
    config = _config()
    config["models"].append({
        "name": "highest_value", "executor": "kafka_streams",
        "sql": 'SELECT order_id FROM {{ ref("valuable_orders") }} WHERE amount > 100',
    })
    compiler, manifest = _compile(config)
    job = next(item for item in manifest.artifacts["kafka_streams_jobs"] if item["name"] == "highest_value")
    assert job["plan"]["input_topic"] == "valuable_orders"
    assert job["plan"]["schema"] == {
        "order_id": {"type": "STRING", "nullable": False},
        "amount": {"type": "BIGINT", "nullable": False},
    }
    assert compiler.project.models[0].columns is None


@pytest.mark.parametrize("upstream_executor", [None, "flink"])
def test_ref_of_explicitly_typed_kafka_output_is_supported(upstream_executor: str | None) -> None:
    config = _config()
    source_model = {
        "name": "upstream", "materialized": "topic", "columns": copy.deepcopy(SOURCE_COLUMNS),
        "topic": {"name": "upstream.physical"},
    }
    if upstream_executor:
        source_model["executor"] = upstream_executor
        source_model["sql"] = 'SELECT id, amount, active FROM {{ source("orders") }}'
    config["models"][0]["sql"] = SQL.replace('source("orders")', 'ref("upstream")')
    config["models"].append(source_model)
    _, manifest = _compile(config)
    assert manifest.artifacts["kafka_streams_jobs"][0]["plan"]["input_topic"] == "upstream.physical"


@pytest.mark.parametrize("upstream", [
    {"name": "upstream", "materialized": "sink", "from": [{"source": "orders"}], "sink": {"connector": "jdbc-sink"}},
    {"name": "upstream", "gateway": {"virtual_topic": {"name": "external.view"}}, "sql": 'SELECT id FROM {{ source("orders") }}'},
])
def test_gateway_and_connector_inputs_cannot_be_mistaken_for_kafka_outputs(upstream: dict) -> None:
    config = _config()
    config["models"][0]["sql"] = SQL.replace('source("orders")', 'ref("upstream")')
    config["models"].append(upstream)
    with pytest.raises(CompileError, match="Gateway or Connector"):
        _compile(config)


def test_declared_ref_schema_cannot_hide_incompatible_generated_output() -> None:
    config = _config()
    config["models"][0]["sql"] = SQL.replace('source("orders")', 'ref("upstream")')
    config["models"].append({
        "name": "upstream", "materialized": "topic",
        "sql": 'SELECT id, amount, active FROM {{ source("orders") }}',
        "columns": [{**column, "type": "STRING"} for column in SOURCE_COLUMNS],
    })
    with pytest.raises(CompileError, match="inferred SQL output"):
        _compile(config)


@pytest.mark.parametrize("predicate", [
    "amount IS NULL", "amount IS NOT NULL", "amount = -9223372036854775808",
    "amount = 9223372036854775807", "id = 'été'", "active != FALSE",
])
def test_supported_predicate_boundary_stays_compatible_with_plan_v1(predicate: str) -> None:
    config = _config()
    config["models"][0]["sql"] = SQL.replace("amount >= 50", predicate)
    _, manifest = _compile(config)
    assert manifest.artifacts["kafka_streams_jobs"][0]["plan"]["version"] == 1


@pytest.mark.parametrize("columns", [
    [{"name": "order_id", "type": "STRING", "required": True}],
    [{"name": "order_id", "type": "STRING", "required": True}, {"name": "amount", "type": "STRING", "required": True}],
    [{"name": "order_id", "type": "STRING"}, {"name": "amount", "type": "BIGINT", "required": True}],
])
def test_declared_output_must_match_inferred_columns_types_and_nullability(columns: list) -> None:
    config = _config()
    config["models"][0]["columns"] = columns
    with pytest.raises(CompileError, match="disagree"):
        _compile(config)


def test_explicit_columns_and_partial_contract_match_inferred_output() -> None:
    config = _config()
    config["models"][0]["columns"] = [
        {"name": "order_id", "type": "STRING", "required": True},
        {"name": "amount", "type": "BIGINT", "required": True},
    ]
    config["models"][0]["contract"] = {"columns": [{"name": "order_id"}, {"name": "amount", "type": "BIGINT"}]}
    assert _compile(config)[1].artifacts["kafka_streams_jobs"]


@pytest.mark.parametrize("schema", [
    {"format": "avro"}, {"format": "protobuf"}, {},
    {"format": "json", "registry": "default"}, {"format": "json", "subject": "orders-value"},
    {"format": "json", "definition": "{}"},
    {"format": "json", "fields": [{"name": "id", "type": "STRING"}]},
])
def test_framed_unknown_or_conflicting_input_schema_fails_closed(schema: dict) -> None:
    config = _config()
    config["sources"][0]["schema"] = schema
    with pytest.raises(CompileError, match="Kafka Streams"):
        _compile(config)


@pytest.mark.parametrize("columns", [
    [], [{"name": "id"}], [{"name": "id", "type": "INT"}],
    [{"name": "id", "type": "DOUBLE"}], [{"name": "id", "type": "STRING", "proctime": True}],
    [{"name": "id", "type": "STRING"}, {"name": "id", "type": "STRING"}],
])
def test_missing_or_unsupported_input_columns_are_not_inferred(columns: list) -> None:
    config = _config()
    config["sources"][0]["columns"] = columns
    with pytest.raises(CompileError, match="Kafka Streams"):
        _compile(config)


@pytest.mark.parametrize("extra", [
    {"materialized": "flink"}, {"materialized": "virtual_topic"}, {"materialized": "sink"},
    {"flink": {}}, {"flink_cluster": "main"}, {"gateway": {}}, {"key": "id"},
    {"primary_key": ["id"]}, {"security": {}}, {"connect_cluster": "main"},
])
def test_conflicting_executor_configuration_is_not_silently_ignored(extra: dict) -> None:
    with pytest.raises(ValidationError, match="kafka_streams"):
        Model.model_validate({"name": "model", "executor": "kafka_streams", "sql": SQL, **extra})


def test_compiler_rejects_conflicting_model_copy_and_clears_prior_artifacts() -> None:
    compiler, _ = _compile()
    original = compiler.project.models[0]
    compiler.project.models[0] = original.model_copy(update={"flink_cluster": "unsupported"})
    with pytest.raises(CompileError, match="configuration"):
        compiler.compile(dry_run=True)
    assert compiler.kafka_streams_jobs == []
    assert compiler.topics == []
    assert not compiler.compiled_models


def test_physical_self_loop_is_rejected_before_writing_any_artifact(tmp_path: Path) -> None:
    config = _config()
    config["models"][0]["topic"] = {"name": "orders.raw.v1"}
    compiler = Compiler(StreamtProject.model_validate({**config, "project_path": tmp_path}))
    with pytest.raises(CompileError, match="must differ"):
        compiler.compile()
    assert not (tmp_path / "generated").exists()
    assert compiler.kafka_streams_jobs == []


def test_normal_compilation_writes_only_secret_free_runner_configuration(tmp_path: Path) -> None:
    compiler, _ = _compile(project_path=tmp_path)
    manifest = compiler.compile()
    path = tmp_path / "generated" / "kafka_streams" / "valuable_orders.json"
    artifact = json.loads(path.read_text())
    assert artifact == manifest.artifacts["kafka_streams_jobs"][0]
    assert "broker-private" not in path.read_text()
    assert parse_compiled_kafka_streams_job_artifact(artifact).to_dict() == artifact
    assert Manifest.load(tmp_path / "generated" / "manifest.json").artifacts == manifest.artifacts


def test_existing_models_and_explicit_flink_keep_original_compilation_behavior() -> None:
    config = _config()
    config["runtime"]["flink"] = {"clusters": {"main": {"rest_url": "http://flink.invalid:8081"}}}
    config["models"][0].pop("executor")
    _, implicit = _compile(config)
    assert "kafka_streams_jobs" not in implicit.artifacts
    assert "executor" not in implicit.models[0]
    config["models"][0]["executor"] = "flink"
    compiler, explicit = _compile(config)
    assert explicit.artifacts["flink_jobs"] == implicit.artifacts["flink_jobs"]
    assert compiler.project.models[0].executor == Executor.FLINK


@pytest.mark.parametrize("image", ["runner:latest", "runner:v1", "sha256:abc", "a" * 64, "https://runner@sha256:" + "b" * 64, "sha256:" + "A" * 64])
def test_runtime_requires_pinned_image(image: str) -> None:
    with pytest.raises(ValidationError):
        KafkaStreamsConfig(image=image)


@pytest.mark.parametrize(("field", "value"), [
    ("startup_timeout", 0), ("startup_timeout", 301), ("startup_timeout", True),
    ("stop_timeout", "30"), ("stop_timeout", -1), ("network", "--privileged"),
    ("network", "container:foreign"), ("backend", "kubernetes"), ("initial_offset", "none"),
])
def test_runtime_has_closed_bounded_configuration(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        KafkaStreamsConfig.model_validate({"image": IMAGE, field: value})


@pytest.mark.parametrize("sql", [
    "SELECT * FROM orders", "SELECT id FROM orders LIMIT 1", "SELECT DISTINCT id FROM orders",
    "SELECT COUNT(id) FROM orders", "SELECT id FROM orders GROUP BY id", "SELECT id FROM orders ORDER BY id",
    "SELECT id FROM orders UNION SELECT id FROM orders", "SELECT id FROM orders, other",
    "SELECT id FROM orders JOIN other ON orders.id = other.id", "SELECT id + 1 FROM orders",
    "SELECT UPPER(id) FROM orders", "SELECT CAST(amount AS STRING) FROM orders",
    "SELECT id FROM orders WHERE active = TRUE OR amount > 1", "SELECT id FROM orders WHERE NOT active",
    "SELECT id FROM orders WHERE amount IN (1, 2)", "SELECT id FROM orders WHERE amount BETWEEN 1 AND 2",
    "SELECT id FROM orders WHERE id LIKE 'a%'", "SELECT id FROM orders WHERE amount = NULL",
    "SELECT id FROM orders WHERE amount = '1'", "SELECT id FROM orders WHERE active = 1",
    "SELECT id FROM orders WHERE id > 'a'", "SELECT id FROM orders WHERE amount = 1.5",
    "SELECT id FROM orders WHERE amount = 9223372036854775808", "SELECT id FROM orders WHERE amount = -9223372036854775809",
    "SELECT id FROM orders WHERE amount = amount", "SELECT other FROM orders", "SELECT id FROM unknown",
    "SELECT id FROM db.orders", "SELECT orders.id FROM orders", 'SELECT "id" FROM orders',
    "SELECT ID FROM orders", "SELECT id FROM orders o", "SELECT id, amount AS id FROM orders",
    "SELECT 1", "SELECT id FROM (SELECT id FROM orders)", "WITH o AS (SELECT id FROM orders) SELECT id FROM o",
    "DELETE FROM orders", "SELECT id FROM orders; SELECT id FROM orders", "SELECT id FROM orders -- comment",
    "SELECT id FROM orders WHERE (amount > 1)", "SELECT id FROM orders WHERE amount / 2 > 1",
])
def test_sql_subset_is_not_silently_widened(sql: str) -> None:
    schema = {item["name"]: {"type": item["type"], "nullable": not item.get("required", False)} for item in SOURCE_COLUMNS}
    with pytest.raises(KafkaStreamsPlanError):
        compile_plan(sql, schema, "orders", "input", "output")


@pytest.mark.parametrize(("field", "value"), [
    ("version", True), ("version", 2), ("predicates", [{"column": "amount", "op": "ge", "value": True}]),
    ("projection", [{"column": "missing", "as": "id"}]), ("input_topic", "valuable_orders"),
    ("extra", "ignored"),
])
def test_loaded_runner_plan_is_strictly_revalidated(field: str, value: object) -> None:
    _, manifest = _compile()
    job = copy.deepcopy(manifest.artifacts["kafka_streams_jobs"][0])
    job["plan"][field] = value
    with pytest.raises(KafkaStreamsArtifactFormatError):
        parse_compiled_kafka_streams_job_artifact(job)


@pytest.mark.parametrize(("field", "value"), [
    ("ownership", None), ("ownership", {"mode": "external"}), ("extra", "ignored"),
    ("application_id", "other-app"), ("image", "runner:latest"), ("initial_offset", "auto"),
])
def test_loaded_job_artifact_rejects_incomplete_or_ambiguous_identity(field: str, value: object) -> None:
    _, manifest = _compile()
    job = copy.deepcopy(manifest.artifacts["kafka_streams_jobs"][0])
    job[field] = value
    with pytest.raises(KafkaStreamsArtifactFormatError):
        parse_compiled_kafka_streams_job_artifact(job)


def test_plan_validation_defensively_copies_nested_mutable_fields() -> None:
    _, manifest = _compile()
    raw = manifest.artifacts["kafka_streams_jobs"][0]["plan"]
    copied = validate_plan(raw)
    copied["schema"]["id"]["nullable"] = True
    assert raw["schema"]["id"]["nullable"] is False
