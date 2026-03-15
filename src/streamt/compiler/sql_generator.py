"""SQL generation mixin for streamt compiler."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Optional

from streamt.compiler.flink_ddl import kafka_with_properties
from streamt.compiler.masking import apply_masking_to_sql
from streamt.core.models import (
    DataTest,
    EventTimeConfig,
    Model,
    Source,
    WatermarkStrategy,
)

if TYPE_CHECKING:
    pass


class SQLGeneratorMixin:
    """Mixin providing SQL generation methods for the Compiler."""

    def _get_type_cast_expression(self, user_type: str) -> str:
        """Map user-friendly type names to Flink SQL type expressions."""
        type_mapping = {
            "string": "STRING",
            "str": "STRING",
            "text": "STRING",
            "number": "DOUBLE",
            "numeric": "DOUBLE",
            "double": "DOUBLE",
            "float": "DOUBLE",
            "int": "INT",
            "integer": "INT",
            "bigint": "BIGINT",
            "long": "BIGINT",
            "boolean": "BOOLEAN",
            "bool": "BOOLEAN",
            "timestamp": "TIMESTAMP(3)",
            "datetime": "TIMESTAMP(3)",
            "date": "DATE",
            "time": "TIME",
        }
        return type_mapping.get(user_type.lower(), "")

    def _generate_test_flink_sql(
        self, test: DataTest, source_topic: str, columns: list[str]
    ) -> str:
        """Generate Flink SQL for a continuous test."""
        bootstrap = self._get_flink_bootstrap_servers()  # type: ignore[attr-defined]
        sql_parts = []

        # Generate column DDL
        if columns:
            columns_ddl = ",\n    ".join(f"`{col}` STRING" for col in columns)
        else:
            columns_ddl = "`_raw` STRING"

        # Source table (model output)
        kafka = self.project.runtime.kafka  # type: ignore[attr-defined]
        src_with = kafka_with_properties(
            kafka, source_topic, bootstrap, {"scan.startup.mode": "latest-offset"}
        )
        sql_parts.append(
            f"CREATE TABLE IF NOT EXISTS test_source_{test.name} (\n    {columns_ddl}\n) {src_with};"
        )

        # Failures sink table
        fail_cols = "`test_name` STRING,\n    `violation_type` STRING,\n    `violation_details` STRING,\n    `record` STRING,\n    `detected_at` TIMESTAMP(3)"
        fail_with = kafka_with_properties(kafka, "_streamt_test_failures", bootstrap)
        sql_parts.append(
            f"CREATE TABLE IF NOT EXISTS test_failures_{test.name} (\n    {fail_cols}\n) {fail_with};"
        )

        # Build WHERE clause from assertions
        violation_conditions: list[tuple[str, str, str]] = []
        for assertion in test.assertions:
            assertion_type = list(assertion.keys())[0]
            config = assertion[assertion_type]

            if assertion_type == "not_null":
                for col in config.get("columns", []):
                    if col in columns:
                        violation_conditions.append((f"`{col}` IS NULL", f"not_null:{col}", col))

            elif assertion_type == "accepted_values":
                col = config.get("column")
                values = config.get("values", [])
                if col and col in columns and values:
                    values_str = ", ".join(f"'{v}'" for v in values)
                    violation_conditions.append(
                        (f"`{col}` NOT IN ({values_str})", f"accepted_values:{col}", col)
                    )

            elif assertion_type == "range":
                col = config.get("column")
                min_val = config.get("min")
                max_val = config.get("max")
                if col and col in columns:
                    if min_val is not None:
                        violation_conditions.append(
                            (f"CAST(`{col}` AS DOUBLE) < {min_val}", f"range_min:{col}", col)
                        )
                    if max_val is not None:
                        violation_conditions.append(
                            (f"CAST(`{col}` AS DOUBLE) > {max_val}", f"range_max:{col}", col)
                        )

            elif assertion_type == "accepted_types":
                types = config.get("types", {})
                for col, expected_type in types.items():
                    if col in columns:
                        type_cast = self._get_type_cast_expression(expected_type)
                        if type_cast:
                            violation_conditions.append(
                                (
                                    f"TRY_CAST(`{col}` AS {type_cast}) IS NULL AND `{col}` IS NOT NULL",
                                    f"accepted_types:{col}",
                                    col,
                                )
                            )

            elif assertion_type == "custom_sql":
                name = config.get("name", "custom")
                where_clause = config.get("where")
                detail_column = config.get("detail_column", columns[0] if columns else "_raw")

                if where_clause and detail_column in columns:
                    violation_conditions.append((where_clause, f"custom_sql:{name}", detail_column))

        # Generate INSERT statement for each violation type
        if violation_conditions:
            union_parts = []
            for condition, violation_type, col_name in violation_conditions:
                union_parts.append(f"""SELECT
    '{test.name}' AS test_name,
    '{violation_type}' AS violation_type,
    CAST(`{col_name}` AS STRING) AS violation_details,
    '' AS record,
    CURRENT_TIMESTAMP AS detected_at
FROM test_source_{test.name}
WHERE {condition}""")

            sql_parts.append(
                f"INSERT INTO test_failures_{test.name}\n" + "\nUNION ALL\n".join(union_parts) + ";"
            )

        return "\n\n".join(sql_parts)

    def _generate_flink_sql(self, model: Model, output_topic: str) -> str:
        """Generate Flink SQL for a model."""
        sql_parts = []

        set_statements = self._generate_flink_set_statements(model)
        if set_statements:
            sql_parts.append(set_statements)

        dependencies = self._get_model_dependencies(model)

        for dep_name, dep_type in dependencies:
            if dep_type == "source":
                source = self.project.get_source(dep_name)  # type: ignore[attr-defined]
                if source:
                    sql_parts.append(self._generate_source_table_ddl(source, dep_name))
            else:
                dep_model = self.project.get_model(dep_name)  # type: ignore[attr-defined]
                if dep_model:
                    topic_name = (
                        dep_model.get_topic_config().name
                        if dep_model.get_topic_config() and dep_model.get_topic_config().name
                        else dep_model.name
                    )
                    sql_parts.append(
                        self._generate_model_table_ddl(dep_model, dep_name, topic_name)
                    )

        sql_parts.append(self._generate_sink_table_ddl(model, output_topic))

        transformed_sql = self._transform_sql(model.sql or "")

        if model.security and model.security.policies:
            schema = self._build_source_schema(model)
            masks = []
            for policy in model.security.policies:
                if "mask" in policy:
                    mask_config = policy["mask"]
                    masks.append({"column": mask_config["column"], "method": mask_config["method"]})
            if masks:
                transformed_sql = apply_masking_to_sql(transformed_sql, masks, schema)

        sink_table = self._topic_to_table_name(output_topic)
        sql_parts.append(f"INSERT INTO {sink_table}\n{transformed_sql};")

        return "\n\n".join(sql_parts)

    def _get_flink_bootstrap_servers(self) -> str:
        """Get bootstrap servers for Flink (internal if available)."""
        kafka_config = self.project.runtime.kafka  # type: ignore[attr-defined]
        return kafka_config.bootstrap_servers_internal or kafka_config.bootstrap_servers

    def _generate_source_table_ddl(self, source: Source, alias: str) -> str:
        """Generate Flink CREATE TABLE DDL for a source."""
        bootstrap = self._get_flink_bootstrap_servers()

        column_lines = []
        if source.columns:
            for col in source.columns:
                if col.proctime:
                    column_lines.append(f"`{col.name}` AS PROCTIME()")
                elif source.event_time and col.name == source.event_time.column:
                    column_lines.append(f"`{col.name}` TIMESTAMP(3)")
                elif col.type:
                    column_lines.append(f"`{col.name}` {col.type}")
                else:
                    column_lines.append(f"`{col.name}` STRING")
        else:
            column_lines.append("`_raw` STRING")

        if source.event_time:
            watermark_ddl = self._generate_watermark_ddl(source.event_time)
            if watermark_ddl:
                column_lines.append(watermark_ddl)

        columns = ",\n    ".join(column_lines)

        kafka = self.project.runtime.kafka  # type: ignore[attr-defined]
        with_clause = kafka_with_properties(
            kafka, source.topic, bootstrap, {"scan.startup.mode": "earliest-offset"}
        )
        return f"CREATE TABLE IF NOT EXISTS {alias} (\n    {columns}\n) {with_clause};"

    def _generate_flink_set_statements(self, model: Model) -> str:
        """Generate SET statements for Flink job configuration."""
        fc = model.get_flink_config()
        if not fc:
            return ""
        stmts: list[str] = []

        def _set(key: str, val: object) -> None:
            stmts.append(f"SET '{key}' = '{val}';")

        if fc.parallelism:
            _set("parallelism.default", fc.parallelism)
        if fc.state_ttl_ms:
            ms = fc.state_ttl_ms
            if ms >= 3600000 and ms % 3600000 == 0:
                ttl = f"{ms // 3600000} h"
            elif ms >= 60000 and ms % 60000 == 0:
                ttl = f"{ms // 60000} min"
            elif ms >= 1000 and ms % 1000 == 0:
                ttl = f"{ms // 1000} s"
            else:
                ttl = f"{ms} ms"
            _set("table.exec.state.ttl", ttl)
        if fc.state_backend:
            _set("state.backend", fc.state_backend)
        if fc.checkpoint_interval_ms:
            _set("execution.checkpointing.interval", f"{fc.checkpoint_interval_ms}ms")

        # Advanced checkpointing
        if fc.checkpoint:
            cp = fc.checkpoint
            if cp.timeout_ms is not None:
                _set("execution.checkpointing.timeout", f"{cp.timeout_ms}ms")
            if cp.min_pause_ms is not None:
                _set("execution.checkpointing.min-pause", f"{cp.min_pause_ms}ms")
            if cp.max_concurrent is not None:
                _set("execution.checkpointing.max-concurrent-checkpoints", cp.max_concurrent)
            if cp.mode:
                _set("execution.checkpointing.mode", cp.mode)
            if cp.externalized:
                _set("execution.checkpointing.externalized-checkpoint-retention", cp.externalized)
            if cp.unaligned is not None:
                _set("execution.checkpointing.unaligned.enabled", str(cp.unaligned).lower())
            if cp.incremental is not None:
                _set("state.backend.incremental", str(cp.incremental).lower())

        # RocksDB tuning
        if fc.rocksdb:
            rb = fc.rocksdb
            if rb.block_cache_size_mb is not None:
                _set("state.backend.rocksdb.block.cache-size", f"{rb.block_cache_size_mb}mb")
            if rb.write_buffer_size_mb is not None:
                _set("state.backend.rocksdb.writebuffer.size", f"{rb.write_buffer_size_mb}mb")
            if rb.predefined_options:
                _set("state.backend.rocksdb.predefined-options", rb.predefined_options)

        # Resource configuration
        if fc.resources:
            res = fc.resources
            if res.taskmanager_memory_mb is not None:
                _set("taskmanager.memory.process.size", f"{res.taskmanager_memory_mb}mb")
            if res.taskmanager_slots is not None:
                _set("taskmanager.numberOfTaskSlots", res.taskmanager_slots)
            if res.jobmanager_memory_mb is not None:
                _set("jobmanager.memory.process.size", f"{res.jobmanager_memory_mb}mb")

        # Restart strategy
        if fc.restart_strategy:
            rs = fc.restart_strategy
            _set("restart-strategy.type", rs.type)
            if rs.type == "fixed-delay":
                if rs.attempts is not None:
                    _set("restart-strategy.fixed-delay.attempts", rs.attempts)
                if rs.delay_ms is not None:
                    _set("restart-strategy.fixed-delay.delay", f"{rs.delay_ms}ms")
            elif rs.type == "failure-rate":
                if rs.max_failures_per_interval is not None:
                    _set(
                        "restart-strategy.failure-rate.max-failures-per-interval",
                        rs.max_failures_per_interval,
                    )
                if rs.failure_rate_interval_ms is not None:
                    _set(
                        "restart-strategy.failure-rate.failure-rate-interval",
                        f"{rs.failure_rate_interval_ms}ms",
                    )
                if rs.delay_ms is not None:
                    _set("restart-strategy.failure-rate.delay", f"{rs.delay_ms}ms")
            elif rs.type == "exponential-delay":
                if rs.initial_backoff_ms is not None:
                    _set(
                        "restart-strategy.exponential-delay.initial-backoff",
                        f"{rs.initial_backoff_ms}ms",
                    )
                if rs.max_backoff_ms is not None:
                    _set("restart-strategy.exponential-delay.max-backoff", f"{rs.max_backoff_ms}ms")
                if rs.backoff_multiplier is not None:
                    _set(
                        "restart-strategy.exponential-delay.backoff-multiplier",
                        rs.backoff_multiplier,
                    )

        return "\n".join(stmts)

    def _generate_watermark_ddl(self, event_time: EventTimeConfig) -> str:
        """Generate watermark DDL clause for event time configuration."""
        column = event_time.column

        if event_time.watermark:
            if event_time.watermark.strategy == WatermarkStrategy.CUSTOM:
                expr = event_time.watermark.expression or f"`{column}`"
                return f"WATERMARK FOR `{column}` AS {expr}"
            elif event_time.watermark.strategy == WatermarkStrategy.MONOTONOUSLY_INCREASING:
                return f"WATERMARK FOR `{column}` AS `{column}`"
            else:
                delay_ms = event_time.watermark.max_out_of_orderness_ms or 5000
                delay_seconds = delay_ms / 1000
                return f"WATERMARK FOR `{column}` AS `{column}` - INTERVAL '{int(delay_seconds)}' SECOND"
        else:
            return f"WATERMARK FOR `{column}` AS `{column}` - INTERVAL '5' SECOND"

    def _generate_model_table_ddl(self, model: Model, alias: str, topic_name: str) -> str:
        """Generate Flink CREATE TABLE DDL for a model reference."""
        bootstrap = self._get_flink_bootstrap_servers()

        columns_with_types = self._extract_select_columns_with_types(model.sql or "", model=model)  # type: ignore[attr-defined]
        if columns_with_types:
            columns_ddl = ",\n    ".join(
                f"`{col}` {col_type}" for col, col_type in columns_with_types
            )
        else:
            columns_ddl = "`_raw` STRING"

        kafka = self.project.runtime.kafka  # type: ignore[attr-defined]
        with_clause = kafka_with_properties(
            kafka, topic_name, bootstrap, {"scan.startup.mode": "earliest-offset"}
        )
        return f"CREATE TABLE IF NOT EXISTS {alias} (\n    {columns_ddl}\n) {with_clause};"

    def _topic_to_table_name(self, topic_name: str) -> str:
        """Convert topic name to valid Flink SQL table name."""
        return re.sub(r"[.\-]", "_", topic_name) + "_sink"

    def _generate_sink_table_ddl(self, model: Model, topic_name: str) -> str:
        """Generate Flink CREATE TABLE DDL for the output sink."""
        bootstrap = self._get_flink_bootstrap_servers()
        table_name = self._topic_to_table_name(topic_name)

        columns_with_types = self._extract_select_columns_with_types(model.sql or "", model=model)  # type: ignore[attr-defined]
        if columns_with_types:
            columns_ddl = ",\n    ".join(
                f"`{col}` {col_type}" for col, col_type in columns_with_types
            )
        else:
            columns_ddl = "`_raw` STRING"

        if model.primary_key and columns_with_types:
            pk_cols = ", ".join(f"`{k}`" for k in model.primary_key)
            columns_ddl += f",\n    PRIMARY KEY ({pk_cols}) NOT ENFORCED"

        kafka = self.project.runtime.kafka  # type: ignore[attr-defined]
        fc = model.get_flink_config()
        extra: dict[str, str] = {}
        connector = "kafka"
        if fc and fc.changelog_mode == "upsert":
            connector = "upsert-kafka"
            extra["key.format"] = "json"
        with_clause = kafka_with_properties(
            kafka, topic_name, bootstrap, extra, connector=connector
        )
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_ddl}\n) {with_clause};"

    def _extract_select_columns(self, sql: str) -> list[str]:
        """Extract column names from SELECT clause."""
        return [col for col, _ in self._extract_select_columns_with_types(sql)]  # type: ignore[attr-defined]

    def _build_source_schema(self, model: Model) -> dict[str, str]:
        """Build a schema dictionary from model dependencies."""
        schema: dict[str, str] = {}
        dependencies = self._get_model_dependencies(model)

        for dep_name, dep_type in dependencies:
            if dep_type == "source":
                source = self.project.get_source(dep_name)  # type: ignore[attr-defined]
                if source and source.columns:
                    for col in source.columns:
                        if col.proctime:
                            col_type = "TIMESTAMP_LTZ(3)"
                        elif source.event_time and col.name == source.event_time.column:
                            col_type = "TIMESTAMP(3)"
                        elif col.type:
                            col_type = col.type
                        else:
                            col_type = "STRING"
                        schema[col.name] = col_type
                        schema[f"{dep_name}.{col.name}"] = col_type
            else:
                dep_model = self.project.get_model(dep_name)  # type: ignore[attr-defined]
                if dep_model and dep_model.sql:
                    upstream_schema = self._build_source_schema(dep_model)
                    dep_columns = self._extract_select_columns_with_types(  # type: ignore[attr-defined]
                        dep_model.sql, schema_context=upstream_schema
                    )
                    for col_name, col_type in dep_columns:
                        schema[col_name] = col_type

        return schema

    def _transform_sql(self, sql: str) -> str:
        """Transform Jinja SQL to plain SQL."""
        sql = re.sub(
            r'\{\{\s*source\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}',
            r"\1",
            sql,
        )
        sql = re.sub(
            r'\{\{\s*ref\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}',
            r"\1",
            sql,
        )
        return sql.strip()

    def _get_source_topic(self, model: Model) -> Optional[str]:
        """Get the source topic for a model."""
        if model.sql and self.parser:  # type: ignore[attr-defined]
            sources, refs = self.parser.extract_refs_from_sql(model.sql)  # type: ignore[attr-defined]
            if sources:
                source = self.project.get_source(sources[0])  # type: ignore[attr-defined]
                if source:
                    return source.topic
            if refs:
                ref_model = self.project.get_model(refs[0])  # type: ignore[attr-defined]
                if ref_model:
                    return (
                        ref_model.get_topic_config().name
                        if ref_model.get_topic_config() and ref_model.get_topic_config().name
                        else ref_model.name
                    )
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    source = self.project.get_source(from_ref.source)  # type: ignore[attr-defined]
                    if source:
                        return source.topic
                if from_ref.ref:
                    ref_model = self.project.get_model(from_ref.ref)  # type: ignore[attr-defined]
                    if ref_model:
                        return (
                            ref_model.get_topic_config().name
                            if ref_model.get_topic_config() and ref_model.get_topic_config().name
                            else ref_model.name
                        )
        return None

    def _get_source_topics(self, model: Model) -> list[str]:
        """Get all source topics for a model."""
        topics = []

        if model.sql and self.parser:  # type: ignore[attr-defined]
            sources, refs = self.parser.extract_refs_from_sql(model.sql)  # type: ignore[attr-defined]
            for source_name in sources:
                source = self.project.get_source(source_name)  # type: ignore[attr-defined]
                if source:
                    topics.append(source.topic)
            for ref_name in refs:
                ref_model = self.project.get_model(ref_name)  # type: ignore[attr-defined]
                if ref_model:
                    topics.append(
                        ref_model.get_topic_config().name
                        if ref_model.get_topic_config() and ref_model.get_topic_config().name
                        else ref_model.name
                    )
        elif model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    source = self.project.get_source(from_ref.source)  # type: ignore[attr-defined]
                    if source:
                        topics.append(source.topic)
                if from_ref.ref:
                    ref_model = self.project.get_model(from_ref.ref)  # type: ignore[attr-defined]
                    if ref_model:
                        topics.append(
                            ref_model.get_topic_config().name
                            if ref_model.get_topic_config() and ref_model.get_topic_config().name
                            else ref_model.name
                        )

        return topics

    def _get_model_dependencies(self, model: Model) -> list[tuple[str, str]]:
        """Get model dependencies as (name, type) tuples."""
        dependencies = []

        if model.sql:
            if self.parser:  # type: ignore[attr-defined]
                sources, refs = self.parser.extract_refs_from_sql(model.sql)  # type: ignore[attr-defined]
            else:
                sources, refs = self._extract_refs_from_sql(model.sql)

            for source_name in sources:
                dependencies.append((source_name, "source"))
            for ref_name in refs:
                dependencies.append((ref_name, "model"))

        if model.from_:
            for from_ref in model.from_:
                if from_ref.source:
                    dependencies.append((from_ref.source, "source"))
                if from_ref.ref:
                    dependencies.append((from_ref.ref, "model"))

        return dependencies

    def _extract_refs_from_sql(self, sql: str) -> tuple[list[str], list[str]]:
        """Extract source and ref names from SQL using regex (fallback)."""
        sources = []
        refs = []

        source_pattern = r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(source_pattern, sql):
            sources.append(match.group(1))

        ref_pattern = r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}"
        for match in re.finditer(ref_pattern, sql):
            refs.append(match.group(1))

        return sources, refs

    def _extract_where_clause(self, sql: str) -> Optional[str]:
        """Extract WHERE clause from SQL."""
        match = re.search(
            r"WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)", sql, re.IGNORECASE | re.DOTALL
        )
        if match:
            return match.group(1).strip()
        return None
