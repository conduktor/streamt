"""TestJobCompiler: compiles continuous test assertions into Flink monitoring jobs."""

from __future__ import annotations

from streamt.compiler.manifest import FlinkJobArtifact


class TestJobCompiler:
    """Compiles continuous test assertions into Flink SQL monitoring jobs.

    Generates INSERT jobs that filter assertion violations into
    the __streamt_test_failures__ topic for continuous monitoring.
    """

    @staticmethod
    def assertion_to_where_clause(assertion: dict) -> str:
        """Convert a single assertion dict to a SQL WHERE clause fragment.

        The fragment represents the violation condition (rows that FAIL the test).
        """
        if len(assertion) != 1:
            raise ValueError(f"Assertion must have exactly one key, got: {list(assertion.keys())}")

        assertion_type, config = next(iter(assertion.items()))

        if assertion_type == "not_null":
            columns = config.get("columns", [])
            if not columns:
                raise ValueError("not_null assertion requires at least one column")
            return " OR ".join(f"{col} IS NULL" for col in columns)

        if assertion_type == "range":
            col = config.get("column")
            if not col:
                raise ValueError("range assertion requires 'column'")
            min_val = config.get("min")
            max_val = config.get("max")
            conditions = []
            if min_val is not None:
                conditions.append(f"{col} < {min_val}")
            if max_val is not None:
                conditions.append(f"{col} > {max_val}")
            if not conditions:
                raise ValueError("range assertion requires at least 'min' or 'max'")
            return " OR ".join(conditions)

        raise ValueError(f"Unknown assertion type: {assertion_type!r}")

    @classmethod
    def assertions_to_where_clause(cls, assertions: list[dict]) -> str:
        """Convert a list of assertions to a combined SQL WHERE clause."""
        parts = [cls.assertion_to_where_clause(a) for a in assertions]
        return " OR ".join(parts)

    @classmethod
    def compile_job(cls, test) -> FlinkJobArtifact | None:
        """Compile a continuous test to a FlinkJobArtifact, or None if no assertions.

        Only ``not_null`` and ``range`` assertion types are supported; others are
        silently skipped. Returns None if no supported assertions remain.
        """
        if not test.assertions:
            return None

        supported_types = {"not_null", "range"}
        supported = [
            a for a in test.assertions if len(a) == 1 and list(a.keys())[0] in supported_types
        ]
        if not supported:
            return None

        where_clause = cls.assertions_to_where_clause(supported)

        sql = (
            f"INSERT INTO `__streamt_test_failures__`\n"
            f"SELECT *, CURRENT_TIMESTAMP AS detected_at, '{test.name}' AS test_name\n"
            f"FROM `{test.model}`\n"
            f"WHERE {where_clause};"
        )

        return FlinkJobArtifact(
            name=f"test_{test.name}",
            sql=sql,
            cluster=getattr(test, "flink_cluster", None),
        )
