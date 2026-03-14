"""CLI integration tests for migration fixes.

Tests --strict, --select, --upstream/--downstream, show contract/assertions,
list summary, and Pydantic error wrapping through CLI commands.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from streamt.core.parser import ParseError, ProjectParser
from streamt.core.validator import ProjectValidator


def _parse_project(tmpdir: str, config: dict) -> StreamtProject:  # noqa: F821
    """Write, parse, and return a StreamtProject."""
    p = _write_project(tmpdir, config)
    parser = ProjectParser(p)
    return parser.parse()


def _write_project(tmpdir: str, config: dict) -> Path:
    """Write a stream_project.yml and return the directory Path."""
    p = Path(tmpdir)
    (p / "stream_project.yml").write_text(yaml.dump(config))
    return p


class TestStrictFlagBehavior:
    """VALIDATE-2: --strict causes exit 1 on warnings."""

    def test_strict_fails_on_warnings(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                },
            )
            r = runner.invoke(main, ["validate", "-p", d])
            assert r.exit_code == 0

            r2 = runner.invoke(main, ["validate", "-p", d, "--strict"])
            assert r2.exit_code == 1


class TestListSummary:
    """UX-4: `streamt list` with no arg shows summary."""

    def test_list_no_arg_shows_summary(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                },
            )
            r = runner.invoke(main, ["list", "-p", d])
            assert r.exit_code == 0
            assert "Sources" in r.output or "sources" in r.output.lower()

    def test_list_models_with_tag_filter(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": "a",
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                            "tags": ["payments"],
                        },
                        {
                            "name": "b",
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                            "tags": ["fraud"],
                        },
                    ],
                },
            )
            r = runner.invoke(main, ["list", "models", "-p", d, "--select", "tag:payments"])
            assert r.exit_code == 0
            assert "a" in r.output
            assert not any("│ b " in line or "│b " in line for line in r.output.splitlines())


class TestLineageUpstreamDownstream:
    """LINEAGE-1: --upstream / --downstream on lineage CLI."""

    def test_upstream_requires_model(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                },
            )
            r = runner.invoke(main, ["lineage", "-p", d, "--upstream"])
            assert r.exit_code != 0

    def test_upstream_shows_ancestors(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {"name": "a", "sql": 'SELECT * FROM {{ source("raw") }}'},
                        {"name": "b", "sql": 'SELECT * FROM {{ ref("a") }}'},
                    ],
                },
            )
            r = runner.invoke(main, ["lineage", "-p", d, "-m", "b", "--upstream"])
            assert r.exit_code == 0
            assert "raw" in r.output
            assert "a" in r.output


class TestShowContractInfo:
    """SHOW-1: show model includes contract data."""

    def test_show_model_contract_in_json(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": "clean",
                            "sql": 'SELECT id, amount FROM {{ source("raw") }}',
                            "contract": {
                                "enforce": True,
                                "columns": [
                                    {"name": "id", "type": "STRING"},
                                    {"name": "amount", "type": "DECIMAL(18,4)"},
                                ],
                            },
                        }
                    ],
                },
            )
            r = runner.invoke(main, ["-o", "json", "show", "model", "clean", "-p", d])
            assert r.exit_code == 0
            data = json.loads(r.output)
            assert "contract" in data.get("data", {})
            assert data["data"]["contract"]["enforced"] is True


class TestShowTestAssertions:
    """SHOW-3: show test includes assertion details."""

    def test_show_test_assertion_details(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "models": [{"name": "clean", "sql": "SELECT 1"}],
                    "tests": [
                        {
                            "name": "not_null_check",
                            "model": "clean",
                            "type": "continuous",
                            "assertions": [{"not_null": {"columns": ["id", "amount"]}}],
                        }
                    ],
                },
            )
            r = runner.invoke(main, ["show", "test", "not_null_check", "-p", d])
            assert r.exit_code == 0
            assert "not_null" in r.output


class TestPydanticErrorWrappingFull:
    """VALIDATE-3: all parse methods wrap Pydantic errors."""

    def test_invalid_exposure_type_raises_parse_error(self):
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "exposures": [{"name": "app", "type": "invalid_type"}],
                },
            )
            parser = ProjectParser(Path(d))
            with pytest.raises(ParseError):
                parser.parse()

    def test_invalid_test_type_raises_parse_error(self):
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "models": [{"name": "clean", "sql": "SELECT 1"}],
                    "tests": [{"name": "t1", "model": "clean", "type": "nonexistent"}],
                },
            )
            parser = ProjectParser(Path(d))
            with pytest.raises(ParseError):
                parser.parse()


class TestExposureOwnersViaParser:
    """owners list survives YAML round-trip through the parser."""

    def test_owners_populated_from_yaml(self):
        with tempfile.TemporaryDirectory() as d:
            project = _parse_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "exposures": [
                        {
                            "name": "svc",
                            "type": "application",
                            "owners": [{"name": "data-team", "email": "data@co.com"}],
                        }
                    ],
                },
            )
            assert project.exposures[0].owner == "data-team"


class TestValidatorWithParsedProject:
    """Integration: run validator against parsed project (has project_path)."""

    def test_valid_project_passes(self):
        with tempfile.TemporaryDirectory() as d:
            project = _parse_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {
                            "name": "clean",
                            "sql": 'SELECT * FROM {{ source("raw") }}',
                        }
                    ],
                },
            )
            validator = ProjectValidator(project)
            result = validator.validate()
            assert result.is_valid

    def test_missing_source_ref_fails(self):
        with tempfile.TemporaryDirectory() as d:
            project = _parse_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "models": [
                        {
                            "name": "clean",
                            "sql": 'SELECT * FROM {{ source("missing") }}',
                        }
                    ],
                },
            )
            validator = ProjectValidator(project)
            result = validator.validate()
            assert not result.is_valid
            assert any(m.code == "SOURCE_NOT_FOUND" for m in result.errors)


# ===========================================================================
# Tier 1 deferred items
# ===========================================================================


class TestShowCompiledSQL:
    """SHOW-2: show model displays compiled (Jinja-resolved) SQL."""

    def test_compiled_sql_resolves_source(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "events.raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT id FROM {{ source("raw") }}'}],
                },
            )
            r = runner.invoke(main, ["show", "model", "clean", "-p", d])
            assert r.exit_code == 0
            assert "events.raw.v1" in r.output

    def test_compiled_sql_in_json(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "events.raw.v1"}],
                    "models": [{"name": "clean", "sql": 'SELECT id FROM {{ source("raw") }}'}],
                },
            )
            r = runner.invoke(main, ["-o", "json", "show", "model", "clean", "-p", d])
            assert r.exit_code == 0
            data = json.loads(r.output)
            assert "events.raw.v1" in data["data"]["compiled_sql"]


class TestSelectStarWarning:
    """COMPILE-5: validator warns on SELECT * in models."""

    def test_select_star_produces_warning(self):
        with tempfile.TemporaryDirectory() as d:
            project = _parse_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "wide", "sql": 'SELECT * FROM {{ source("raw") }}'}],
                },
            )
            result = ProjectValidator(project).validate()
            assert any(m.code == "SELECT_STAR" for m in result.warnings)

    def test_explicit_columns_no_warning(self):
        with tempfile.TemporaryDirectory() as d:
            project = _parse_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {"name": "narrow", "sql": 'SELECT id, name FROM {{ source("raw") }}'}
                    ],
                },
            )
            result = ProjectValidator(project).validate()
            assert not any(m.code == "SELECT_STAR" for m in result.warnings)


class TestQuietFlag:
    """JSON-3: --quiet suppresses non-error output."""

    def test_quiet_suppresses_output(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [{"name": "a", "sql": 'SELECT id FROM {{ source("raw") }}'}],
                },
            )
            r = runner.invoke(main, ["-q", "validate", "-p", d])
            assert r.exit_code == 0
            # Quiet mode: no table, no "is valid" — output should be minimal
            assert "Project Summary" not in r.output


class TestVerboseFlag:
    """UX-5: --verbose enables debug logging."""

    def test_verbose_flag_accepted(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                },
            )
            r = runner.invoke(main, ["-v", "validate", "-p", d])
            assert r.exit_code == 0


class TestTestCoverage:
    """TEST-3: test --coverage shows model coverage report."""

    def test_coverage_report(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {"name": "a", "sql": 'SELECT id FROM {{ source("raw") }}'},
                        {"name": "b", "sql": 'SELECT id FROM {{ source("raw") }}'},
                    ],
                    "tests": [
                        {
                            "name": "t1",
                            "model": "a",
                            "type": "schema",
                            "assertions": [{"row_count": {"min": 1}}],
                        }
                    ],
                },
            )
            r = runner.invoke(main, ["test", "--coverage", "-p", d])
            assert r.exit_code == 0
            assert "1/2" in r.output or "50%" in r.output

    def test_coverage_json(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                    "sources": [{"name": "raw", "topic": "raw.v1"}],
                    "models": [
                        {"name": "a", "sql": 'SELECT id FROM {{ source("raw") }}'},
                    ],
                    "tests": [
                        {
                            "name": "t1",
                            "model": "a",
                            "type": "schema",
                            "assertions": [{"row_count": {"min": 1}}],
                        }
                    ],
                },
            )
            r = runner.invoke(main, ["-o", "json", "test", "--coverage", "-p", d])
            assert r.exit_code == 0
            data = json.loads(r.output)
            assert data["data"]["percent"] == 100


class TestWarningsInJSON:
    """JSON-1: all commands include warnings array in JSON output."""

    def test_warnings_always_present_in_json(self):
        from click.testing import CliRunner

        from streamt.cli import main

        runner = CliRunner()
        with tempfile.TemporaryDirectory() as d:
            _write_project(
                d,
                {
                    "project": {"name": "test", "version": "1.0.0"},
                    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
                },
            )
            r = runner.invoke(main, ["-o", "json", "validate", "-p", d])
            assert r.exit_code == 0
            data = json.loads(r.output)
            assert "warnings" in data
            assert isinstance(data["warnings"], list)

    def test_print_warning_captured_in_json(self):
        """Warnings from print_warning() auto-captured in JSON envelope."""
        from streamt.output import OutputFormatter

        fmt = OutputFormatter("json")
        fmt.set_command("test")
        fmt.print_warning("something is off")
        result = fmt.get_result()
        assert len(result.warnings) == 1
        assert result.warnings[0].message == "something is off"
