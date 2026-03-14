"""TDD tests for P1: Data Contracts — column-level breaking change enforcement.

Tests are written before implementation and drive the design.
"""

import tempfile
from pathlib import Path

import yaml

from streamt.core.models import StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


def _parse(tmpdir: str, config: dict) -> StreamtProject:
    p = Path(tmpdir)
    (p / "stream_project.yml").write_text(yaml.dump(config))
    return ProjectParser(p).parse()


BASE = {
    "project": {"name": "test", "version": "1.0.0"},
    "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    "sources": [{"name": "raw", "topic": "raw.v1"}],
}


class TestContractParsing:
    """Contract fields parse correctly from YAML."""

    def test_model_accepts_contract_block(self):
        """A model can declare a contract with enforced + columns."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT id, amount FROM {{ source("raw") }}',
                        "contract": {
                            "enforced": True,
                            "columns": [
                                {"name": "id", "type": "BIGINT"},
                                {"name": "amount", "type": "DECIMAL(18,2)"},
                            ],
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            model = project.get_model("clean")
            assert model.contract is not None
            assert model.contract.enforced is True
            assert len(model.contract.columns) == 2
            assert model.contract.columns[0].name == "id"
            assert model.contract.columns[0].type == "BIGINT"

    def test_contract_defaults_to_none(self):
        """Model without contract block has contract=None."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {**BASE, "models": [{"name": "m", "sql": "SELECT 1"}]}
            project = _parse(d, cfg)
            assert project.get_model("m").contract is None

    def test_contract_enforced_defaults_to_true(self):
        """Contract block without enforced: defaults to enforced=True."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "m",
                        "sql": "SELECT 1",
                        "contract": {"columns": [{"name": "col", "type": "INT"}]},
                    }
                ],
            }
            project = _parse(d, cfg)
            assert project.get_model("m").contract.enforced is True

    def test_contract_column_nullable_field(self):
        """Contract column can declare nullable."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "m",
                        "sql": "SELECT 1",
                        "contract": {
                            "columns": [{"name": "id", "type": "BIGINT", "nullable": False}]
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            col = project.get_model("m").contract.columns[0]
            assert col.nullable is False


class TestContractValidation:
    """Validator enforces contract at compile time."""

    def test_contract_passes_when_types_match(self):
        """No error when declared types are compatible with SQL inference."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT id, name FROM {{ source("raw") }}',
                        "contract": {
                            "enforced": True,
                            "columns": [
                                {"name": "id", "type": "STRING"},
                                {"name": "name", "type": "STRING"},
                            ],
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            contract_errors = [e for e in result.errors if "CONTRACT" in e.code]
            assert contract_errors == [], contract_errors

    def test_contract_error_on_missing_column(self):
        """Error when contract declares a column not produced by SQL."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT id FROM {{ source("raw") }}',
                        "contract": {
                            "enforced": True,
                            "columns": [
                                {"name": "id", "type": "STRING"},
                                {"name": "missing_col", "type": "INT"},  # not in SQL
                            ],
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            contract_errors = [e for e in result.errors if "CONTRACT" in e.code]
            assert len(contract_errors) >= 1
            assert any("missing_col" in e.message for e in contract_errors)

    def test_contract_error_on_type_mismatch(self):
        """Error when declared type is incompatible with inferred SQL type."""
        with tempfile.TemporaryDirectory() as d:
            # SQL returns a literal BIGINT (CAST to be explicit), contract says STRING
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "clean",
                        "sql": "SELECT CAST(1 AS BIGINT) AS id",
                        "contract": {
                            "enforced": True,
                            "columns": [{"name": "id", "type": "STRING"}],  # wrong type
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            contract_errors = [e for e in result.errors if "CONTRACT" in e.code]
            assert len(contract_errors) >= 1
            assert any("id" in e.message for e in contract_errors)

    def test_contract_not_enforced_produces_warnings_not_errors(self):
        """enforced: false → type violations are warnings, not errors."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "clean",
                        "sql": "SELECT CAST(1 AS BIGINT) AS id",
                        "contract": {
                            "enforced": False,
                            "columns": [{"name": "id", "type": "STRING"}],
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert result.is_valid  # no errors
            contract_warnings = [w for w in result.warnings if "CONTRACT" in w.code]
            assert len(contract_warnings) >= 1

    def test_no_contract_no_contract_errors(self):
        """Models without contract: skip contract validation entirely."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {**BASE, "models": [{"name": "m", "sql": "SELECT CAST(1 AS BIGINT) AS x"}]}
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            assert not any("CONTRACT" in e.code for e in result.errors)

    def test_contract_type_normalization_string_variants(self):
        """STRING / VARCHAR / TEXT are equivalent in contract matching."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "clean",
                        "sql": 'SELECT name FROM {{ source("raw") }}',
                        "contract": {
                            "enforced": True,
                            "columns": [{"name": "name", "type": "VARCHAR"}],  # STRING inferred
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            contract_errors = [e for e in result.errors if "CONTRACT" in e.code]
            assert contract_errors == [], "VARCHAR and STRING should be compatible"

    def test_contract_skipped_when_sql_uninferrable(self):
        """If type inference fails entirely, contract validation is skipped (no false errors)."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "clean",
                        "sql": "SELECT * FROM {{ source('raw') }}",  # SELECT * can't be fully inferred
                        "contract": {
                            "enforced": True,
                            "columns": [{"name": "id", "type": "BIGINT"}],
                        },
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            # Should not produce CONTRACT errors for uninferrable SQL
            contract_errors = [e for e in result.errors if "CONTRACT_TYPE_MISMATCH" in e.code]
            assert contract_errors == []


class TestContractBreakingChangeDetection:
    """Validator warns when an exposure's declared consumption conflicts with contract."""

    def test_breaking_change_warning_when_exposure_consumes_removed_column(self):
        """Warning when exposure declares it consumes a column not in model contract."""
        with tempfile.TemporaryDirectory() as d:
            cfg = {
                **BASE,
                "models": [
                    {
                        "name": "payments_clean",
                        "sql": 'SELECT id FROM {{ source("raw") }}',
                        "contract": {
                            "enforced": True,
                            "columns": [{"name": "id", "type": "STRING"}],
                            # note: no 'amount' column in contract
                        },
                    }
                ],
                "exposures": [
                    {
                        "name": "fraud_service",
                        "type": "application",
                        "consumes": [{"ref": "payments_clean"}],
                        "columns": [
                            {"name": "id"},
                            {"name": "amount"},  # not in contract → breaking
                        ],
                    }
                ],
            }
            project = _parse(d, cfg)
            result = ProjectValidator(project).validate()
            breaking = [
                m
                for m in result.messages
                if "BREAKING" in m.code or "breaking" in m.message.lower()
            ]
            assert len(breaking) >= 1
