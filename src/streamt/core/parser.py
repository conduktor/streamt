"""Parser for streamt project files."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path

import yaml
from jinja2 import BaseLoader, Environment, TemplateSyntaxError
from pydantic import ValidationError

from streamt.core.environment import (
    EnvironmentConfig,
    EnvironmentManager,
)
from streamt.core.models import (
    CURRENT_API_VERSION,
    ConnectionConfig,
    DataTest,
    Defaults,
    Exposure,
    Model,
    ProjectInfo,
    Rules,
    RuntimeConfig,
    Source,
    StreamtProject,
    UDFDeclaration,
)


class EnvVarError(Exception):
    """Error when environment variable is not set."""

    pass


class ParseError(Exception):
    """Error during parsing."""

    pass


class JinjaError(Exception):
    """Error in Jinja template."""

    pass


def _format_pydantic_error(exc: ValidationError) -> str:
    """Format a Pydantic ValidationError into a user-friendly message."""
    parts = []
    for err in exc.errors():
        loc = " → ".join(str(part) for part in err["loc"]) if err["loc"] else "root"
        parts.append(f"field '{loc}': {err['msg']}")
    return "; ".join(parts)


class ProjectParser:
    """Parser for streamt projects."""

    # Matches ${VAR} and ${VAR:-default}
    ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-((?:[^}]|\\})*)?)?\}")

    def __init__(
        self,
        project_path: Path,
        environment: str | None = None,
        warn_callback: Callable[[str], None] | None = None,
    ) -> None:
        """Initialize parser with project path and optional environment.

        Args:
            project_path: Path to the project directory.
            environment: Environment name (for multi-env mode). If None, uses
                        STREAMT_ENV env var or fails if in multi-env mode.
            warn_callback: Optional callback for warnings (e.g., console.print).
        """
        self.project_path = project_path.resolve()
        self.environment = environment
        self.warn_callback = warn_callback or (lambda _: None)

        # Environment manager for multi-env support
        self.env_manager = EnvironmentManager(self.project_path)
        self.env_config: EnvironmentConfig | None = None
        self.warnings: list[str] = []

        # Jinja environment for SQL parsing
        self.jinja_env = Environment(loader=BaseLoader())

    def _setup_environment(self) -> None:
        """Setup environment variables and load environment config."""
        # Resolve environment (handles mode detection, env var loading, etc.)
        self.env_config, self.warnings = self.env_manager.resolve_environment(
            self.environment,
        )

        # Emit warnings
        for warning in self.warnings:
            self.warn_callback(f"[yellow]WARNING[/yellow]: {warning}")

    def parse(self) -> StreamtProject:
        """Parse the entire project."""
        # Setup environment first (loads .env files, resolves env config)
        self._setup_environment()

        # Find and parse stream_project.yml
        project_file = self._find_project_file()
        if not project_file:
            raise ParseError(f"No stream_project.yml found in {self.project_path}")

        project_data = self._load_yaml(project_file)
        self._validate_top_level_keys(project_data)
        api_version = self._parse_api_version(project_data)

        # Check for runtime: in project file when in multi-env mode
        runtime_warning = self.env_manager.check_project_runtime_warning(project_data)
        if runtime_warning:
            self.warn_callback(f"[yellow]WARNING[/yellow]: {runtime_warning}")

        # ENV-1: Warn about environments: key in project file
        if "environments" in project_data:
            self.warn_callback(
                "[yellow]WARNING[/yellow]: The 'environments:' key in stream_project.yml "
                "is not used. Environments are configured in the environments/ directory. "
                "See: https://streamt.dev/docs/reference/environments"
            )

        # Parse project info and runtime
        project_info = self._parse_project_info(project_data)
        runtime = self._parse_runtime(project_data)
        defaults = self._parse_defaults(project_data)
        rules = self._parse_rules(project_data)

        # Parse sources, models, tests, exposures
        sources = self._parse_sources(project_data)
        models = self._parse_models(project_data)
        tests = self._parse_tests(project_data)
        exposures = self._parse_exposures(project_data)
        udfs = self._parse_udfs(project_data)

        connections = self._parse_connections(project_data)

        return StreamtProject(
            apiVersion=api_version,
            project=project_info,
            runtime=runtime,
            defaults=defaults,
            rules=rules,
            connections=connections,
            sources=sources,
            models=models,
            tests=tests,
            exposures=exposures,
            udfs=udfs,
            project_path=self.project_path,
        )

    def _validate_top_level_keys(self, data: dict[str, object]) -> None:
        """Reject unknown root keys before parsing the project section-by-section."""
        allowed = {
            field.alias or name
            for name, field in StreamtProject.model_fields.items()
            if name != "project_path"
        }
        # Kept temporarily for the existing migration warning below.
        allowed.add("environments")
        unknown = sorted(set(data) - allowed)
        if unknown:
            details = "; ".join(
                f"field '{key}': Extra inputs are not permitted" for key in unknown
            )
            raise ParseError(f"Invalid stream_project.yml: {details}")

    def _parse_api_version(self, data: dict[str, object]) -> str:
        """Validate the DSL version while allowing legacy unversioned projects."""
        if "apiVersion" not in data:
            self.warn_callback(
                "[yellow]WARNING[/yellow]: stream_project.yml has no 'apiVersion'. "
                f"Interpreting it as legacy alpha '{CURRENT_API_VERSION}'. Add "
                f"'apiVersion: {CURRENT_API_VERSION}' to make the configuration contract explicit."
            )
        value = data.get("apiVersion", CURRENT_API_VERSION)
        if value != CURRENT_API_VERSION:
            raise ParseError(
                f"Invalid apiVersion '{value}'. Supported version: '{CURRENT_API_VERSION}'"
            )
        return CURRENT_API_VERSION

    def _validate_declaration_file(
        self, data: dict[str, object], path: Path, section: str
    ) -> None:
        """Reject misspelled or misplaced keys in declaration-directory files."""
        unknown = sorted(set(data) - {section})
        if unknown:
            details = "; ".join(
                f"field '{key}': Extra inputs are not permitted" for key in unknown
            )
            raise ParseError(f"Invalid {section} file '{path.name}': {details}")

    def _find_project_file(self) -> Path | None:
        """Find the stream_project.yml file."""
        candidates = [
            self.project_path / "stream_project.yml",
            self.project_path / "stream_project.yaml",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _load_yaml(self, path: Path) -> dict[str, object]:
        """Load and parse a YAML file."""
        try:
            with open(path) as f:
                content = f.read()
            return yaml.safe_load(content) or {}
        except yaml.YAMLError as e:
            raise ParseError(f"YAML parse error in '{path}': {e}") from e

    def _resolve_env_vars(self, value: object) -> object:
        """Recursively resolve environment variables in a value."""
        if isinstance(value, str):
            return self._resolve_env_var_string(value)
        elif isinstance(value, dict):
            return {k: self._resolve_env_vars(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_env_vars(v) for v in value]
        return value

    def _resolve_env_var_string(self, value: str) -> str:
        """Resolve environment variables in a string.

        Supports ${VAR} and ${VAR:-default} syntax.
        """

        def replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default = match.group(2)  # None if no :- syntax
            env_value = os.environ.get(var_name)
            if env_value is not None:
                return env_value
            if default is not None:
                return default
            raise EnvVarError(f"Environment variable '{var_name}' not set")

        return self.ENV_VAR_PATTERN.sub(replace, value)

    def _check_env_vars(self, value: object) -> list[str]:
        """Check which environment variables are used but not set (ignoring those with defaults)."""
        missing = []
        if isinstance(value, str):
            for match in self.ENV_VAR_PATTERN.finditer(value):
                var_name = match.group(1)
                has_default = match.group(2) is not None
                if os.environ.get(var_name) is None and not has_default:
                    missing.append(var_name)
        elif isinstance(value, dict):
            for v in value.values():
                missing.extend(self._check_env_vars(v))
        elif isinstance(value, list):
            for v in value:
                missing.extend(self._check_env_vars(v))
        return missing

    def _parse_project_info(self, data: dict[str, object]) -> ProjectInfo:
        """Parse project info section."""
        if "project" not in data:
            raise ParseError("Missing 'project' section in stream_project.yml")
        try:
            return ProjectInfo(**data["project"])
        except ValidationError as e:
            raise ParseError(f"Invalid project metadata: {_format_pydantic_error(e)}") from e

    def _parse_runtime(self, data: dict[str, object]) -> RuntimeConfig:
        """Parse runtime configuration.

        In multi-env mode, runtime comes from the environment file.
        In single-env mode, runtime is required in the project file.
        """
        # In multi-env mode, use runtime from environment config
        if self.env_config is not None:
            runtime_data = self.env_config.runtime

            # Check for missing env vars
            missing = self._check_env_vars(runtime_data)
            if missing:
                raise EnvVarError(
                    f"Environment variable{'s' if len(missing) > 1 else ''} "
                    f"not set: {', '.join(sorted(set(missing)))}"
                )

            # Resolve env vars
            resolved = self._resolve_env_vars(runtime_data)
            try:
                return RuntimeConfig(**resolved)
            except ValidationError as e:
                raise ParseError(f"Invalid runtime: {_format_pydantic_error(e)}") from e

        # Single-env mode: require runtime in project file
        if "runtime" not in data:
            raise ParseError("Missing 'runtime' section in stream_project.yml")

        runtime_data = data["runtime"]

        # Check for missing env vars but don't resolve yet (for validation)
        missing = self._check_env_vars(runtime_data)
        if missing:
            raise EnvVarError(
                f"Environment variable{'s' if len(missing) > 1 else ''} "
                f"not set: {', '.join(sorted(set(missing)))}"
            )

        # Resolve env vars
        resolved = self._resolve_env_vars(runtime_data)
        try:
            return RuntimeConfig(**resolved)
        except ValidationError as e:
            raise ParseError(f"Invalid runtime: {_format_pydantic_error(e)}") from e

    def _parse_defaults(self, data: dict[str, object]) -> Defaults | None:
        """Parse defaults section."""
        if "defaults" not in data:
            return None
        try:
            return Defaults(**data["defaults"])
        except ValidationError as e:
            raise ParseError(f"Invalid defaults: {_format_pydantic_error(e)}") from e

    def _parse_rules(self, data: dict[str, object]) -> Rules | None:
        """Parse rules section."""
        if "rules" not in data:
            return None
        try:
            return Rules(**data["rules"])
        except ValidationError as e:
            raise ParseError(f"Invalid rules: {_format_pydantic_error(e)}") from e

    def _parse_connections(self, data: dict[str, object]) -> dict[str, ConnectionConfig]:
        """Parse global connections section."""
        if "connections" not in data:
            return {}
        connections = {}
        for name, connection_data in data["connections"].items():
            try:
                connections[name] = ConnectionConfig(**connection_data)
            except ValidationError as e:
                raise ParseError(
                    f"Invalid connection '{name}': {_format_pydantic_error(e)}"
                ) from e
        return connections

    def _parse_sources(self, data: dict[str, object]) -> list[Source]:
        """Parse sources from project file and sources/ directory."""
        sources = []

        # From main project file
        if "sources" in data:
            for source_data in data["sources"]:
                try:
                    sources.append(Source(**source_data))
                except ValidationError as e:
                    name = source_data.get("name", "<unknown>")
                    raise ParseError(f"Invalid source '{name}': {_format_pydantic_error(e)}") from e

        # From sources/ directory
        sources_dir = self.project_path / "sources"
        if sources_dir.exists():
            for yml_file in sources_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                self._validate_declaration_file(file_data, yml_file, "sources")
                if "sources" in file_data:
                    for source_data in file_data["sources"]:
                        try:
                            sources.append(Source(**source_data))
                        except ValidationError as e:
                            name = source_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid source '{name}' in {yml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

            for yaml_file in sources_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                self._validate_declaration_file(file_data, yaml_file, "sources")
                if "sources" in file_data:
                    for source_data in file_data["sources"]:
                        try:
                            sources.append(Source(**source_data))
                        except ValidationError as e:
                            name = source_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid source '{name}' in {yaml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

        return sources

    def _parse_models(self, data: dict[str, object]) -> list[Model]:
        """Parse models from project file and models/ directory."""
        models = []

        # From main project file
        if "models" in data:
            for model_data in data["models"]:
                try:
                    models.append(Model(**model_data))
                except ValidationError as e:
                    name = model_data.get("name", "<unknown>")
                    raise ParseError(f"Invalid model '{name}': {_format_pydantic_error(e)}") from e

        # From models/ directory
        models_dir = self.project_path / "models"
        if models_dir.exists():
            # INIT-2: Warn about .sql files in models/ directory
            sql_files = list(models_dir.glob("*.sql"))
            if sql_files:
                names = ", ".join(f.name for f in sql_files[:5])
                suffix = f" and {len(sql_files) - 5} more" if len(sql_files) > 5 else ""
                self.warn_callback(
                    f"[yellow]WARNING[/yellow]: Found .sql files in models/ directory "
                    f"({names}{suffix}). streamt uses YAML model definitions with inline SQL, "
                    f"not separate .sql files. Move your SQL into the 'sql:' field of each model."
                )

            for yml_file in models_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                self._validate_declaration_file(file_data, yml_file, "models")
                if "models" in file_data:
                    for model_data in file_data["models"]:
                        try:
                            models.append(Model(**model_data))
                        except ValidationError as e:
                            name = model_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid model '{name}' in {yml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

            for yaml_file in models_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                self._validate_declaration_file(file_data, yaml_file, "models")
                if "models" in file_data:
                    for model_data in file_data["models"]:
                        try:
                            models.append(Model(**model_data))
                        except ValidationError as e:
                            name = model_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid model '{name}' in {yaml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

        return models

    def _parse_tests(self, data: dict[str, object]) -> list[DataTest]:
        """Parse tests from project file and tests/ directory."""
        tests = []

        # From main project file
        if "tests" in data:
            for test_data in data["tests"]:
                try:
                    tests.append(DataTest(**test_data))
                except ValidationError as e:
                    name = test_data.get("name", "<unknown>")
                    raise ParseError(f"Invalid test '{name}': {_format_pydantic_error(e)}") from e

        # From tests/ directory
        tests_dir = self.project_path / "tests"
        if tests_dir.exists():
            for yml_file in tests_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                self._validate_declaration_file(file_data, yml_file, "tests")
                if "tests" in file_data:
                    for test_data in file_data["tests"]:
                        try:
                            tests.append(DataTest(**test_data))
                        except ValidationError as e:
                            name = test_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid test '{name}' in {yml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

            for yaml_file in tests_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                self._validate_declaration_file(file_data, yaml_file, "tests")
                if "tests" in file_data:
                    for test_data in file_data["tests"]:
                        try:
                            tests.append(DataTest(**test_data))
                        except ValidationError as e:
                            name = test_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid test '{name}' in {yaml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

        return tests

    def _parse_exposures(self, data: dict[str, object]) -> list[Exposure]:
        """Parse exposures from project file and exposures/ directory."""
        exposures = []

        # From main project file
        if "exposures" in data:
            for exposure_data in data["exposures"]:
                try:
                    exposures.append(Exposure(**exposure_data))
                except ValidationError as e:
                    name = exposure_data.get("name", "<unknown>")
                    raise ParseError(
                        f"Invalid exposure '{name}': {_format_pydantic_error(e)}"
                    ) from e

        # From exposures/ directory
        exposures_dir = self.project_path / "exposures"
        if exposures_dir.exists():
            for yml_file in exposures_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                self._validate_declaration_file(file_data, yml_file, "exposures")
                if "exposures" in file_data:
                    for exposure_data in file_data["exposures"]:
                        try:
                            exposures.append(Exposure(**exposure_data))
                        except ValidationError as e:
                            name = exposure_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid exposure '{name}' in {yml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

            for yaml_file in exposures_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                self._validate_declaration_file(file_data, yaml_file, "exposures")
                if "exposures" in file_data:
                    for exposure_data in file_data["exposures"]:
                        try:
                            exposures.append(Exposure(**exposure_data))
                        except ValidationError as e:
                            name = exposure_data.get("name", "<unknown>")
                            raise ParseError(
                                f"Invalid exposure '{name}' in {yaml_file.name}: "
                                f"{_format_pydantic_error(e)}"
                            ) from e

        return exposures

    def _parse_udfs(self, data: dict[str, object]) -> list[UDFDeclaration]:
        """Parse UDF type declarations from the project file."""
        udfs = []
        for udf_data in data.get("udfs", []):
            try:
                udfs.append(UDFDeclaration(**udf_data))
            except ValidationError as e:
                name = udf_data.get("name", "<unknown>")
                raise ParseError(f"Invalid UDF '{name}': {_format_pydantic_error(e)}") from e
        return udfs

    def validate_jinja_sql(self, sql: str) -> tuple[bool, str | None]:
        """Validate Jinja syntax in SQL."""
        try:
            self.jinja_env.parse(sql)
            return True, None
        except TemplateSyntaxError as e:
            return False, str(e)

    def extract_refs_from_sql(self, sql: str) -> tuple[list[str], list[str]]:
        """Extract source() and ref() calls from SQL.

        Returns:
            Tuple of (sources, refs) lists.
        """
        sources: list[str] = []
        refs: list[str] = []

        # Pattern for {{ source("name") }} or {{ source('name') }}
        source_pattern = re.compile(r'\{\{\s*source\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}')

        # Pattern for {{ ref("name") }} or {{ ref('name') }}
        ref_pattern = re.compile(r'\{\{\s*ref\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}')

        for match in source_pattern.finditer(sql):
            sources.append(match.group(1))

        for match in ref_pattern.finditer(sql):
            refs.append(match.group(1))

        return sources, refs
