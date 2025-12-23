"""Parser for streamt project files."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from jinja2 import BaseLoader, Environment, TemplateSyntaxError

from streamt.core.models import (
    DataTest,
    Defaults,
    Exposure,
    Model,
    ProjectInfo,
    Rules,
    RuntimeConfig,
    Source,
    StreamtProject,
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


class ProjectParser:
    """Parser for streamt projects."""

    ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

    def __init__(self, project_path: Path) -> None:
        """Initialize parser with project path."""
        self.project_path = project_path.resolve()

        # Load .env file if present
        env_file = self.project_path / ".env"
        if env_file.exists():
            load_dotenv(env_file)

        # Jinja environment for SQL parsing
        self.jinja_env = Environment(loader=BaseLoader())

    def parse(self) -> StreamtProject:
        """Parse the entire project."""
        # Find and parse stream_project.yml
        project_file = self._find_project_file()
        if not project_file:
            raise ParseError(f"No stream_project.yml found in {self.project_path}")

        project_data = self._load_yaml(project_file)

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

        return StreamtProject(
            project=project_info,
            runtime=runtime,
            defaults=defaults,
            rules=rules,
            sources=sources,
            models=models,
            tests=tests,
            exposures=exposures,
            project_path=self.project_path,
        )

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

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        """Load and parse a YAML file."""
        try:
            with open(path) as f:
                content = f.read()
            return yaml.safe_load(content) or {}
        except yaml.YAMLError as e:
            raise ParseError(f"YAML parse error in '{path}': {e}")

    def _resolve_env_vars(self, value: Any) -> Any:
        """Recursively resolve environment variables in a value."""
        if isinstance(value, str):
            return self._resolve_env_var_string(value)
        elif isinstance(value, dict):
            return {k: self._resolve_env_vars(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_env_vars(v) for v in value]
        return value

    def _resolve_env_var_string(self, value: str) -> str:
        """Resolve environment variables in a string."""

        def replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            env_value = os.environ.get(var_name)
            if env_value is None:
                raise EnvVarError(f"Environment variable '{var_name}' not set")
            return env_value

        return self.ENV_VAR_PATTERN.sub(replace, value)

    def _check_env_vars(self, value: Any) -> list[str]:
        """Check which environment variables are used but not set."""
        missing = []
        if isinstance(value, str):
            for match in self.ENV_VAR_PATTERN.finditer(value):
                var_name = match.group(1)
                if os.environ.get(var_name) is None:
                    missing.append(var_name)
        elif isinstance(value, dict):
            for v in value.values():
                missing.extend(self._check_env_vars(v))
        elif isinstance(value, list):
            for v in value:
                missing.extend(self._check_env_vars(v))
        return missing

    def _parse_project_info(self, data: dict[str, Any]) -> ProjectInfo:
        """Parse project info section."""
        if "project" not in data:
            raise ParseError("Missing 'project' section in stream_project.yml")
        return ProjectInfo(**data["project"])

    def _parse_runtime(self, data: dict[str, Any]) -> RuntimeConfig:
        """Parse runtime configuration."""
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
        return RuntimeConfig(**resolved)

    def _parse_defaults(self, data: dict[str, Any]) -> Defaults | None:
        """Parse defaults section."""
        if "defaults" not in data:
            return None
        return Defaults(**data["defaults"])

    def _parse_rules(self, data: dict[str, Any]) -> Rules | None:
        """Parse rules section."""
        if "rules" not in data:
            return None
        return Rules(**data["rules"])

    def _parse_sources(self, data: dict[str, Any]) -> list[Source]:
        """Parse sources from project file and sources/ directory."""
        sources = []

        # From main project file
        if "sources" in data:
            for source_data in data["sources"]:
                sources.append(Source(**source_data))

        # From sources/ directory
        sources_dir = self.project_path / "sources"
        if sources_dir.exists():
            for yml_file in sources_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                if "sources" in file_data:
                    for source_data in file_data["sources"]:
                        sources.append(Source(**source_data))

            for yaml_file in sources_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                if "sources" in file_data:
                    for source_data in file_data["sources"]:
                        sources.append(Source(**source_data))

        return sources

    def _parse_models(self, data: dict[str, Any]) -> list[Model]:
        """Parse models from project file and models/ directory."""
        models = []

        # From main project file
        if "models" in data:
            for model_data in data["models"]:
                models.append(Model(**model_data))

        # From models/ directory
        models_dir = self.project_path / "models"
        if models_dir.exists():
            for yml_file in models_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                if "models" in file_data:
                    for model_data in file_data["models"]:
                        models.append(Model(**model_data))

            for yaml_file in models_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                if "models" in file_data:
                    for model_data in file_data["models"]:
                        models.append(Model(**model_data))

        return models

    def _parse_tests(self, data: dict[str, Any]) -> list[DataTest]:
        """Parse tests from project file and tests/ directory."""
        tests = []

        # From main project file
        if "tests" in data:
            for test_data in data["tests"]:
                tests.append(DataTest(**test_data))

        # From tests/ directory
        tests_dir = self.project_path / "tests"
        if tests_dir.exists():
            for yml_file in tests_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                if "tests" in file_data:
                    for test_data in file_data["tests"]:
                        tests.append(DataTest(**test_data))

            for yaml_file in tests_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                if "tests" in file_data:
                    for test_data in file_data["tests"]:
                        tests.append(DataTest(**test_data))

        return tests

    def _parse_exposures(self, data: dict[str, Any]) -> list[Exposure]:
        """Parse exposures from project file and exposures/ directory."""
        exposures = []

        # From main project file
        if "exposures" in data:
            for exposure_data in data["exposures"]:
                exposures.append(Exposure(**exposure_data))

        # From exposures/ directory
        exposures_dir = self.project_path / "exposures"
        if exposures_dir.exists():
            for yml_file in exposures_dir.glob("*.yml"):
                file_data = self._load_yaml(yml_file)
                if "exposures" in file_data:
                    for exposure_data in file_data["exposures"]:
                        exposures.append(Exposure(**exposure_data))

            for yaml_file in exposures_dir.glob("*.yaml"):
                file_data = self._load_yaml(yaml_file)
                if "exposures" in file_data:
                    for exposure_data in file_data["exposures"]:
                        exposures.append(Exposure(**exposure_data))

        return exposures

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
