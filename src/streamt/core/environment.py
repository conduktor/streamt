"""Multi-environment support for streamt.

This module handles:
- Mode detection (single-env vs multi-env)
- Environment file discovery and loading
- .env file loading with precedence
- Environment configuration validation
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import yaml
from dotenv import dotenv_values

logger = logging.getLogger(__name__)


class EnvironmentError(Exception):
    """Error related to environment configuration."""

    pass


class EnvironmentNotFoundError(EnvironmentError):
    """Environment not found."""

    def __init__(self, env_name: str, available: list[str]):
        self.env_name = env_name
        self.available = available
        available_str = ", ".join(sorted(available)) if available else "none"
        super().__init__(f"Environment '{env_name}' not found. Available: {available_str}")


class NoEnvironmentSpecifiedError(EnvironmentError):
    """No environment specified in multi-env mode."""

    def __init__(self, available: list[str]):
        self.available = available
        available_str = ", ".join(sorted(available))
        super().__init__(
            f"Multiple environments found. Specify with --env. Available: {available_str}"
        )


class NoEnvironmentsConfiguredError(EnvironmentError):
    """--env flag used in single-env mode."""

    def __init__(self):
        super().__init__(
            "No environments configured. Remove --env flag or create environments/ directory."
        )


class EmptyEnvironmentsDirectoryError(EnvironmentError):
    """environments/ directory is empty."""

    def __init__(self):
        super().__init__(
            "No environment files found in environments/ directory. "
            "Add environment files like environments/dev.yml."
        )


class EnvironmentNameMismatchError(EnvironmentError):
    """Environment name in YAML doesn't match filename."""

    def __init__(self, filename: str, yaml_name: str):
        super().__init__(
            f"Environment name mismatch: file is '{filename}.yml' but "
            f"environment.name is '{yaml_name}'. They must match."
        )


class InvalidEnvironmentNameError(EnvironmentError):
    """Invalid environment name (e.g., path traversal attempt)."""

    def __init__(self, env_name: str):
        super().__init__(
            f"Invalid environment name '{env_name}'. "
            "Environment names can only contain alphanumeric characters and hyphens."
        )


@dataclass
class SafetyConfig:
    """Safety configuration for an environment."""

    confirm_apply: bool = True
    allow_destructive: bool = False
    require_reviewed_plan: bool = False


@dataclass
class EnvironmentInfo:
    """Metadata about an environment."""

    name: str
    description: str = ""
    protected: bool = False


@dataclass
class EnvironmentConfig:
    """Complete environment configuration."""

    environment: EnvironmentInfo
    runtime: dict[str, object]
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    raw_data: dict[str, object] = field(default_factory=dict)

    @property
    def requires_reviewed_plan(self) -> bool:
        """Return whether apply must use a saved, reviewed plan file."""
        return self.environment.protected or self.safety.require_reviewed_plan

    @property
    def requires_apply_confirmation(self) -> bool:
        """Return whether apply requires explicit environment confirmation."""
        return self.environment.protected or self.safety.confirm_apply


class EnvironmentManager:
    """Manages multi-environment configuration for streamt projects."""

    # Pattern for valid environment names (alphanumeric and hyphens)
    ENV_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]*$")

    def __init__(self, project_path: Path):
        """Initialize with project path."""
        self.project_path = project_path.resolve()
        self.environments_dir = self.project_path / "environments"
        self._mode: Literal["single", "multi"] | None = None
        self._environments: dict[str, EnvironmentConfig] | None = None

    @property
    def mode(self) -> Literal["single", "multi"]:
        """Detect and return the environment mode."""
        if self._mode is None:
            self._mode = self._detect_mode()
        return self._mode

    def _detect_mode(self) -> Literal["single", "multi"]:
        """Detect whether project uses single or multi environment mode."""
        if self.environments_dir.exists() and self.environments_dir.is_dir():
            return "multi"
        return "single"

    def discover_environments(self) -> list[str]:
        """Discover available environment names."""
        if self.mode == "single":
            return []

        environments = []
        for f in self.environments_dir.iterdir():
            if f.is_file() and f.suffix in (".yml", ".yaml"):
                env_name = f.stem
                environments.append(env_name)

        return sorted(environments)

    def validate_env_name(self, env_name: str) -> None:
        """Validate environment name (prevent path traversal, etc.)."""
        if not self.ENV_NAME_PATTERN.match(env_name):
            raise InvalidEnvironmentNameError(env_name)
        if ".." in env_name or "/" in env_name or "\\" in env_name:
            raise InvalidEnvironmentNameError(env_name)

    def get_effective_environment(self, cli_flag: str | None, env_var: str | None) -> str | None:
        """Get effective environment name from CLI flag or env var.

        CLI flag takes precedence over environment variable.
        """
        if cli_flag:
            return cli_flag
        return env_var

    def load_dotenv_for_environment(self, env_name: str | None) -> dict[str, str]:
        """Load .env files with proper precedence.

        Order (later overrides earlier):
        1. .env (base, always loaded)
        2. .env.{environment} (if exists)
        3. Actual environment variables (highest priority - handled by caller)
        """
        env_vars: dict[str, str] = {}

        # Load base .env
        base_env = self.project_path / ".env"
        if base_env.exists():
            env_vars.update(dotenv_values(base_env))

        # Load environment-specific .env
        if env_name:
            env_specific = self.project_path / f".env.{env_name}"
            if env_specific.exists():
                env_vars.update(dotenv_values(env_specific))

        return env_vars

    def apply_env_vars(self, env_name: str | None) -> None:
        """Load and apply .env files to os.environ.

        Respects precedence: .env < .env.{env} < actual env vars.
        """
        # Load from files
        file_vars = self.load_dotenv_for_environment(env_name)

        # Apply to os.environ (but don't override existing real env vars)
        for key, value in file_vars.items():
            if key not in os.environ:
                os.environ[key] = value

    def load_environment(self, env_name: str) -> EnvironmentConfig:
        """Load a specific environment configuration."""
        self.validate_env_name(env_name)

        if self.mode == "single":
            raise NoEnvironmentsConfiguredError()

        available = self.discover_environments()
        if not available:
            raise EmptyEnvironmentsDirectoryError()

        if env_name not in available:
            raise EnvironmentNotFoundError(env_name, available)

        # Load the environment file
        env_file = self.environments_dir / f"{env_name}.yml"
        if not env_file.exists():
            env_file = self.environments_dir / f"{env_name}.yaml"

        try:
            with open(env_file) as f:
                loaded = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise EnvironmentError(f"YAML parse error in '{env_file}': {e}") from e
        if loaded is None:
            data: dict[str, object] = {}
        elif isinstance(loaded, dict):
            data = loaded
        else:
            raise EnvironmentError(
                f"Invalid environment file '{env_file.name}': expected an object"
            )

        allowed_top_level_fields = {"environment", "runtime", "safety"}
        unknown_top_level_fields = sorted(set(data) - allowed_top_level_fields)
        if unknown_top_level_fields:
            details = "; ".join(
                f"field '{field}': Extra inputs are not permitted"
                for field in unknown_top_level_fields
            )
            raise EnvironmentError(
                f"Invalid environment file '{env_file.name}': {details}"
            )

        # Validate environment name matches filename
        env_info = data.get("environment", {})
        if not isinstance(env_info, dict):
            raise EnvironmentError(
                f"Invalid environment configuration in '{env_file.name}': expected an object"
            )
        allowed_environment_fields = {"name", "description", "protected"}
        unknown_environment_fields = sorted(
            set(env_info) - allowed_environment_fields
        )
        if unknown_environment_fields:
            details = "; ".join(
                f"environment.{field}: Extra inputs are not permitted"
                for field in unknown_environment_fields
            )
            raise EnvironmentError(
                f"Invalid environment configuration in '{env_file.name}': {details}"
            )
        yaml_name = env_info.get("name")
        if yaml_name is not None and not isinstance(yaml_name, str):
            raise EnvironmentError(
                f"Invalid environment configuration in '{env_file.name}': "
                "environment.name must be a string"
            )
        if yaml_name and yaml_name != env_name:
            raise EnvironmentNameMismatchError(env_name, yaml_name)

        description = env_info.get("description", "")
        if not isinstance(description, str):
            raise EnvironmentError(
                f"Invalid environment configuration in '{env_file.name}': "
                "environment.description must be a string"
            )
        protected = env_info.get("protected", False)
        if not isinstance(protected, bool):
            raise EnvironmentError(
                f"Invalid environment configuration in '{env_file.name}': "
                "environment.protected must be a boolean"
            )

        # Parse environment info
        environment = EnvironmentInfo(
            name=env_name,
            description=description,
            protected=protected,
        )

        # Parse runtime
        runtime = data.get("runtime", {})
        if not runtime:
            raise EnvironmentError(
                f"Missing 'runtime' section in environment file '{env_file.name}'"
            )

        # Validate runtime against Pydantic model.
        # Resolve ${VAR} env-var placeholders first so value validators
        # (e.g. URL format checks) don't reject raw placeholder strings.
        from streamt.core.models import RuntimeConfig

        resolved_runtime = _resolve_env_var_placeholders(runtime)
        try:
            RuntimeConfig.model_validate(resolved_runtime)
        except Exception as e:
            raise EnvironmentError(
                f"Invalid runtime configuration in '{env_file.name}': {e}"
            ) from e

        # Parse safety config
        safety_data = data.get("safety", {})
        if not isinstance(safety_data, dict):
            raise EnvironmentError(
                f"Invalid safety configuration in '{env_file.name}': expected an object"
            )
        allowed_safety_fields = {
            "confirm_apply",
            "allow_destructive",
            "require_reviewed_plan",
        }
        unknown_safety_fields = sorted(set(safety_data) - allowed_safety_fields)
        if unknown_safety_fields:
            details = "; ".join(
                f"safety.{field}: Extra inputs are not permitted"
                for field in unknown_safety_fields
            )
            raise EnvironmentError(
                f"Invalid safety configuration in '{env_file.name}': {details}"
            )
        for field_name, default in (
            ("confirm_apply", environment.protected),
            ("allow_destructive", False),
            ("require_reviewed_plan", False),
        ):
            value = safety_data.get(field_name, default)
            if not isinstance(value, bool):
                raise EnvironmentError(
                    f"Invalid safety configuration in '{env_file.name}': "
                    f"safety.{field_name} must be a boolean"
                )
        safety = SafetyConfig(
            confirm_apply=safety_data.get("confirm_apply", environment.protected),
            allow_destructive=safety_data.get("allow_destructive", False),
            require_reviewed_plan=safety_data.get("require_reviewed_plan", False),
        )

        return EnvironmentConfig(
            environment=environment,
            runtime=runtime,
            safety=safety,
            raw_data=data,
        )

    def resolve_environment(
        self,
        cli_env: str | None,
    ) -> tuple[EnvironmentConfig | None, list[str]]:
        """Resolve environment based on mode, CLI flag, and env var.

        Returns:
            Tuple of (EnvironmentConfig or None, list of warnings)

        Raises:
            EnvironmentError subclasses for various error conditions
        """
        warnings: list[str] = []
        env_var = os.environ.get("STREAMT_ENV")
        effective_env = self.get_effective_environment(cli_env, env_var)

        if self.mode == "single":
            # Single-env mode
            if cli_env:
                raise NoEnvironmentsConfiguredError()
            if env_var:
                warnings.append(f"STREAMT_ENV='{env_var}' ignored in single-environment mode")
            # Apply base .env only
            self.apply_env_vars(None)
            return None, warnings

        # Multi-env mode
        available = self.discover_environments()
        if not available:
            raise EmptyEnvironmentsDirectoryError()

        if not effective_env:
            raise NoEnvironmentSpecifiedError(available)

        # Apply .env files for this environment
        self.apply_env_vars(effective_env)

        # Load and return environment config
        env_config = self.load_environment(effective_env)
        return env_config, warnings

    def check_project_runtime_warning(self, project_data: dict[str, object]) -> str | None:
        """Check if project has runtime: in multi-env mode (should warn)."""
        if self.mode == "multi" and "runtime" in project_data:
            return "runtime: in stream_project.yml is ignored in multi-environment mode"
        return None

    def list_environments(self) -> list[EnvironmentConfig]:
        """List all available environments with their configs."""
        if self.mode == "single":
            return []

        environments = []
        for env_name in self.discover_environments():
            try:
                env_config = self.load_environment(env_name)
                environments.append(env_config)
            except EnvironmentError as e:
                logger.debug("Skipping environment '%s': %s", env_name, e)

        return environments


_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-((?:[^}]|\\})*)?)?\}")


def _resolve_env_var_placeholders(value: object) -> object:
    """Resolve ${VAR} and ${VAR:-default} placeholders from os.environ.

    Unresolved variables without defaults are replaced with a safe
    placeholder (``http://placeholder``) so that Pydantic value
    validators (e.g. URL format) don't reject raw ``${VAR}`` strings
    while structural validation still catches typos in field names.
    """
    if isinstance(value, str):
        return _resolve_env_var_string(value)
    if isinstance(value, dict):
        return {k: _resolve_env_var_placeholders(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_var_placeholders(v) for v in value]
    return value


def _resolve_env_var_string(value: str) -> str:
    """Resolve env-var references in a single string."""

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default = match.group(2)
        env_value = os.environ.get(var_name)
        if env_value is not None:
            return env_value
        if default is not None:
            return default
        # Placeholder that satisfies common validators (URL, host:port)
        return "http://placeholder"

    return _ENV_VAR_PATTERN.sub(_replace, value)


_SECRET_FIELD_NAMES = {
    "password",
    "sasl_password",
    "ssl_key_password",
    "api_key",
    "api_secret",
    "secret",
    "token",
    "credential",
}


def mask_secrets(value: object, secret_keys: set[str] | None = None) -> object:
    """Recursively mask secret values in a dictionary.

    Only exact field names in the secret set are masked. File path fields
    like ssl_key_location are NOT masked.
    """
    if secret_keys is None:
        secret_keys = _SECRET_FIELD_NAMES

    if isinstance(value, dict):
        return {
            k: "****"
            if k.lower() in secret_keys and isinstance(v, str)
            else mask_secrets(v, secret_keys)
            for k, v in value.items()
        }
    elif isinstance(value, list):
        return [mask_secrets(v, secret_keys) for v in value]
    return value
