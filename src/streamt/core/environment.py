"""Multi-environment support for streamt.

This module handles:
- Mode detection (single-env vs multi-env)
- Environment file discovery and loading
- .env file loading with precedence
- Environment configuration validation
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import dotenv_values


class EnvironmentError(Exception):
    """Error related to environment configuration."""

    pass


class EnvironmentNotFoundError(EnvironmentError):
    """Environment not found."""

    def __init__(self, env_name: str, available: list[str]):
        self.env_name = env_name
        self.available = available
        available_str = ", ".join(sorted(available)) if available else "none"
        super().__init__(
            f"Environment '{env_name}' not found. Available: {available_str}"
        )


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
    allow_destructive: bool = True


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
    runtime: dict[str, Any]
    safety: SafetyConfig = field(default_factory=SafetyConfig)
    raw_data: dict[str, Any] = field(default_factory=dict)


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

    def get_effective_environment(
        self, cli_flag: str | None, env_var: str | None
    ) -> str | None:
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
                data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise EnvironmentError(f"YAML parse error in '{env_file}': {e}")

        # Validate environment name matches filename
        env_info = data.get("environment", {})
        yaml_name = env_info.get("name")
        if yaml_name and yaml_name != env_name:
            raise EnvironmentNameMismatchError(env_name, yaml_name)

        # Parse environment info
        environment = EnvironmentInfo(
            name=env_name,
            description=env_info.get("description", ""),
            protected=env_info.get("protected", False),
        )

        # Parse runtime
        runtime = data.get("runtime", {})
        if not runtime:
            raise EnvironmentError(
                f"Missing 'runtime' section in environment file '{env_file.name}'"
            )

        # Parse safety config
        safety_data = data.get("safety", {})
        safety = SafetyConfig(
            confirm_apply=safety_data.get("confirm_apply", environment.protected),
            allow_destructive=safety_data.get("allow_destructive", True),
        )

        return EnvironmentConfig(
            environment=environment,
            runtime=runtime,
            safety=safety,
            raw_data=data,
        )

    def resolve_environment(
        self, cli_env: str | None, warn_callback: callable = None
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
                warnings.append(
                    f"STREAMT_ENV='{env_var}' ignored in single-environment mode"
                )
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

    def check_project_runtime_warning(self, project_data: dict[str, Any]) -> str | None:
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
            except EnvironmentError:
                # Skip environments that fail to load
                pass

        return environments


def mask_secrets(value: Any, secret_keys: set[str] | None = None) -> Any:
    """Recursively mask secret values in a dictionary.

    Keys containing 'password', 'secret', 'key', 'token', 'credential'
    will have their values replaced with '****'.
    """
    if secret_keys is None:
        secret_keys = {"password", "secret", "key", "token", "credential", "api_key", "api_secret"}

    def is_secret_key(key: str) -> bool:
        key_lower = key.lower()
        return any(s in key_lower for s in secret_keys)

    if isinstance(value, dict):
        return {
            k: "****" if is_secret_key(k) and isinstance(v, str) else mask_secrets(v, secret_keys)
            for k, v in value.items()
        }
    elif isinstance(value, list):
        return [mask_secrets(v, secret_keys) for v in value]
    return value
