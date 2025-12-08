"""Rich error messages with actionable suggestions.

This module provides error message formatting that teaches users how to fix issues.
Each error includes:
- What went wrong
- Why it matters
- How to fix it
- Link to documentation
"""

from difflib import get_close_matches
from typing import Optional

# Base URL for documentation
DOCS_BASE_URL = "https://streamt.dev/docs"


def format_error(
    title: str,
    explanation: str,
    suggestion: Optional[str] = None,
    example: Optional[str] = None,
    docs_path: Optional[str] = None,
) -> str:
    """Format a rich error message.

    Args:
        title: Short error title
        explanation: What went wrong and why
        suggestion: How to fix it (optional)
        example: Code example showing the fix (optional)
        docs_path: Path to docs page, e.g., "concepts/sources" (optional)

    Returns:
        Formatted error message string
    """
    parts = [title, "", explanation]

    if suggestion:
        parts.extend(["", suggestion])

    if example:
        # Indent example code
        indented = "\n".join(f"    {line}" for line in example.strip().split("\n"))
        parts.extend(["", indented])

    if docs_path:
        parts.extend(["", f"Learn more: {DOCS_BASE_URL}/{docs_path}"])

    return "\n".join(parts)


def suggest_similar(name: str, available: list[str], noun: str = "name") -> str:
    """Suggest similar names from a list.

    Args:
        name: The name that wasn't found
        available: List of available names
        noun: What kind of thing (e.g., "source", "model")

    Returns:
        Suggestion string or empty if no good matches
    """
    if not available:
        return ""

    matches = get_close_matches(name, available, n=3, cutoff=0.6)
    if matches:
        if len(matches) == 1:
            return f"Did you mean '{matches[0]}'?"
        else:
            quoted = [f"'{m}'" for m in matches]
            return f"Did you mean one of: {', '.join(quoted)}?"

    # No close matches, show available options if not too many
    if len(available) <= 5:
        quoted = [f"'{n}'" for n in sorted(available)]
        return f"Available {noun}s: {', '.join(quoted)}"

    return f"Available {noun}s: {len(available)} defined"


# =============================================================================
# Validation Errors
# =============================================================================


def source_not_found(
    source_name: str,
    model_name: str,
    available_sources: list[str],
) -> str:
    """Error when a source reference doesn't exist."""
    suggestion = suggest_similar(source_name, available_sources, "source")

    return format_error(
        title=f"Source '{source_name}' not found",
        explanation=f"Model '{model_name}' references source '{source_name}' in its SQL, "
        f"but no source with that name is defined.",
        suggestion=suggestion or "Define the source in your sources configuration.",
        example=f"""sources:
  - name: {source_name}
    topic: {source_name.replace('_', '.')}.v1""",
        docs_path="concepts/sources",
    )


def model_not_found(
    model_name: str,
    referencing_model: str,
    available_models: list[str],
) -> str:
    """Error when a model reference doesn't exist."""
    suggestion = suggest_similar(model_name, available_models, "model")

    return format_error(
        title=f"Model '{model_name}' not found",
        explanation=f"Model '{referencing_model}' references model '{model_name}' via ref(), "
        f"but no model with that name is defined.",
        suggestion=suggestion or "Define the model or check for typos in the ref() call.",
        docs_path="concepts/models",
    )


def gateway_required(model_name: str) -> str:
    """Error when virtual_topic is used without Gateway config."""
    return format_error(
        title=f"Gateway configuration required for '{model_name}'",
        explanation=f"Model '{model_name}' uses materialization 'virtual_topic', which requires "
        f"Conduktor Gateway to be configured. Virtual topics are read-time filters "
        f"that don't create physical Kafka topics.",
        suggestion="Add Gateway configuration to your runtime section:",
        example="""runtime:
  conduktor:
    gateway:
      url: http://localhost:8888
      username: admin
      password: ${GATEWAY_PASSWORD}""",
        docs_path="reference/configuration#conduktor-gateway",
    )


def flink_required(model_name: str) -> str:
    """Error when flink materialization is used without Flink config."""
    return format_error(
        title=f"Flink configuration required for '{model_name}'",
        explanation=f"Model '{model_name}' uses materialization 'flink', which requires "
        f"a Flink cluster to be configured for SQL job execution.",
        suggestion="Add Flink configuration to your runtime section:",
        example="""runtime:
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8081
        sql_gateway_url: http://localhost:8083""",
        docs_path="reference/flink-options",
    )


def missing_sink_config(model_name: str) -> str:
    """Error when sink materialization is missing sink config."""
    return format_error(
        title=f"Sink configuration required for '{model_name}'",
        explanation=f"Model '{model_name}' uses materialization 'sink', which requires "
        f"a sink configuration specifying the connector type and settings.",
        suggestion="Add sink configuration to your model:",
        example="""models:
  - name: to_snowflake
    materialized: sink
    from:
      - ref: orders_clean
    sink:
      connector: snowflake-sink
      config:
        snowflake.database.name: ANALYTICS
        snowflake.schema.name: PUBLIC
        snowflake.url.name: ${SNOWFLAKE_URL}""",
        docs_path="reference/materializations#sink",
    )


def connect_required(model_name: str) -> str:
    """Error when sink materialization is used without Connect config."""
    return format_error(
        title=f"Kafka Connect configuration required for '{model_name}'",
        explanation=f"Model '{model_name}' uses materialization 'sink', which requires "
        f"Kafka Connect to be configured for connector deployment.",
        suggestion="Add Connect configuration to your runtime section:",
        example="""runtime:
  connect:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8083""",
        docs_path="reference/configuration#kafka-connect",
    )


def jinja_syntax_error(model_name: str, error: str, sql: Optional[str] = None) -> str:
    """Error when Jinja template has syntax errors."""
    explanation = f"Model '{model_name}' has invalid Jinja2 syntax in its SQL template."

    # Try to extract line number from Jinja error
    line_info = ""
    if "line " in error.lower():
        line_info = f"\n\nError details: {error}"
    else:
        line_info = f"\n\nJinja error: {error}"

    # Show problematic SQL if available
    sql_snippet = ""
    if sql:
        lines = sql.strip().split("\n")[:5]  # First 5 lines
        sql_snippet = "\n\nYour SQL:\n" + "\n".join(f"    {l}" for l in lines)
        if len(sql.strip().split("\n")) > 5:
            sql_snippet += "\n    ..."

    return format_error(
        title=f"Jinja syntax error in model '{model_name}'",
        explanation=explanation + line_info + sql_snippet,
        suggestion="Common issues:\n"
        "  - Missing closing braces: {{ source(\"name\") }} not {{ source(\"name\")\n"
        "  - Wrong quotes: use {{ source(\"name\") }} not {{ source('name') }}\n"
        "  - Typo in function: use source() or ref(), not sources() or refs()",
        docs_path="concepts/models#sql-templates",
    )


def duplicate_name(
    entity_type: str,
    name: str,
    locations: Optional[list[str]] = None,
) -> str:
    """Error when a name is defined multiple times."""
    explanation = f"The {entity_type} name '{name}' is defined more than once. "
    explanation += "Each name must be unique within its type."

    if locations and len(locations) > 1:
        explanation += f"\n\nFound in:\n" + "\n".join(f"  - {loc}" for loc in locations)

    return format_error(
        title=f"Duplicate {entity_type} name '{name}'",
        explanation=explanation,
        suggestion=f"Rename one of the {entity_type}s to have a unique name.",
    )


def cycle_detected(cycle: list[str]) -> str:
    """Error when a circular dependency is detected."""
    cycle_str = " -> ".join(cycle)

    return format_error(
        title="Circular dependency detected",
        explanation=f"Your models form a cycle: {cycle_str}\n\n"
        "Each model in this chain depends on the next, creating an infinite loop.",
        suggestion="Break the cycle by:\n"
        "  1. Removing one of the ref() calls\n"
        "  2. Restructuring to have a clear upstream -> downstream flow\n"
        "  3. Creating an intermediate model that breaks the dependency",
        docs_path="concepts/dag",
    )


def continuous_test_without_flink(test_name: str) -> str:
    """Error when continuous test is defined without Flink config."""
    return format_error(
        title=f"Flink required for continuous test '{test_name}'",
        explanation=f"Test '{test_name}' is a continuous test that runs as a Flink job. "
        "Continuous tests monitor data quality in real-time by running SQL assertions "
        "against your streaming data.",
        suggestion="Either:\n"
        "  1. Add Flink configuration to your runtime section\n"
        "  2. Change the test type to 'schema' or 'sample' (one-time tests)",
        example="""runtime:
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8081
        sql_gateway_url: http://localhost:8083""",
        docs_path="concepts/tests#continuous-tests",
    )


def test_model_not_found(test_name: str, model_name: str, available_models: list[str]) -> str:
    """Error when a test references a non-existent model."""
    suggestion = suggest_similar(model_name, available_models, "model")

    return format_error(
        title=f"Model '{model_name}' not found for test '{test_name}'",
        explanation=f"Test '{test_name}' references model '{model_name}', "
        f"but no model with that name is defined.",
        suggestion=suggestion or "Define the model or check for typos in the test configuration.",
        docs_path="concepts/tests",
    )


def exposure_model_not_found(
    exposure_name: str,
    model_name: str,
    available_models: list[str],
) -> str:
    """Error when an exposure references a non-existent model."""
    suggestion = suggest_similar(model_name, available_models, "model")

    return format_error(
        title=f"Model '{model_name}' not found for exposure '{exposure_name}'",
        explanation=f"Exposure '{exposure_name}' references model '{model_name}', "
        f"but no model with that name is defined.",
        suggestion=suggestion or "Define the model or check for typos in the exposure configuration.",
        docs_path="concepts/exposures",
    )


def exposure_source_not_found(
    exposure_name: str,
    source_name: str,
    available_sources: list[str],
) -> str:
    """Error when an exposure references a non-existent source."""
    suggestion = suggest_similar(source_name, available_sources, "source")

    return format_error(
        title=f"Source '{source_name}' not found for exposure '{exposure_name}'",
        explanation=f"Exposure '{exposure_name}' produces source '{source_name}', "
        f"but no source with that name is defined.",
        suggestion=suggestion or "Define the source or check for typos in the exposure configuration.",
        docs_path="concepts/exposures",
    )


def exposure_dependency_not_found(
    exposure_name: str,
    dependency_name: str,
    dependency_type: str,  # "model" or "source"
    available: list[str],
) -> str:
    """Error when an exposure depends on a non-existent model or source."""
    suggestion = suggest_similar(dependency_name, available, dependency_type)

    return format_error(
        title=f"{dependency_type.capitalize()} '{dependency_name}' not found for exposure '{exposure_name}'",
        explanation=f"Exposure '{exposure_name}' depends on {dependency_type} '{dependency_name}', "
        f"but no {dependency_type} with that name is defined.",
        suggestion=suggestion or f"Define the {dependency_type} or check for typos in the depends_on configuration.",
        docs_path="concepts/exposures",
    )


def access_denied(
    referencing_model: str,
    target_model: str,
    target_group: str,
) -> str:
    """Error when a model tries to reference a private model from another group."""
    return format_error(
        title=f"Access denied: '{referencing_model}' cannot reference '{target_model}'",
        explanation=f"Model '{target_model}' has access set to 'private' and belongs to group '{target_group}'. "
        f"Only models within the same group can reference it.\n\n"
        f"Model '{referencing_model}' is not in group '{target_group}'.",
        suggestion="Options:\n"
        f"  1. Move '{referencing_model}' to group '{target_group}'\n"
        f"  2. Change '{target_model}' access to 'protected' or 'public'\n"
        "  3. Create an intermediate public model that exposes the needed data",
        example=f"""# Option 1: Add model to the same group
models:
  - name: {referencing_model}
    group: {target_group}
    ...

# Option 2: Change target access level
models:
  - name: {target_model}
    access: protected  # or 'public'
    ...""",
        docs_path="concepts/models#access-control",
    )


# =============================================================================
# Deployment Errors
# =============================================================================


def cannot_reduce_partitions(
    topic_name: str,
    current: int,
    desired: int,
) -> str:
    """Error when trying to reduce partition count."""
    return format_error(
        title=f"Cannot reduce partitions for topic '{topic_name}'",
        explanation=f"You're trying to change partitions from {current} to {desired}, "
        "but Kafka does not support reducing the number of partitions.\n\n"
        "Why? Reducing partitions would require moving data between partitions, "
        "which could break ordering guarantees and consumer group assignments.",
        suggestion="Options:\n"
        f"  1. Keep the current partition count ({current})\n"
        "  2. Create a new topic with fewer partitions and migrate data\n"
        "  3. Delete and recreate the topic (WARNING: data loss)",
        docs_path="concepts/streaming-fundamentals",
    )


def schema_incompatible(
    subject: str,
    compatibility_mode: str,
    breaking_changes: Optional[list[str]] = None,
) -> str:
    """Error when schema change is incompatible."""
    changes_str = ""
    if breaking_changes:
        changes_str = "\n\nBreaking changes detected:\n" + "\n".join(
            f"  - {c}" for c in breaking_changes
        )

    return format_error(
        title=f"Schema incompatible for subject '{subject}'",
        explanation=f"The new schema is not compatible with the existing schema "
        f"under '{compatibility_mode}' compatibility mode.{changes_str}",
        suggestion="Options:\n"
        "  1. Fix the breaking changes to maintain compatibility\n"
        "  2. Create a new subject version (e.g., topic.v2-value)\n"
        "  3. Change compatibility mode (not recommended for production)",
        docs_path="concepts/sources#schema-registry",
    )


def flink_sql_error(error_msg: str, sql_snippet: Optional[str] = None) -> str:
    """Parse and improve Flink SQL error messages."""
    # Try to extract useful info from Flink's error
    improved = error_msg

    # Common patterns and their user-friendly explanations
    patterns = [
        (
            "Table '(.+)' not found",
            "The table doesn't exist. Check that the source/model is defined and spelled correctly.",
        ),
        (
            "Column '(.+)' not found",
            "The column doesn't exist in the source. Check your source's columns definition.",
        ),
        (
            "Cannot apply '(.+)' to arguments of type",
            "Type mismatch in your SQL. Check that you're comparing compatible types.",
        ),
        (
            "SQL validation failed",
            "Your SQL has syntax errors. Check for missing commas, parentheses, or keywords.",
        ),
        (
            "No match found for function signature",
            "Unknown function. Check the function name and argument types.",
        ),
    ]

    suggestion = None
    for pattern, explanation in patterns:
        import re

        if re.search(pattern, error_msg, re.IGNORECASE):
            suggestion = explanation
            break

    if not suggestion:
        suggestion = "Check your SQL syntax and ensure all referenced tables/columns exist."

    sql_info = ""
    if sql_snippet:
        sql_info = f"\n\nSQL:\n    {sql_snippet[:200]}..."

    return format_error(
        title="Flink SQL execution error",
        explanation=f"{error_msg}{sql_info}",
        suggestion=suggestion,
        docs_path="reference/flink-options",
    )


def sql_gateway_not_configured() -> str:
    """Error when SQL Gateway URL is missing."""
    return format_error(
        title="SQL Gateway URL not configured",
        explanation="Flink SQL execution requires the SQL Gateway URL to be set. "
        "The REST URL is for job management, but SQL submission needs the SQL Gateway.",
        suggestion="Add sql_gateway_url to your Flink cluster configuration:",
        example="""runtime:
  flink:
    default: local
    clusters:
      local:
        rest_url: http://localhost:8081      # For job management
        sql_gateway_url: http://localhost:8083  # For SQL execution""",
        docs_path="reference/flink-options",
    )
