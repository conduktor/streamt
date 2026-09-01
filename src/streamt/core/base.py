"""Shared Pydantic configuration for the streamt DSL."""

from pydantic import BaseModel, ConfigDict


class StreamtBaseModel(BaseModel):
    """Base class for user-authored streamt configuration.

    Configuration is intentionally strict: accepting a misspelled or unsupported
    field would make a project appear valid while silently discarding the user's
    intent.
    """

    model_config = ConfigDict(extra="forbid")
