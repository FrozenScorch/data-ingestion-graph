"""Schema-validated, provider-neutral vision table responses."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any

from ingestion_graph.document_ai.models import JSONValue, TableArtifact
from ingestion_graph.errors import ConfigurationError

VISION_TABLE_SCHEMA_VERSION = "1"
VISION_TABLE_RESPONSE_SCHEMA: dict[str, JSONValue] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://ingestion-graph.dev/schemas/vision-table-response-v1.json",
    "type": "object",
    "required": ["table_artifacts"],
    "properties": {
        "table_artifacts": {
            "type": "array",
            "maxItems": 256,
            "items": {
                "type": "object",
                "required": [
                    "schema_version",
                    "table_id",
                    "row_count",
                    "column_count",
                    "cells",
                ],
                "properties": {
                    "schema_version": {"const": VISION_TABLE_SCHEMA_VERSION},
                    "table_id": {"type": "string", "minLength": 1, "maxLength": 512},
                    "page_number": {"type": ["integer", "null"], "minimum": 1},
                    "row_count": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "column_count": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "confidence": {"type": ["number", "null"], "minimum": 0, "maximum": 1},
                    "cells": {
                        "type": "array",
                        "maxItems": 250000,
                        "items": {
                            "type": "object",
                            "required": ["row", "column", "text"],
                            "properties": {
                                "row": {"type": "integer", "minimum": 0},
                                "column": {"type": "integer", "minimum": 0},
                                "text": {"type": "string"},
                                "rowspan": {"type": "integer", "minimum": 1},
                                "colspan": {"type": "integer", "minimum": 1},
                                "header_level": {"type": ["integer", "null"], "minimum": 0},
                                "confidence": {
                                    "type": ["number", "null"],
                                    "minimum": 0,
                                    "maximum": 1,
                                },
                            },
                            "additionalProperties": True,
                        },
                    },
                },
                "additionalProperties": True,
            },
        }
    },
    "additionalProperties": False,
}


def validate_vision_table_response(value: Mapping[str, Any]) -> tuple[TableArtifact, ...]:
    """Validate provider output locally and normalize it to SDK table artifacts."""
    try:
        jsonschema = importlib.import_module("jsonschema")
    except (ImportError, ModuleNotFoundError) as exc:
        raise ConfigurationError(
            "Vision response validation requires: pip install 'ingestion-graph[vision]'"
        ) from exc
    try:
        jsonschema.Draft202012Validator(VISION_TABLE_RESPONSE_SCHEMA).validate(value)
        raw_artifacts = value["table_artifacts"]
        if not isinstance(raw_artifacts, list):
            raise ValueError("table_artifacts must be a list")
        return tuple(TableArtifact.from_dict(item) for item in raw_artifacts)
    except Exception as exc:
        raise ConfigurationError("Vision extractor returned invalid table artifacts") from exc


__all__ = [
    "VISION_TABLE_RESPONSE_SCHEMA",
    "VISION_TABLE_SCHEMA_VERSION",
    "validate_vision_table_response",
]
