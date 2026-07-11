"""Messages exchanged between a source and the embedded runtime."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from ingestion_graph.models import Envelope


class MessageKind(StrEnum):
    RECORD = "record"
    STATE = "state"
    SCHEMA = "schema"
    LOG = "log"


@dataclass(frozen=True, slots=True)
class RecordMessage:
    envelope: Envelope
    kind: MessageKind = field(default=MessageKind.RECORD, init=False)


@dataclass(frozen=True, slots=True)
class StateMessage:
    stream: str
    state: Mapping[str, Any]
    kind: MessageKind = field(default=MessageKind.STATE, init=False)


@dataclass(frozen=True, slots=True)
class SchemaMessage:
    stream: str
    schema: Mapping[str, Any]
    version: str
    kind: MessageKind = field(default=MessageKind.SCHEMA, init=False)


@dataclass(frozen=True, slots=True)
class LogMessage:
    level: str
    message: str
    attributes: Mapping[str, Any] = field(default_factory=dict)
    kind: MessageKind = field(default=MessageKind.LOG, init=False)


SourceMessage = RecordMessage | StateMessage | SchemaMessage | LogMessage
