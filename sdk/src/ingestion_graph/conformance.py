"""Reusable, dependency-free connector conformance checks.

The checks in this module validate portable SDK protocol invariants. They do
not attempt to prove connector-specific cursor ordering, remote API behavior,
or physical tombstone storage.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    Source,
    StreamDescriptor,
)
from ingestion_graph.errors import PluginError
from ingestion_graph.messages import (
    LogMessage,
    RecordMessage,
    SchemaMessage,
    SourceMessage,
    StateMessage,
)
from ingestion_graph.models import Envelope, Operation
from ingestion_graph.plugins import load_connector_manifest, validate_connector_manifest


class ConformanceSeverity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True, slots=True)
class ConformanceIssue:
    code: str
    message: str
    severity: ConformanceSeverity = ConformanceSeverity.ERROR
    path: str | None = None


class ConnectorConformanceError(AssertionError):
    """Raised by :meth:`ConformanceReport.raise_for_errors`."""


@dataclass(slots=True)
class ConformanceReport:
    connector: str
    issues: list[ConformanceIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(issue.severity is ConformanceSeverity.ERROR for issue in self.issues)

    def add(
        self,
        code: str,
        message: str,
        *,
        severity: ConformanceSeverity = ConformanceSeverity.ERROR,
        path: str | None = None,
    ) -> None:
        self.issues.append(ConformanceIssue(code, message, severity, path))

    def extend(self, other: ConformanceReport) -> None:
        self.issues.extend(other.issues)

    def raise_for_errors(self) -> None:
        errors = [issue for issue in self.issues if issue.severity is ConformanceSeverity.ERROR]
        if not errors:
            return
        details = "; ".join(
            f"{issue.code}{f' at {issue.path}' if issue.path else ''}: {issue.message}"
            for issue in errors
        )
        raise ConnectorConformanceError(f"{self.connector} failed conformance: {details}")


def inspect_manifest(
    spec: ConnectorSpec,
    *,
    expected_name: str | None = None,
) -> ConformanceReport:
    """Inspect an already-loaded connector manifest without plugin discovery."""
    report = ConformanceReport(expected_name or _safe_name(spec))
    try:
        validate_connector_manifest(
            spec,
            kind="connector",
            entry_point_name=expected_name or _safe_name(spec),
        )
    except PluginError as exc:
        report.add("manifest.invalid", str(exc), path="manifest")
    if not isinstance(spec.name, str) or not spec.name.strip():
        report.add("manifest.name", "name must be a non-empty string", path="name")
    elif expected_name is not None and spec.name != expected_name:
        report.add(
            "manifest.name_mismatch",
            f"expected {expected_name!r}, received {spec.name!r}",
            path="name",
        )
    if not isinstance(spec.version, str) or not spec.version.strip():
        report.add("manifest.version", "version must be a non-empty string", path="version")
    capabilities: Any = spec.capabilities
    if not isinstance(capabilities, ConnectorCapabilities):
        report.add(
            "manifest.capabilities",
            "capabilities must be ConnectorCapabilities",
            path="capabilities",
        )
    else:
        for name in (
            "incremental",
            "resumable_full_refresh",
            "deletes",
            "schema_discovery",
            "rate_limits",
        ):
            if not isinstance(getattr(capabilities, name), bool):
                report.add(
                    "manifest.capability_type",
                    f"{name} must be a boolean",
                    path=f"capabilities.{name}",
                )
    _inspect_config_schema(spec.config_schema, report)
    return report


def inspect_installed_manifest(kind: str, name: str) -> ConformanceReport:
    """Load an installed entry point and inspect its constructor-free manifest."""
    try:
        spec = load_connector_manifest(kind, name)
    except Exception as exc:
        report = ConformanceReport(name)
        report.add("manifest.load", f"could not load {kind} manifest: {exc}")
        return report
    return inspect_manifest(spec, expected_name=name)


def inspect_source_messages(
    source: Source,
    stream: StreamDescriptor,
    messages: Sequence[SourceMessage],
) -> ConformanceReport:
    """Inspect a finite captured source read.

    Connector tests remain responsible for creating the configured source and
    capturing a deterministic read. Every record-bearing capture must finish
    with a state message, matching :class:`~ingestion_graph.pipeline.Pipeline`.
    """
    spec, report = _inspect_source_spec(source)
    if spec is None:
        return report
    return _inspect_source_messages_with_spec(source, spec, stream, messages, report)


def _inspect_source_messages_with_spec(
    source: Source,
    spec: ConnectorSpec,
    stream: StreamDescriptor,
    messages: Sequence[SourceMessage],
    report: ConformanceReport,
) -> ConformanceReport:
    del source
    capabilities: Any = spec.capabilities
    if not isinstance(capabilities, ConnectorCapabilities):
        return report
    if not stream.name:
        report.add("source.stream_name", "descriptor name must not be empty", path="stream.name")
    json_schema: Any = stream.json_schema
    if not isinstance(json_schema, Mapping):
        report.add(
            "source.stream_schema",
            "descriptor json_schema must be an object",
            path="stream.json_schema",
        )

    saw_record = False
    last_record_index = -1
    last_state_index = -1
    for index, message in enumerate(messages):
        path = f"messages[{index}]"
        message_value: Any = message
        if isinstance(message_value, RecordMessage):
            saw_record = True
            last_record_index = index
            envelope_value: Any = message_value.envelope
            if not isinstance(envelope_value, Envelope):
                report.add(
                    "source.envelope_type",
                    f"record envelope must be Envelope, received {type(envelope_value).__name__}",
                    path=f"{path}.envelope",
                )
                continue
            envelope = envelope_value
            if envelope.source != spec.name:
                report.add(
                    "source.envelope_source",
                    f"expected source {spec.name!r}, received {envelope.source!r}",
                    path=f"{path}.envelope.source",
                )
            if envelope.stream != stream.name:
                report.add(
                    "source.envelope_stream",
                    f"expected stream {stream.name!r}, received {envelope.stream!r}",
                    path=f"{path}.envelope.stream",
                )
            if not envelope.id:
                report.add(
                    "source.envelope_id",
                    "record IDs must not be empty",
                    path=f"{path}.envelope.id",
                )
            if envelope.operation is Operation.DELETE and not capabilities.deletes:
                report.add(
                    "source.undeclared_deletes",
                    "DELETE emitted while deletes capability is false",
                    path=f"{path}.envelope.operation",
                )
        elif isinstance(message_value, StateMessage):
            last_state_index = index
            if message_value.stream != stream.name:
                report.add(
                    "source.state_stream",
                    f"expected stream {stream.name!r}, received {message_value.stream!r}",
                    path=f"{path}.stream",
                )
            state_value: Any = message_value.state
            if not isinstance(state_value, Mapping):
                report.add("source.state_shape", "state must be an object", path=f"{path}.state")
        elif isinstance(message_value, SchemaMessage):
            if message_value.stream != stream.name:
                report.add(
                    "source.schema_stream",
                    f"expected stream {stream.name!r}, received {message_value.stream!r}",
                    path=f"{path}.stream",
                )
            if not capabilities.schema_discovery:
                report.add(
                    "source.undeclared_schema",
                    "SchemaMessage emitted while schema_discovery capability is false",
                    path=path,
                )
            schema_value: Any = message_value.schema
            if not isinstance(schema_value, Mapping):
                report.add(
                    "source.schema_shape",
                    "schema message payload must be an object",
                    path=f"{path}.schema",
                )
            if not isinstance(message_value.version, str) or not message_value.version.strip():
                report.add(
                    "source.schema_version",
                    "schema message version must be a non-empty string",
                    path=f"{path}.version",
                )
        elif isinstance(message_value, LogMessage):
            level_value: Any = message_value.level
            log_message_value: Any = message_value.message
            attributes_value: Any = message_value.attributes
            if not isinstance(level_value, str) or not level_value.strip():
                report.add(
                    "source.log_level",
                    "log level must be a non-empty string",
                    path=f"{path}.level",
                )
            if not isinstance(log_message_value, str):
                report.add(
                    "source.log_message",
                    "log message must be a string",
                    path=f"{path}.message",
                )
            if not isinstance(attributes_value, Mapping):
                report.add(
                    "source.log_attributes",
                    "log attributes must be an object",
                    path=f"{path}.attributes",
                )
        else:
            report.add(
                "source.message_type",
                f"unsupported message type {type(message_value).__name__}",
                path=path,
            )

    if saw_record and last_state_index < last_record_index:
        report.add(
            "source.uncheckpointed_records",
            "a record-bearing read must end its batch with a StateMessage",
            path="messages",
        )
    return report


async def inspect_source_read(
    source: Source,
    stream: StreamDescriptor,
    *,
    state: Mapping[str, Any] | None = None,
    max_messages: int = 10_000,
) -> ConformanceReport:
    """Capture one bounded read and inspect it; callers provide deterministic sources."""
    spec, report = _inspect_source_spec(source)
    if spec is None:
        return report
    try:
        check = await source.check()
    except Exception as exc:
        report.add("source.check_exception", f"check() raised {type(exc).__name__}: {exc}")
        return report
    check_value: Any = check
    if not isinstance(check_value, CheckResult):
        report.add(
            "source.check_type",
            f"check() returned {type(check).__name__}; expected CheckResult",
        )
        return report
    if not check_value.ok:
        report.add("source.check_failed", check_value.message or "check() returned ok=False")
        return report

    try:
        discovered_value: Any = await source.discover()
    except Exception as exc:
        report.add(
            "source.discover_exception",
            f"discover() raised {type(exc).__name__}: {exc}",
        )
        return report
    if (
        not isinstance(discovered_value, Sequence)
        or isinstance(discovered_value, (str, bytes))
        or any(not isinstance(item, StreamDescriptor) for item in discovered_value)
    ):
        report.add(
            "source.discover_type",
            "discover() must return a sequence of StreamDescriptor values",
        )
        return report
    discovered: Sequence[StreamDescriptor] = discovered_value
    if not any(
        item.name == stream.name and item.namespace == stream.namespace for item in discovered
    ):
        report.add(
            "source.stream_not_discovered",
            f"discover() did not return stream {stream.name!r}",
            path="stream",
        )
        return report

    messages: list[SourceMessage] = []
    try:
        async for message in source.read(stream, state):
            messages.append(message)
            if len(messages) > max_messages:
                report.add(
                    "source.read_limit",
                    f"read exceeded the configured {max_messages} message limit",
                )
                return report
    except Exception as exc:
        report.add("source.read_exception", f"read() raised {type(exc).__name__}: {exc}")
        return report
    return _inspect_source_messages_with_spec(source, spec, stream, messages, report)


async def inspect_destination_replay(
    destination: Destination,
    records: Sequence[Envelope],
    *,
    expected_first_write: int | None = None,
) -> ConformanceReport:
    """Exercise check/write/flush and one exact replay on a disposable destination.

    UPSERT-only cases default to every supplied record being newly written.
    DELETE cases require an explicit positive expectation after the caller has
    populated the disposable destination with the records that should be removed.
    """
    spec, report = _inspect_destination_spec(destination)
    if spec is None:
        return report
    capabilities: Any = spec.capabilities
    if not isinstance(capabilities, ConnectorCapabilities):
        return report
    if not destination.idempotent:
        report.add(
            "destination.idempotent",
            "Pipeline-compatible destinations must declare idempotent=True",
        )
    if not records:
        report.add(
            "destination.empty_fixture",
            "at least one record is required to exercise destination writes",
            path="records",
        )
        return report
    if any(record.operation is Operation.DELETE for record in records) and not (
        capabilities.deletes
    ):
        report.add(
            "destination.undeclared_deletes",
            "DELETE case supplied while deletes capability is false",
        )
        return report
    has_delete = any(record.operation is Operation.DELETE for record in records)
    if expected_first_write is None:
        if has_delete:
            report.add(
                "destination.delete_expectation",
                "DELETE cases require expected_first_write after destination setup",
            )
            return report
        expected_first_write = len(records)
    if (
        isinstance(expected_first_write, bool)
        or not isinstance(expected_first_write, int)
        or expected_first_write < 0
        or expected_first_write > len(records)
        or (records and expected_first_write == 0)
    ):
        report.add(
            "destination.first_write_expectation",
            "expected_first_write must be a positive count within the supplied records",
        )
        return report
    try:
        check = await destination.check()
    except Exception as exc:
        report.add("destination.check_exception", f"check() raised {type(exc).__name__}: {exc}")
        return report
    check_value: Any = check
    if not isinstance(check_value, CheckResult):
        report.add(
            "destination.check_type",
            f"check() returned {type(check).__name__}; expected CheckResult",
        )
        return report
    if not check_value.ok:
        report.add(
            "destination.check_failed",
            check_value.message or "check() returned ok=False",
        )
        return report

    first = await _write_and_flush(destination, records, report, phase="first")
    if first is None:
        return report
    if first < 0 or first > len(records):
        report.add(
            "destination.write_count",
            f"write() returned {first} for {len(records)} records",
            path="first_write",
        )
    elif first != expected_first_write:
        report.add(
            "destination.first_write",
            f"first write reported {first} newly written records; expected {expected_first_write}",
            path="first_write",
        )
    replay = await _write_and_flush(destination, records, report, phase="replay")
    if replay is None:
        return report
    if replay < 0 or replay > len(records):
        report.add(
            "destination.write_count",
            f"replay write() returned {replay} for {len(records)} records",
            path="replay_write",
        )
    if destination.idempotent and replay != 0:
        report.add(
            "destination.replay",
            f"exact replay reported {replay} newly written records; expected 0",
            path="replay_write",
        )
    return report


def inspect_secret_redaction(
    values: Mapping[str, str],
    rendered: Sequence[str],
    *,
    connector: str = "connector",
) -> ConformanceReport:
    """Check caller-selected representations against caller-supplied secret values.

    The kit cannot infer which configuration fields are secrets. Connector tests
    must explicitly provide the values and representations (for example repr,
    logs, errors, provenance, and serialized state) that matter for that plugin.
    """
    report = ConformanceReport(connector)
    for name, secret in values.items():
        if not secret:
            report.add(
                "secret.empty_fixture",
                f"secret fixture {name!r} is empty and cannot be checked",
                severity=ConformanceSeverity.WARNING,
            )
            continue
        for index, text in enumerate(rendered):
            if secret in text:
                report.add(
                    "secret.exposed",
                    f"secret {name!r} appears in rendered output",
                    path=f"rendered[{index}]",
                )
    return report


async def _write_and_flush(
    destination: Destination,
    records: Sequence[Envelope],
    report: ConformanceReport,
    *,
    phase: str,
) -> int | None:
    try:
        written = await destination.write(records)
        if isinstance(written, bool) or not isinstance(written, int):
            report.add(
                "destination.write_count_type",
                f"write() returned {type(written).__name__}; expected int",
                path=f"{phase}_write",
            )
            return None
        await destination.flush()
        return written
    except Exception as exc:
        report.add(
            f"destination.{phase}_exception",
            f"{phase} write/flush raised {type(exc).__name__}: {exc}",
        )
        return None


def _inspect_config_schema(schema: Mapping[str, Any], report: ConformanceReport) -> None:
    if not isinstance(schema, Mapping) or schema.get("type") != "object":
        report.add(
            "manifest.config_schema",
            "config_schema must be an object schema",
            path="config_schema",
        )
        return
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        report.add(
            "manifest.config_properties",
            "config_schema.properties must be an object",
            path="config_schema.properties",
        )
        return
    required = schema.get("required", [])
    if (
        not isinstance(required, Sequence)
        or isinstance(required, (str, bytes))
        or any(not isinstance(item, str) for item in required)
    ):
        report.add(
            "manifest.config_required",
            "config_schema.required must be an array of strings",
            path="config_schema.required",
        )
        return
    unknown = set(required) - set(properties)
    if unknown:
        report.add(
            "manifest.unknown_required",
            f"required fields are missing from properties: {', '.join(sorted(unknown))}",
            path="config_schema.required",
        )


def _safe_name(spec: ConnectorSpec) -> str:
    return spec.name if isinstance(spec.name, str) and spec.name else "connector"


def _inspect_source_spec(
    source: Source,
) -> tuple[ConnectorSpec | None, ConformanceReport]:
    connector = type(source).__name__
    report = ConformanceReport(connector)
    try:
        raw_spec: Any = source.spec()
    except Exception as exc:
        report.add("source.spec_exception", f"spec() raised {type(exc).__name__}: {exc}")
        return None, report
    if not isinstance(raw_spec, ConnectorSpec):
        report.add(
            "source.spec_type",
            f"spec() returned {type(raw_spec).__name__}; expected ConnectorSpec",
        )
        return None, report
    report = inspect_manifest(raw_spec)
    report.connector = raw_spec.name
    return raw_spec, report


def _inspect_destination_spec(
    destination: Destination,
) -> tuple[ConnectorSpec | None, ConformanceReport]:
    connector = type(destination).__name__
    report = ConformanceReport(connector)
    try:
        raw_spec: Any = destination.spec()
    except Exception as exc:
        report.add(
            "destination.spec_exception",
            f"spec() raised {type(exc).__name__}: {exc}",
        )
        return None, report
    if not isinstance(raw_spec, ConnectorSpec):
        report.add(
            "destination.spec_type",
            f"spec() returned {type(raw_spec).__name__}; expected ConnectorSpec",
        )
        return None, report
    report = inspect_manifest(raw_spec)
    report.connector = raw_spec.name
    return raw_spec, report


__all__ = [
    "ConformanceIssue",
    "ConformanceReport",
    "ConformanceSeverity",
    "ConnectorConformanceError",
    "inspect_destination_replay",
    "inspect_installed_manifest",
    "inspect_manifest",
    "inspect_secret_redaction",
    "inspect_source_messages",
    "inspect_source_read",
]
