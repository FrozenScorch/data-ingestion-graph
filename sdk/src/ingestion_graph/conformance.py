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
from ingestion_graph.messages import RecordMessage, SchemaMessage, SourceMessage, StateMessage
from ingestion_graph.models import Envelope, Operation
from ingestion_graph.plugins import load_connector_manifest


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
    capturing a deterministic read. Stateful connectors declare that contract
    through ``incremental`` or ``resumable_full_refresh`` and must finish a
    record-bearing capture with a state message.
    """
    spec = source.spec()
    report = inspect_manifest(spec)
    report.connector = spec.name
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
        if isinstance(message, RecordMessage):
            saw_record = True
            last_record_index = index
            envelope = message.envelope
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
            if envelope.operation is Operation.DELETE and not spec.capabilities.deletes:
                report.add(
                    "source.undeclared_deletes",
                    "DELETE emitted while deletes capability is false",
                    path=f"{path}.envelope.operation",
                )
        elif isinstance(message, StateMessage):
            last_state_index = index
            if message.stream != stream.name:
                report.add(
                    "source.state_stream",
                    f"expected stream {stream.name!r}, received {message.stream!r}",
                    path=f"{path}.stream",
                )
            state_value: Any = message.state
            if not isinstance(state_value, Mapping):
                report.add("source.state_shape", "state must be an object", path=f"{path}.state")
        elif isinstance(message, SchemaMessage):
            if message.stream != stream.name:
                report.add(
                    "source.schema_stream",
                    f"expected stream {stream.name!r}, received {message.stream!r}",
                    path=f"{path}.stream",
                )
            if not spec.capabilities.schema_discovery:
                report.add(
                    "source.undeclared_schema",
                    "SchemaMessage emitted while schema_discovery capability is false",
                    path=path,
                )

    stateful = spec.capabilities.incremental or spec.capabilities.resumable_full_refresh
    if stateful and saw_record and last_state_index < last_record_index:
        report.add(
            "source.uncheckpointed_records",
            "a stateful read must end its record batch with a StateMessage",
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
    report = ConformanceReport(_source_name(source))
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
        discovered = await source.discover()
    except Exception as exc:
        report.add(
            "source.discover_exception",
            f"discover() raised {type(exc).__name__}: {exc}",
        )
        return report
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
    report.extend(inspect_source_messages(source, stream, messages))
    return report


async def inspect_destination_replay(
    destination: Destination,
    records: Sequence[Envelope],
) -> ConformanceReport:
    """Exercise check/write/flush and one exact replay on a disposable destination."""
    spec = destination.spec()
    report = inspect_manifest(spec)
    report.connector = spec.name
    if not destination.idempotent:
        report.add(
            "destination.idempotent",
            "Pipeline-compatible destinations must declare idempotent=True",
        )
    if any(record.operation is Operation.DELETE for record in records) and not (
        spec.capabilities.deletes
    ):
        report.add(
            "destination.undeclared_deletes",
            "DELETE case supplied while deletes capability is false",
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


def _source_name(source: Source) -> str:
    try:
        return _safe_name(source.spec())
    except Exception:
        return type(source).__name__


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
