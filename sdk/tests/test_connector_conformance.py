from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, cast

import pytest

from ingestion_graph.conformance import (
    ConformanceReport,
    ConnectorConformanceError,
    inspect_destination_replay,
    inspect_manifest,
    inspect_secret_redaction,
    inspect_source_messages,
    inspect_source_read,
)
from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    Source,
    StreamDescriptor,
)
from ingestion_graph.destinations import JsonlDestination
from ingestion_graph.messages import RecordMessage, SchemaMessage, SourceMessage, StateMessage
from ingestion_graph.models import Envelope, Operation, RecordPayload, Tombstone
from ingestion_graph.sources import JsonlSource


def spec(**capabilities: bool) -> ConnectorSpec:
    return ConnectorSpec(
        name="example",
        version="1.0.0",
        config_schema={"type": "object", "properties": {}, "additionalProperties": False},
        capabilities=ConnectorCapabilities(**capabilities),
    )


def record(
    record_id: str = "record-1",
    *,
    source: str = "example",
    stream: str = "items",
    operation: Operation = Operation.UPSERT,
) -> Envelope:
    payload = Tombstone() if operation is Operation.DELETE else RecordPayload({"id": record_id})
    return Envelope(
        id=record_id,
        source=source,
        stream=stream,
        payload=payload,
        operation=operation,
    )


class FakeSource(Source):
    def __init__(
        self,
        connector_spec: ConnectorSpec,
        messages: Sequence[SourceMessage],
        *,
        check: CheckResult | None = None,
        discovered: Sequence[StreamDescriptor] | None = None,
    ) -> None:
        self.connector_spec = connector_spec
        self.messages = messages
        self.check_result = check or CheckResult(True)
        self.discovered = (
            list(discovered) if discovered is not None else [StreamDescriptor("items")]
        )

    def spec(self) -> ConnectorSpec:
        return self.connector_spec

    async def check(self) -> CheckResult:
        return self.check_result

    async def discover(self) -> Sequence[StreamDescriptor]:
        return self.discovered

    async def read(
        self,
        stream: StreamDescriptor,
        state: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[SourceMessage]:
        del stream, state
        for message in self.messages:
            yield message


class FakeDestination(Destination):
    idempotent = True

    def __init__(
        self,
        connector_spec: ConnectorSpec,
        *,
        counts: Sequence[int] = (1, 0),
        flush_error: Exception | None = None,
    ) -> None:
        self.connector_spec = connector_spec
        self.counts = list(counts)
        self.flush_error = flush_error
        self.flushes = 0

    def spec(self) -> ConnectorSpec:
        return self.connector_spec

    async def check(self) -> CheckResult:
        return CheckResult(True)

    async def write(self, records: Sequence[Envelope]) -> int:
        del records
        return self.counts.pop(0)

    async def flush(self) -> None:
        self.flushes += 1
        if self.flush_error is not None:
            raise self.flush_error


def test_report_is_plain_python_and_raises_all_errors() -> None:
    report = ConformanceReport("example")
    report.add("bad.one", "first")
    report.add("bad.two", "second", path="field")

    assert not report.ok
    with pytest.raises(ConnectorConformanceError, match="bad.one.*bad.two at field"):
        report.raise_for_errors()


def test_manifest_inspection_reports_schema_and_name_drift() -> None:
    connector_spec = ConnectorSpec(
        name="actual",
        version="",
        config_schema={
            "type": "object",
            "properties": {"known": {"type": "string"}},
            "required": ["missing"],
        },
    )

    report = inspect_manifest(connector_spec, expected_name="expected")

    assert {issue.code for issue in report.issues} == {
        "manifest.invalid",
        "manifest.name_mismatch",
        "manifest.unknown_required",
        "manifest.version",
    }


def test_direct_manifest_inspection_uses_canonical_validation() -> None:
    connector_spec = ConnectorSpec(
        name="example",
        version="1",
        config_schema={"type": "object", "properties": {"bad": "not-a-schema"}},
    )

    report = inspect_manifest(connector_spec)

    assert not report.ok
    assert "manifest.invalid" in {issue.code for issue in report.issues}


def test_source_capture_accepts_checkpointed_capability_consistent_messages() -> None:
    connector_spec = spec(incremental=True, schema_discovery=True)
    source = FakeSource(
        connector_spec,
        [
            SchemaMessage("items", {"type": "object"}, "1"),
            RecordMessage(record()),
            StateMessage("items", {"cursor": "1"}),
        ],
    )

    report = inspect_source_messages(
        source,
        StreamDescriptor("items", json_schema={"type": "object"}),
        source.messages,
    )

    assert report.ok


def test_source_capture_detects_identity_capability_and_checkpoint_violations() -> None:
    connector_spec = spec(incremental=True)
    source = FakeSource(
        connector_spec,
        [
            SchemaMessage("wrong", {}, "1"),
            StateMessage("items", {"cursor": "premature"}),
            RecordMessage(
                record(
                    source="other",
                    stream="wrong",
                    operation=Operation.DELETE,
                )
            ),
        ],
    )

    report = inspect_source_messages(source, StreamDescriptor("items"), source.messages)
    codes = {issue.code for issue in report.issues}

    assert {
        "source.envelope_source",
        "source.envelope_stream",
        "source.schema_stream",
        "source.undeclared_deletes",
        "source.undeclared_schema",
        "source.uncheckpointed_records",
    } <= codes


def test_every_record_bearing_source_requires_a_trailing_checkpoint() -> None:
    source = FakeSource(spec(), [RecordMessage(record())])

    report = inspect_source_messages(source, StreamDescriptor("items"), source.messages)

    assert "source.uncheckpointed_records" in {issue.code for issue in report.issues}


def test_malformed_source_spec_is_reported_instead_of_raised() -> None:
    malformed_capabilities = ConnectorSpec(
        name="example",
        version="1",
        config_schema={"type": "object", "properties": {}},
        capabilities=cast(Any, object()),
    )
    malformed = FakeSource(malformed_capabilities, [])

    capability_report = inspect_source_messages(
        malformed,
        StreamDescriptor("items"),
        [],
    )

    assert not capability_report.ok
    assert "manifest.invalid" in {issue.code for issue in capability_report.issues}

    wrong_type = FakeSource(cast(Any, {"name": "not-a-spec"}), [])
    type_report = inspect_source_messages(wrong_type, StreamDescriptor("items"), [])
    assert type_report.issues[0].code == "source.spec_type"


@pytest.mark.asyncio
async def test_source_read_is_bounded_and_reports_failed_checks() -> None:
    failed = FakeSource(spec(), [], check=CheckResult(False, "bad credentials"))
    failed_report = await inspect_source_read(failed, StreamDescriptor("items"))
    assert failed_report.issues[0].code == "source.check_failed"

    too_many = FakeSource(
        spec(),
        [RecordMessage(record(str(index))) for index in range(3)],
    )
    bounded_report = await inspect_source_read(
        too_many,
        StreamDescriptor("items"),
        max_messages=2,
    )
    assert bounded_report.issues[0].code == "source.read_limit"

    undiscovered = FakeSource(
        spec(),
        [],
        discovered=[StreamDescriptor("other")],
    )
    undiscovered_report = await inspect_source_read(
        undiscovered,
        StreamDescriptor("items"),
    )
    assert undiscovered_report.issues[0].code == "source.stream_not_discovered"


@pytest.mark.asyncio
async def test_destination_replay_accepts_new_then_duplicate_counts() -> None:
    destination = FakeDestination(spec(incremental=True), counts=(1, 0))

    report = await inspect_destination_replay(destination, [record()])

    assert report.ok
    assert destination.flushes == 2


@pytest.mark.asyncio
async def test_destination_replay_detects_count_replay_and_flush_failures() -> None:
    replayed = FakeDestination(spec(), counts=(2, 1))
    replay_report = await inspect_destination_replay(replayed, [record()])
    assert {issue.code for issue in replay_report.issues} == {
        "destination.replay",
        "destination.write_count",
    }

    failed_flush = FakeDestination(
        spec(),
        counts=(1, 0),
        flush_error=RuntimeError("disk unavailable"),
    )
    flush_report = await inspect_destination_replay(failed_flush, [record()])
    assert flush_report.issues[0].code == "destination.first_exception"


@pytest.mark.asyncio
async def test_destination_that_drops_every_record_cannot_pass() -> None:
    destination = FakeDestination(spec(), counts=(0, 0))

    report = await inspect_destination_replay(destination, [record()])

    assert "destination.first_write" in {issue.code for issue in report.issues}

    malformed = FakeDestination(cast(Any, {"name": "not-a-spec"}))
    malformed_report = await inspect_destination_replay(malformed, [record()])
    assert malformed_report.issues[0].code == "destination.spec_type"


@pytest.mark.asyncio
async def test_destination_delete_case_is_capability_gated() -> None:
    destination = FakeDestination(spec(deletes=False))

    report = await inspect_destination_replay(
        destination,
        [record(operation=Operation.DELETE)],
    )

    assert report.issues[-1].code == "destination.undeclared_deletes"
    assert destination.flushes == 0

    declared = FakeDestination(spec(deletes=True), counts=(1, 0))
    missing_expectation = await inspect_destination_replay(
        declared,
        [record(operation=Operation.DELETE)],
    )
    assert missing_expectation.issues[-1].code == "destination.delete_expectation"
    assert declared.flushes == 0

    prepared = FakeDestination(spec(deletes=True), counts=(1, 0))
    prepared_report = await inspect_destination_replay(
        prepared,
        [record(operation=Operation.DELETE)],
        expected_first_write=1,
    )
    assert prepared_report.ok


def test_secret_redaction_requires_explicit_values_and_representations() -> None:
    report = inspect_secret_redaction(
        {"token": "top-secret"},
        ["safe repr", "request failed with top-secret"],
        connector="example",
    )

    assert not report.ok
    assert report.issues[0].code == "secret.exposed"
    assert report.issues[0].path == "rendered[1]"


@pytest.mark.asyncio
async def test_builtin_jsonl_connectors_pass_the_public_kit(tmp_path) -> None:
    source_path = tmp_path / "source.jsonl"
    source_path.write_text('{"id":"one","value":1}\n', encoding="utf-8")
    source = JsonlSource(source_path)
    stream = (await source.discover())[0]

    source_report = await inspect_source_read(source, stream)
    destination_report = await inspect_destination_replay(
        JsonlDestination(tmp_path / "destination.jsonl"),
        [record()],
    )

    assert source_report.ok, source_report.issues
    assert destination_report.ok, destination_report.issues
