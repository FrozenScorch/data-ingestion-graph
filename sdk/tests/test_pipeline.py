from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import replace

import pytest

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
    Source,
    StreamDescriptor,
)
from ingestion_graph.destinations import JsonlDestination
from ingestion_graph.errors import ProtocolError
from ingestion_graph.messages import RecordMessage, SourceMessage, StateMessage
from ingestion_graph.models import Envelope, RecordPayload
from ingestion_graph.pipeline import Pipeline
from ingestion_graph.state import MemoryStateStore
from ingestion_graph.transforms import Transform


class ExampleSource(Source):
    def __init__(self, checkpoint: bool = True):
        self.checkpoint = checkpoint

    def spec(self):
        return ConnectorSpec("example", "1", {}, ConnectorCapabilities(incremental=True))

    async def check(self):
        return CheckResult(True)

    async def discover(self) -> Sequence[StreamDescriptor]:
        return [StreamDescriptor("items")]

    async def read(
        self, stream: StreamDescriptor, state: Mapping | None = None
    ) -> AsyncIterator[SourceMessage]:
        if state and state.get("cursor") == "2":
            yield StateMessage("items", state)
            return
        for item in (1, 2):
            yield RecordMessage(
                Envelope(
                    id=str(item),
                    source="example",
                    stream="items",
                    cursor=str(item),
                    payload=RecordPayload({"value": item}),
                )
            )
        if self.checkpoint:
            yield StateMessage("items", {"cursor": "2"})


class FailingCheckSource(ExampleSource):
    def __init__(self):
        super().__init__()
        self.closed = False

    async def check(self):
        return CheckResult(False, "source unavailable")

    async def close(self):
        self.closed = True


class FailingDestination(Destination):
    idempotent = True

    async def check(self):
        return CheckResult(True)

    async def write(self, records):
        raise RuntimeError("destination unavailable")

    async def flush(self):
        return None


class RecordingDestination(Destination):
    idempotent = True

    def __init__(self):
        self.records: list[Envelope] = []
        self.write_calls = 0
        self.flush_calls = 0
        self.closed = False

    async def check(self):
        return CheckResult(True)

    async def write(self, records):
        self.write_calls += 1
        self.records.extend(records)
        return len(records)

    async def flush(self):
        self.flush_calls += 1

    async def close(self):
        self.closed = True


class AddOne(Transform):
    def __init__(self):
        self.closed = False

    async def apply(self, records):
        return [
            replace(record, payload=RecordPayload({"value": record.payload.data["value"] + 1}))
            for record in records
            if isinstance(record.payload, RecordPayload)
        ]

    async def close(self):
        self.closed = True


class MultiplyByTen(Transform):
    async def apply(self, records):
        return [
            replace(record, payload=RecordPayload({"value": record.payload.data["value"] * 10}))
            for record in records
            if isinstance(record.payload, RecordPayload)
        ]


class DropAll(Transform):
    async def apply(self, records):
        return []


class DuplicateEach(Transform):
    async def apply(self, records):
        return [
            expanded
            for record in records
            for expanded in (record, replace(record, id=f"{record.id}-copy"))
        ]


class FailingTransform(Transform):
    async def apply(self, records):
        raise RuntimeError("transform unavailable")


class InvalidTransform(Transform):
    async def apply(self, records):
        return None


class FailingCloseTransform(FailingTransform):
    async def close(self):
        raise RuntimeError("cleanup unavailable")


class MoveStream(Transform):
    async def apply(self, records):
        return [replace(record, stream="other") for record in records]


@pytest.mark.asyncio
async def test_pipeline_commits_after_destination_flush_and_deduplicates(tmp_path):
    state = MemoryStateStore()
    destination = JsonlDestination(tmp_path / "items.jsonl")
    result = await Pipeline("example", ExampleSource(), destination, state_store=state).run()
    assert result.records_written == 2
    assert await state.load("example", "example", "items") == {"cursor": "2"}

    second = await Pipeline(
        "example", ExampleSource(), JsonlDestination(tmp_path / "items.jsonl"), state_store=state
    ).run()
    assert second.records_written == 0
    assert len((tmp_path / "items.jsonl").read_text(encoding="utf-8").splitlines()) == 2


@pytest.mark.asyncio
async def test_pipeline_rejects_uncheckpointed_records(tmp_path):
    with pytest.raises(ProtocolError, match="uncheckpointed"):
        await Pipeline(
            "example",
            ExampleSource(checkpoint=False),
            JsonlDestination(tmp_path / "items.jsonl"),
            state_store=MemoryStateStore(),
        ).run()


@pytest.mark.asyncio
async def test_pipeline_does_not_advance_state_when_destination_fails():
    state = MemoryStateStore()
    with pytest.raises(RuntimeError, match="destination unavailable"):
        await Pipeline("example", ExampleSource(), FailingDestination(), state_store=state).run()
    assert await state.load("example", "example", "items") == {}


@pytest.mark.asyncio
async def test_pipeline_closes_all_resources_when_preflight_fails():
    source = FailingCheckSource()
    destination = RecordingDestination()
    transform = AddOne()

    with pytest.raises(ProtocolError, match="Source check failed: source unavailable"):
        await Pipeline(
            "example",
            source,
            destination,
            transforms=[transform],
            state_store=MemoryStateStore(),
        ).run()

    assert source.closed is True
    assert destination.closed is True
    assert transform.closed is True


@pytest.mark.asyncio
async def test_pipeline_applies_transforms_in_order_before_flush_and_checkpoint():
    state = MemoryStateStore()
    destination = RecordingDestination()
    add_one = AddOne()

    result = await Pipeline(
        "example",
        ExampleSource(),
        destination,
        transforms=[add_one, MultiplyByTen()],
        state_store=state,
    ).run()

    assert [record.payload.data["value"] for record in destination.records] == [20, 30]
    assert destination.flush_calls == 1
    assert result.records_written == 2
    assert await state.load("example", "example", "items") == {"cursor": "2"}
    assert add_one.closed is True


@pytest.mark.asyncio
async def test_pipeline_can_filter_entire_batch_and_still_checkpoint():
    state = MemoryStateStore()
    destination = RecordingDestination()

    result = await Pipeline(
        "example",
        ExampleSource(),
        destination,
        transforms=[DropAll()],
        state_store=state,
    ).run()

    assert destination.write_calls == 0
    assert destination.flush_calls == 1
    assert result.records_written == 0
    assert await state.load("example", "example", "items") == {"cursor": "2"}


@pytest.mark.asyncio
async def test_pipeline_can_expand_a_checkpoint_batch():
    destination = RecordingDestination()

    result = await Pipeline(
        "example",
        ExampleSource(),
        destination,
        transforms=[DuplicateEach()],
        state_store=MemoryStateStore(),
    ).run()

    assert [record.id for record in destination.records] == ["1", "1-copy", "2", "2-copy"]
    assert result.records_written == 4


@pytest.mark.asyncio
async def test_pipeline_does_not_advance_state_when_transform_fails():
    state = MemoryStateStore()
    destination = RecordingDestination()
    add_one = AddOne()

    with pytest.raises(RuntimeError, match="transform unavailable"):
        await Pipeline(
            "example",
            ExampleSource(),
            destination,
            transforms=[add_one, FailingTransform()],
            state_store=state,
        ).run()

    assert destination.write_calls == 0
    assert await state.load("example", "example", "items") == {}
    assert add_one.closed is True


@pytest.mark.asyncio
async def test_pipeline_rejects_transform_output_for_another_stream():
    state = MemoryStateStore()

    with pytest.raises(ProtocolError, match="moved a record"):
        await Pipeline(
            "example",
            ExampleSource(),
            RecordingDestination(),
            transforms=[MoveStream()],
            state_store=state,
        ).run()

    assert await state.load("example", "example", "items") == {}


@pytest.mark.asyncio
async def test_pipeline_rejects_non_iterable_transform_output():
    with pytest.raises(ProtocolError, match="non-iterable"):
        await Pipeline(
            "example",
            ExampleSource(),
            RecordingDestination(),
            transforms=[InvalidTransform()],
            state_store=MemoryStateStore(),
        ).run()


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_mask_transform_failure():
    with pytest.raises(RuntimeError, match="transform unavailable") as exc_info:
        await Pipeline(
            "example",
            ExampleSource(),
            RecordingDestination(),
            transforms=[FailingCloseTransform()],
            state_store=MemoryStateStore(),
        ).run()

    assert any("cleanup unavailable" in note for note in exc_info.value.__notes__)


@pytest.mark.asyncio
async def test_jsonl_deduplicates_even_if_state_is_lost(tmp_path):
    output = tmp_path / "items.jsonl"
    first = await Pipeline(
        "first",
        ExampleSource(),
        JsonlDestination(output),
        state_store=MemoryStateStore(),
    ).run()
    replay = await Pipeline(
        "replay",
        ExampleSource(),
        JsonlDestination(output),
        state_store=MemoryStateStore(),
    ).run()
    assert first.records_written == 2
    assert replay.records_written == 0
    assert len(output.read_text(encoding="utf-8").splitlines()) == 2


@pytest.mark.asyncio
async def test_jsonl_appends_changed_upsert_but_deduplicates_exact_replay(tmp_path):
    output = tmp_path / "versions.jsonl"
    destination = JsonlDestination(output)
    original = Envelope(
        id="same-id",
        source="example",
        stream="items",
        cursor="1",
        payload=RecordPayload({"value": "original"}),
    )
    edited = Envelope(
        id="same-id",
        source="example",
        stream="items",
        cursor="1",
        payload=RecordPayload({"value": "edited"}),
    )
    assert await destination.write([original]) == 1
    assert await destination.write([original]) == 0
    assert await destination.write([edited]) == 1
    assert len(output.read_text(encoding="utf-8").splitlines()) == 2


@pytest.mark.asyncio
async def test_jsonl_does_not_cache_versions_before_fsync(tmp_path, monkeypatch):
    output = tmp_path / "durability.jsonl"
    destination = JsonlDestination(output)
    record = Envelope(
        id="durable-id",
        source="example",
        stream="items",
        cursor="1",
        payload=RecordPayload({"value": "retry-me"}),
    )

    def fail_fsync(_fd):
        raise OSError("durability unavailable")

    monkeypatch.setattr("ingestion_graph.destinations.jsonl.os.fsync", fail_fsync)
    with pytest.raises(OSError, match="durability unavailable"):
        await destination.write([record])

    # Simulate loss of the non-durable append, then retry on the same instance.
    output.write_text("", encoding="utf-8")
    monkeypatch.undo()
    assert await destination.write([record]) == 1
    assert len(output.read_text(encoding="utf-8").splitlines()) == 1
