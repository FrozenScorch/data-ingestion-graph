from collections.abc import AsyncIterator, Mapping, Sequence

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


class FailingDestination(Destination):
    idempotent = True

    async def check(self):
        return CheckResult(True)

    async def write(self, records):
        raise RuntimeError("destination unavailable")

    async def flush(self):
        return None


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
