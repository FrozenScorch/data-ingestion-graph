"""Zero-infrastructure embedded pipeline runtime."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from ingestion_graph.connectors.base import Destination, Source, StreamDescriptor
from ingestion_graph.errors import ProtocolError
from ingestion_graph.messages import LogMessage, RecordMessage, SchemaMessage, StateMessage
from ingestion_graph.models import Envelope
from ingestion_graph.state import SQLiteStateStore, StateStore

EventHandler = Callable[[object], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class PipelineResult:
    pipeline: str
    streams_processed: int
    records_written: int
    checkpoints_committed: int


class Pipeline:
    def __init__(
        self,
        name: str,
        source: Source,
        destination: Destination,
        *,
        state_store: StateStore | None = None,
        on_event: EventHandler | None = None,
    ) -> None:
        if not name:
            raise ValueError("Pipeline name must not be empty")
        if not destination.idempotent:
            raise ValueError("Destinations must declare idempotent=True for resumable pipelines")
        self.name = name
        self.source = source
        self.destination = destination
        self.state_store = state_store or SQLiteStateStore()
        self.on_event = on_event

    async def run(self, streams: Sequence[StreamDescriptor] | None = None) -> PipelineResult:
        source_name = self.source.spec().name
        source_check = await self.source.check()
        if not source_check.ok:
            raise ProtocolError(f"Source check failed: {source_check.message}")
        destination_check = await self.destination.check()
        if not destination_check.ok:
            raise ProtocolError(f"Destination check failed: {destination_check.message}")

        selected_streams = list(streams or await self.source.discover())
        records_written = 0
        checkpoints = 0
        try:
            for stream in selected_streams:
                state = await self.state_store.load(self.name, source_name, stream.name)
                pending: list[Envelope] = []
                saw_checkpoint_after_record = True
                async for message in self.source.read(stream, state):
                    if isinstance(message, RecordMessage):
                        if message.envelope.stream != stream.name:
                            raise ProtocolError(
                                f"Source emitted stream {message.envelope.stream!r} "
                                f"while reading {stream.name!r}"
                            )
                        pending.append(message.envelope)
                        saw_checkpoint_after_record = False
                    elif isinstance(message, StateMessage):
                        if message.stream != stream.name:
                            raise ProtocolError("State checkpoint belongs to a different stream")
                        if pending:
                            newly_written = await self.destination.write(pending)
                            if not 0 <= newly_written <= len(pending):
                                raise ProtocolError(
                                    "Destination returned an invalid newly-written record count"
                                )
                            await self.destination.flush()
                            records_written += newly_written
                            pending.clear()
                        await self.state_store.save(
                            self.name, source_name, stream.name, message.state
                        )
                        checkpoints += 1
                        saw_checkpoint_after_record = True
                    elif isinstance(message, (SchemaMessage, LogMessage)) and self.on_event:
                        await self.on_event(message)
                    elif not isinstance(message, (SchemaMessage, LogMessage)):
                        raise ProtocolError(
                            f"Source emitted unsupported message type {type(message).__name__}"
                        )

                if pending or not saw_checkpoint_after_record:
                    raise ProtocolError(
                        f"Source {source_name!r} ended stream {stream.name!r} "
                        "with uncheckpointed records"
                    )
        finally:
            await self.destination.close()
            await self.source.close()

        return PipelineResult(
            pipeline=self.name,
            streams_processed=len(selected_streams),
            records_written=records_written,
            checkpoints_committed=checkpoints,
        )
