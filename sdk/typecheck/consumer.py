"""External-consumer typing smoke test, run after installing the built wheel."""

from collections.abc import Sequence

from ingestion_graph import (
    Envelope,
    QueryRequest,
    QueryResult,
    RecordPayload,
    Transform,
    stable_record_id,
)

record: Envelope = Envelope(
    id=stable_record_id("example", "items", "1"),
    source="example",
    stream="items",
    payload=RecordPayload({"value": 1}),
)
serialized: dict[str, object] = record.to_dict()
request: QueryRequest = QueryRequest("example", stream="items")


class KeepAll(Transform):
    async def apply(self, records: Sequence[Envelope]) -> Sequence[Envelope]:
        return records


transform: Transform = KeepAll()


def first_match(result: QueryResult) -> Envelope:
    return result[0].envelope
