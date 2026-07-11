import pytest

from ingestion_graph import (
    Envelope,
    LocalArtifactStore,
    Operation,
    RecordPayload,
    SecretRef,
    SecretValue,
    SQLiteStateStore,
    Tombstone,
    stable_record_id,
)
from ingestion_graph.secrets import EnvSecretProvider


def test_stable_record_id_is_deterministic_and_namespaced():
    assert stable_record_id("discord", "one", "42") == stable_record_id("discord", "one", "42")
    assert stable_record_id("discord", "one", "42") != stable_record_id("discord", "two", "42")


def test_envelope_serializes_payload_kind():
    envelope = Envelope(
        id="id", source="source", stream="stream", payload=RecordPayload({"answer": 42})
    )
    serialized = envelope.to_dict()
    assert serialized["operation"] == "upsert"
    assert serialized["payload"] == {"data": {"answer": 42}, "kind": "record"}


def test_delete_requires_tombstone():
    with pytest.raises(ValueError, match="Tombstone"):
        Envelope(
            id="id",
            source="source",
            stream="stream",
            payload=RecordPayload({}),
            operation=Operation.DELETE,
        )
    assert Envelope(
        id="id",
        source="source",
        stream="stream",
        payload=Tombstone("removed upstream"),
        operation=Operation.DELETE,
    )


def test_secret_values_are_redacted():
    provider = EnvSecretProvider({"TOKEN": "super-secret"})
    value = provider.resolve(SecretRef("TOKEN"))
    assert isinstance(value, SecretValue)
    assert str(value) == "super-secret"
    assert "super-secret" not in repr(value)


@pytest.mark.asyncio
async def test_sqlite_state_round_trip(tmp_path):
    store = SQLiteStateStore(tmp_path / "state.db")
    await store.save("pipe", "source", "stream", {"cursor": "42", "nested": {"x": 1}})
    assert await store.load("pipe", "source", "stream") == {
        "cursor": "42",
        "nested": {"x": 1},
    }


@pytest.mark.asyncio
async def test_local_artifacts_are_content_addressed(tmp_path):
    store = LocalArtifactStore(tmp_path / "artifacts")
    first = await store.put(b"hello", "text/plain")
    second = await store.put(b"hello", "text/plain")
    assert first.uri == second.uri
    assert first.sha256 == second.sha256
    assert await store.get(first) == b"hello"

    forged = type(first)(
        uri=(tmp_path / "elsewhere").resolve().as_uri(),
        sha256=first.sha256,
        size_bytes=first.size_bytes,
        media_type=first.media_type,
    )
    with pytest.raises(ValueError, match="does not match"):
        await store.get(forged)
