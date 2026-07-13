"""Idempotent local JSONL destination for embedded pipelines."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ingestion_graph.connectors.base import (
    CheckResult,
    ConnectorCapabilities,
    ConnectorSpec,
    Destination,
)
from ingestion_graph.models import Envelope


class JsonlDestination(Destination):
    idempotent = True

    @classmethod
    def manifest(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name="jsonl",
            version="1.0.0",
            config_schema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Path to the durable append-only JSONL output.",
                    }
                },
                "required": ["path"],
                "additionalProperties": False,
            },
            capabilities=ConnectorCapabilities(
                incremental=True,
                resumable_full_refresh=True,
                deletes=True,
            ),
        )

    def spec(self) -> ConnectorSpec:
        return self.manifest()

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._known_versions: set[tuple[str, str]] | None = None
        self._lock = asyncio.Lock()

    async def check(self) -> CheckResult:
        try:
            await asyncio.to_thread(self.path.parent.mkdir, parents=True, exist_ok=True)
            return CheckResult(True)
        except OSError as exc:
            return CheckResult(False, str(exc))

    @staticmethod
    def _version_key(item: dict[str, Any]) -> tuple[str, str]:
        semantic = dict(item)
        # These describe this observation, not the source-side record version.
        semantic.pop("observed_at", None)
        semantic.pop("provenance", None)
        encoded = json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
        return str(item["id"]), hashlib.sha256(encoded).hexdigest()

    def _load_versions(self) -> set[tuple[str, str]]:
        versions: set[tuple[str, str]] = set()
        if not self.path.exists():
            return versions
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    item = json.loads(line)
                    if isinstance(item.get("id"), str):
                        versions.add(self._version_key(item))
        return versions

    async def write(self, records: Sequence[Envelope]) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._write_sync, list(records))

    def _write_sync(self, records: list[Envelope]) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self._known_versions is None:
            self._known_versions = self._load_versions()
        written = 0
        pending_versions: set[tuple[str, str]] = set()
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            for record in records:
                serialized = record.to_dict()
                version_key = self._version_key(serialized)
                if version_key in self._known_versions or version_key in pending_versions:
                    continue
                handle.write(json.dumps(serialized, sort_keys=True, separators=(",", ":")))
                handle.write("\n")
                pending_versions.add(version_key)
                written += 1
            handle.flush()
            os.fsync(handle.fileno())
        self._known_versions.update(pending_versions)
        return written

    async def flush(self) -> None:
        return None
