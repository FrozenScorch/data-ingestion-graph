"""Content-addressed storage for raw or large artifacts."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

from ingestion_graph.models import BlobRef


class ArtifactStore(ABC):
    @abstractmethod
    async def put(self, data: bytes, media_type: str = "application/octet-stream") -> BlobRef: ...

    @abstractmethod
    async def get(self, reference: BlobRef) -> bytes: ...


class LocalArtifactStore(ArtifactStore):
    def __init__(self, root: str | Path = ".ingestion/artifacts") -> None:
        self.root = Path(root).resolve()

    async def put(self, data: bytes, media_type: str = "application/octet-stream") -> BlobRef:
        return await asyncio.to_thread(self._put_sync, data, media_type)

    def _put_sync(self, data: bytes, media_type: str) -> BlobRef:
        digest = hashlib.sha256(data).hexdigest()
        target = self.root / digest[:2] / digest[2:]
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            descriptor, temp_name = tempfile.mkstemp(dir=target.parent, prefix=".artifact-")
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, target)
            finally:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)
        return BlobRef(
            uri=target.as_uri(), sha256=digest, size_bytes=len(data), media_type=media_type
        )

    async def get(self, reference: BlobRef) -> bytes:
        return await asyncio.to_thread(self._get_sync, reference)

    def _get_sync(self, reference: BlobRef) -> bytes:
        if len(reference.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in reference.sha256.lower()
        ):
            raise ValueError("Artifact reference contains an invalid SHA-256 digest")
        path = self.root / reference.sha256[:2] / reference.sha256[2:]
        if path.as_uri() != reference.uri:
            raise ValueError("Artifact URI does not match this store")
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != reference.sha256:
            raise ValueError("Artifact checksum mismatch")
        return data
