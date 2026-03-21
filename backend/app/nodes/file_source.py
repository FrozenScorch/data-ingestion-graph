"""
FileSource node: file upload/glob input node.

Supports three source types:
- "upload": list files from the upload directory
- "glob": match files using a glob pattern
- "path": read a single file by path
"""
import logging
import mimetypes
from pathlib import Path
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class FileSourceNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "file_source"

    @property
    def display_name(self) -> str:
        return "File Source"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Read files from upload directory, glob pattern, or single file path"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="file_list", data_type=PortDataType.FILE_LIST, label="File List")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "source_type": {
                    "type": "string",
                    "enum": ["upload", "glob", "path"],
                    "default": "upload",
                },
                "file_pattern": {
                    "type": "string",
                    "default": "**/*",
                },
                "recursive": {
                    "type": "boolean",
                    "default": True,
                },
                "base_dir": {
                    "type": "string",
                    "description": "Base directory for file resolution (defaults to working_dir/uploads)",
                },
            },
            "required": ["source_type"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Read files based on source_type configuration."""
        source_type = context.config.get("source_type", "upload")
        file_pattern = context.config.get("file_pattern", "**/*")
        recursive = context.config.get("recursive", True)

        # Determine base directory
        base_dir = context.config.get("base_dir")
        if not base_dir:
            base_dir = str(Path(context.working_dir) / "uploads")

        base_path = Path(base_dir)

        file_list: list[dict[str, Any]] = []

        try:
            if source_type == "upload":
                # List all files in the upload directory matching the pattern
                if not base_path.exists():
                    logger.warning(f"Upload directory does not exist: {base_path}")
                    return NodeResult(
                        success=True,
                        output_data={"file_list": []},
                        items_processed=0,
                        metadata={"source_type": "upload", "warning": "Upload directory does not exist"},
                    )

                if recursive:
                    matched = sorted(base_path.glob(file_pattern))
                else:
                    matched = sorted(base_path.glob(file_pattern))

                # Filter to files only (not directories)
                for p in matched:
                    if p.is_file():
                        file_list.append(self._file_metadata(p))

            elif source_type == "glob":
                # Use the pattern against the base directory
                if not base_path.exists():
                    logger.warning(f"Base directory does not exist: {base_path}")
                    return NodeResult(
                        success=True,
                        output_data={"file_list": []},
                        items_processed=0,
                        metadata={"source_type": "glob", "warning": "Base directory does not exist"},
                    )

                if recursive:
                    matched = sorted(base_path.glob(file_pattern))
                else:
                    # Non-recursive: only match in the top-level directory
                    matched = sorted(Path(base_path).glob(file_pattern))

                for p in matched:
                    if p.is_file():
                        file_list.append(self._file_metadata(p))

            elif source_type == "path":
                # Single file path (absolute or relative to base_dir)
                file_path_str = context.config.get("file_path", context.config.get("file_pattern", ""))
                if not file_path_str:
                    return NodeResult(
                        success=False,
                        output_data={"file_list": []},
                        items_processed=0,
                        error_message="No file_path provided for path source_type",
                    )

                file_path = Path(file_path_str)
                if not file_path.is_absolute():
                    file_path = base_path / file_path

                if not file_path.exists() or not file_path.is_file():
                    return NodeResult(
                        success=False,
                        output_data={"file_list": []},
                        items_processed=0,
                        error_message=f"File not found: {file_path}",
                    )

                file_list.append(self._file_metadata(file_path))

            else:
                return NodeResult(
                    success=False,
                    output_data={"file_list": []},
                    items_processed=0,
                    error_message=f"Unknown source_type: {source_type}",
                )

        except Exception as e:
            logger.exception(f"FileSourceNode error: {e}")
            return NodeResult(
                success=False,
                output_data={"file_list": []},
                items_processed=0,
                error_message=str(e),
            )

        return NodeResult(
            success=True,
            output_data={"file_list": file_list},
            items_processed=len(file_list),
            metadata={
                "source_type": source_type,
                "file_count": len(file_list),
            },
        )

    @staticmethod
    def _file_metadata(path: Path) -> dict[str, Any]:
        """Build file metadata dict from a Path."""
        content_type, _ = mimetypes.guess_type(str(path))
        return {
            "path": str(path),
            "name": path.name,
            "size": path.stat().st_size,
            "content_type": content_type or "application/octet-stream",
            "extension": path.suffix.lower(),
        }


def register():
    from app.nodes.registry import register_node
    register_node(FileSourceNode())
