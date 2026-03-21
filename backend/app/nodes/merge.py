"""
Merge node: fan-in -- combines multiple inputs into one.

Modes:
- "concat": merge all input lists into one list
- "zip": pair items from multiple inputs together
- "merge_objects": deep-merge dicts from multiple inputs
"""
import copy
import logging
from collections import deque
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)


class MergeNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "merge"

    @property
    def display_name(self) -> str:
        return "Merge"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Combine multiple inputs into one (fan-in)"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="inputs", data_type=PortDataType.ANY, multi=True, label="Inputs")]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="merged", data_type=PortDataType.ANY, label="Merged")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": ["concat", "zip", "merge_objects"],
                    "default": "concat",
                },
            },
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Merge multiple inputs into one output."""
        mode = context.config.get("mode", "concat")

        # Collect all input values.
        # With multi=True port, the executor may provide data under multiple keys.
        # We gather everything from input_data that is a list.
        input_values: list[Any] = []

        # Check for 'inputs' key (multi-port aggregated)
        if "inputs" in context.input_data:
            val = context.input_data["inputs"]
            if isinstance(val, list):
                # Could be a list of lists (multiple sources) or a flat list
                input_values.append(val)
            else:
                input_values.append([val])

        # Also collect from any other keys (individual port connections)
        for key, val in context.input_data.items():
            if key == "inputs":
                continue
            if isinstance(val, list):
                input_values.append(val)
            else:
                input_values.append([val])

        if not input_values:
            return NodeResult(
                success=True,
                output_data={"merged": []},
                items_processed=0,
                metadata={"mode": mode},
            )

        try:
            if mode == "concat":
                result = self._merge_concat(input_values)
            elif mode == "zip":
                result = self._merge_zip(input_values)
            elif mode == "merge_objects":
                result = self._merge_objects(input_values)
            else:
                return NodeResult(
                    success=False,
                    output_data={"merged": []},
                    items_processed=0,
                    error_message=f"Unknown merge mode: {mode}",
                )
        except Exception as e:
            logger.exception(f"MergeNode error: {e}")
            return NodeResult(
                success=False,
                output_data={"merged": []},
                items_processed=0,
                error_message=str(e),
            )

        # Count items
        item_count = len(result) if isinstance(result, list) else 1

        return NodeResult(
            success=True,
            output_data={"merged": result},
            items_processed=item_count,
            metadata={
                "mode": mode,
                "input_count": len(input_values),
                "output_count": item_count,
            },
        )

    @staticmethod
    def _merge_concat(input_values: list[list]) -> list:
        """Concatenate all lists into one flat list."""
        result: list[Any] = []
        for lst in input_values:
            if isinstance(lst, list):
                result.extend(lst)
            else:
                result.append(lst)
        return result

    @staticmethod
    def _merge_zip(input_values: list[list]) -> list:
        """Zip items from multiple inputs together into tuples."""
        # Ensure all inputs are lists
        lists = [lst if isinstance(lst, list) else [lst] for lst in input_values]
        # Zip to the shortest length
        return [list(group) for group in zip(*lists)]

    @staticmethod
    def _merge_objects(input_values: list) -> dict:
        """Deep-merge dicts from multiple inputs."""
        result: dict[str, Any] = {}

        for val in input_values:
            if isinstance(val, list):
                # List of dicts: merge each one
                for item in val:
                    if isinstance(item, dict):
                        MergeNode._deep_merge(result, item)
                    else:
                        # Non-dict item: skip or store in a list
                        key = "_items"
                        if key not in result:
                            result[key] = []
                        result[key].append(item)
            elif isinstance(val, dict):
                MergeNode._deep_merge(result, val)

        return result

    @staticmethod
    def _deep_merge(target: dict, source: dict) -> dict:
        """Deep-merge source dict into target dict."""
        for key, value in source.items():
            if (
                key in target
                and isinstance(target[key], dict)
                and isinstance(value, dict)
            ):
                MergeNode._deep_merge(target[key], value)
            else:
                target[key] = copy.deepcopy(value)
        return target


def register():
    from app.nodes.registry import register_node
    register_node(MergeNode())
