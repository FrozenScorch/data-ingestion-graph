"""
Filter node: filter items based on a condition expression.

Uses restricted eval for safe execution of Python expressions.
"""
import ast
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)

# Restricted builtins available in filter expressions
_RESTRICTED_BUILTINS: dict[str, Any] = {
    "len": len,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "abs": abs,
    "min": min,
    "max": max,
    "sum": sum,
    "any": any,
    "all": all,
    "isinstance": isinstance,
    # Note: getattr/hasattr intentionally excluded — they enable sandbox escapes
    # via getattr(item, '__class__') which bypasses AST attribute checks.
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "round": round,
    "enumerate": enumerate,
    "range": range,
    "zip": zip,
    "True": True,
    "False": False,
    "None": None,
}


class FilterNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "filter"

    @property
    def display_name(self) -> str:
        return "Filter"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Filter items based on a condition expression (safe eval)"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="items", data_type=PortDataType.ITEMS, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [
            PortDef(name="matched", data_type=PortDataType.ITEMS, label="Matched"),
            PortDef(name="rejected", data_type=PortDataType.ITEMS, label="Rejected"),
        ]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "condition": {
                    "type": "string",
                    "description": "Filter expression. Use 'item' to reference each item (e.g., 'item.size > 100')",
                },
            },
            "required": ["condition"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Filter items based on the condition expression."""
        items = context.input_data.get("items", [])
        condition = context.config.get("condition", "")

        if not condition:
            return NodeResult(
                success=False,
                output_data={"matched": [], "rejected": []},
                items_processed=0,
                error_message="No condition expression provided",
            )

        if not items:
            return NodeResult(
                success=True,
                output_data={"matched": [], "rejected": []},
                items_processed=0,
            )

        # Validate the expression for safety
        try:
            self._validate_expression(condition)
        except ValueError as e:
            return NodeResult(
                success=False,
                output_data={"matched": [], "rejected": []},
                items_processed=0,
                error_message=f"Invalid condition: {e}",
            )

        matched: list[Any] = []
        rejected: list[Any] = []

        for item in items:
            try:
                result = eval(  # noqa: S307  -- restricted builtins, validated AST
                    condition,
                    {"__builtins__": _RESTRICTED_BUILTINS},
                    {"item": item},
                )
                if result:
                    matched.append(item)
                else:
                    rejected.append(item)
            except Exception as e:
                logger.warning(f"Filter condition error for item: {e}")
                rejected.append(item)

        return NodeResult(
            success=True,
            output_data={
                "matched": matched,
                "rejected": rejected,
            },
            items_processed=len(items),
            metadata={
                "total": len(items),
                "matched_count": len(matched),
                "rejected_count": len(rejected),
                "condition": condition,
            },
        )

    @staticmethod
    def _validate_expression(expression: str) -> None:
        """
        Validate that the expression is safe to eval.

        Allows only simple expressions -- no imports, no function definitions,
        no class definitions, no comprehensions with side effects.
        """
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as e:
            raise ValueError(f"Invalid syntax: {e}")

        for node in ast.walk(tree):
            # Disallow dangerous constructs
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                raise ValueError("Imports are not allowed in filter expressions")
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                raise ValueError("Function/class definitions are not allowed")
            if isinstance(node, ast.Global):
                raise ValueError("Global variables are not allowed")
            if isinstance(node, ast.Nonlocal):
                raise ValueError("Nonlocal variables are not allowed")
            if isinstance(node, ast.Attribute):
                # Allow simple attribute access but block dangerous ones
                attr_name = node.attr
                if attr_name.startswith("_"):
                    raise ValueError(f"Access to private attribute '{attr_name}' is not allowed")


def register():
    from app.nodes.registry import register_node
    register_node(FilterNode())
