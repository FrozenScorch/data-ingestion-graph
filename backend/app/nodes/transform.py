"""
Transform node: transform data using Python expressions or Jinja2 templates.

Modes:
- "python": evaluates a Python expression with restricted builtins
- "jinja2": renders a Jinja2 template with input data as context
"""
import ast
import logging
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType

logger = logging.getLogger(__name__)

# Restricted builtins for Python expression mode
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
    "hasattr": hasattr,
    "getattr": getattr,
    "setattr": setattr,
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "sorted": sorted,
    "reversed": reversed,
    "enumerate": enumerate,
    "range": range,
    "zip": zip,
    "map": map,
    "filter": filter,
    "round": round,
    "type": type,
    "json": __import__("json"),
    "re": __import__("re"),
    "True": True,
    "False": False,
    "None": None,
}


class TransformNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "transform"

    @property
    def display_name(self) -> str:
        return "Transform"

    @property
    def category(self) -> str:
        return "processing"

    @property
    def description(self) -> str:
        return "Transform data using Python expression or Jinja2 template"

    @property
    def inputs(self) -> list[PortDef]:
        return [PortDef(name="data", data_type=PortDataType.ANY, required=True)]

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="result", data_type=PortDataType.ANY, label="Result")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": ["python", "jinja2"],
                    "default": "python",
                },
                "expression": {
                    "type": "string",
                    "description": "Python expression or Jinja2 template string",
                },
            },
            "required": ["mode", "expression"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        """Transform input data using the configured expression."""
        data = context.input_data.get("data")
        mode = context.config.get("mode", "python")
        expression = context.config.get("expression", "")

        if not expression:
            return NodeResult(
                success=False,
                output_data={"result": None},
                items_processed=0,
                error_message="No expression provided",
            )

        try:
            if mode == "python":
                result = self._eval_python(expression, data)
            elif mode == "jinja2":
                result = self._render_jinja2(expression, data)
            else:
                return NodeResult(
                    success=False,
                    output_data={"result": None},
                    items_processed=0,
                    error_message=f"Unknown transform mode: {mode}",
                )
        except Exception as e:
            logger.exception(f"TransformNode error: {e}")
            return NodeResult(
                success=False,
                output_data={"result": None},
                items_processed=0,
                error_message=str(e),
            )

        return NodeResult(
            success=True,
            output_data={"result": result},
            items_processed=1 if result is not None else 0,
            metadata={"mode": mode},
        )

    @staticmethod
    def _eval_python(expression: str, data: Any) -> Any:
        """Evaluate a Python expression with restricted builtins."""
        # Validate expression safety
        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as e:
            raise ValueError(f"Invalid Python expression: {e}")

        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                raise ValueError("Imports are not allowed in transform expressions")
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                raise ValueError("Definitions are not allowed")
            if isinstance(node, (ast.Global, ast.Nonlocal)):
                raise ValueError("Global/nonlocal variables are not allowed")

        return eval(  # noqa: S307
            expression,
            {"__builtins__": _RESTRICTED_BUILTINS},
            {"data": data, "input": data},
        )

    @staticmethod
    def _render_jinja2(template_str: str, data: Any) -> str:
        """Render a Jinja2 template with data as context."""
        try:
            from jinja2 import Environment, BaseLoader, StrictUndefined
        except ImportError:
            raise ImportError(
                "jinja2 is required for Jinja2 template mode. "
                "Install with: pip install jinja2"
            )

        env = Environment(
            loader=BaseLoader(),
            undefined=StrictUndefined,
        )

        template = env.from_string(template_str)

        # Build context -- if data is a dict, use it directly; otherwise wrap it
        if isinstance(data, dict):
            context = data
        else:
            context = {"data": data, "item": data}

        return template.render(context)


def register():
    from app.nodes.registry import register_node
    register_node(TransformNode())
