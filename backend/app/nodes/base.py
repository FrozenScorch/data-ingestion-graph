"""
Base node ABC, NodeContext, NodeResult, PortDef, NodeTypeDef.
All node implementations inherit from BaseNode.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PortDataType(str, Enum):
    """Data types that can flow between ports."""

    ANY = "any"
    FILE_LIST = "file_list"
    TABLE = "table"
    JSON = "json"
    DOCUMENT = "document"
    CHUNKS = "chunks"
    EMBEDDINGS = "embeddings"
    TEXT = "text"
    ITEMS = "items"


class PortDirection(str, Enum):
    INPUT = "input"
    OUTPUT = "output"


@dataclass
class PortDef:
    """Definition of a node input or output port."""

    name: str
    data_type: PortDataType
    label: str = ""
    required: bool = False
    multi: bool = False  # Can accept multiple connections (fan-in)

    def __post_init__(self):
        if not self.label:
            self.label = self.name.replace("_", " ").title()


@dataclass
class NodeResult:
    """Result returned by a node after execution."""

    success: bool
    output_data: dict[str, Any] = field(default_factory=dict)
    items_processed: int = 0
    error_message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    progress: float = 0.0  # 0.0 - 1.0 for progress reporting


@dataclass
class NodeContext:
    """Execution context passed to each node."""

    run_id: str
    node_id: str
    config: dict[str, Any] = field(default_factory=dict)
    input_data: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)  # Shared state across nodes in a run
    working_dir: str = "./data/temp"
    redis_client: Any = None  # redis.Redis, not typed to avoid import
    db_session: Any = None  # Execution-scoped AsyncSession for atomic control-plane adapters


class BaseNode(ABC):
    """
    Abstract base class for all node implementations.

    Subclasses must define:
    - node_type: unique string identifier
    - display_name: human-readable name
    - category: source/processing/ai/output
    - description: what the node does
    - inputs: list of PortDef
    - outputs: list of PortDef
    - config_schema: JSON schema for node configuration
    - execute(): main execution logic
    """

    @property
    def implementation(self) -> str:
        """Implementation boundary shown by Studio: native or SDK adapter."""
        return "studio"

    @property
    def sdk_component(self) -> str | None:
        """Public SDK component used by this node, when applicable."""
        return None

    @property
    @abstractmethod
    def node_type(self) -> str:
        """Unique node type identifier (e.g., 'file_source', 'embedder')."""
        ...

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable node name for the UI."""
        ...

    @property
    @abstractmethod
    def category(self) -> str:
        """Node category: 'source', 'processing', 'ai', or 'output'."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Description of what the node does."""
        ...

    @property
    @abstractmethod
    def inputs(self) -> list[PortDef]:
        """Input port definitions."""
        ...

    @property
    @abstractmethod
    def outputs(self) -> list[PortDef]:
        """Output port definitions."""
        ...

    @property
    @abstractmethod
    def config_schema(self) -> dict[str, Any]:
        """JSON Schema for node configuration."""
        ...

    @abstractmethod
    async def execute(self, context: NodeContext) -> NodeResult:
        """
        Execute the node's logic.

        Args:
            context: Execution context with input data and configuration.

        Returns:
            NodeResult with output data and metadata.
        """
        ...

    async def validate_config(self, config: dict[str, Any]) -> list[str]:
        """
        Validate node configuration. Returns list of error messages (empty if valid).

        Default implementation checks required fields from config_schema.
        """
        errors = []
        if "required" in self.config_schema:
            for req_field in self.config_schema["required"]:
                properties = self.config_schema.get("properties", {})
                if req_field in properties and req_field not in config:
                    errors.append(f"Missing required field: {req_field}")
        return errors

    def to_dict(self) -> dict[str, Any]:
        """Serialize node definition to dict (for the node registry API)."""
        return {
            "type": self.node_type,
            "display_name": self.display_name,
            "category": self.category,
            "description": self.description,
            "implementation": self.implementation,
            "sdk_component": self.sdk_component,
            "inputs": [
                {
                    "name": p.name,
                    "data_type": p.data_type.value,
                    "label": p.label,
                    "required": p.required,
                    "multi": p.multi,
                }
                for p in self.inputs
            ],
            "outputs": [
                {
                    "name": p.name,
                    "data_type": p.data_type.value,
                    "label": p.label,
                    "required": p.required,
                    "multi": p.multi,
                }
                for p in self.outputs
            ],
            "config_schema": self.config_schema,
        }
