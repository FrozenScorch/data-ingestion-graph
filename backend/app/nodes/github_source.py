"""
GitHubSource node: GitHub API reader node.
"""
from typing import Any

from app.nodes.base import BaseNode, NodeContext, NodeResult, PortDef, PortDataType


class GitHubSourceNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "github_source"

    @property
    def display_name(self) -> str:
        return "GitHub Source"

    @property
    def category(self) -> str:
        return "source"

    @property
    def description(self) -> str:
        return "Read data from GitHub (issues, PRs, files)"

    @property
    def inputs(self) -> list[PortDef]:
        return []

    @property
    def outputs(self) -> list[PortDef]:
        return [PortDef(name="json", data_type=PortDataType.JSON, label="GitHub Data")]

    @property
    def config_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "connection_id": {"type": "string"},
                "repo": {"type": "string", "description": "owner/repo format"},
                "resource_type": {"type": "string", "enum": ["issues", "pull_requests", "files"], "default": "issues"},
            },
            "required": ["connection_id", "repo"],
        }

    async def execute(self, context: NodeContext) -> NodeResult:
        return NodeResult(success=True, output_data={"json": []}, items_processed=0)


def register():
    from app.nodes.registry import register_node
    register_node(GitHubSourceNode())
