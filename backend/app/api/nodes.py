"""
Node registry API routes.
"""
from fastapi import APIRouter, Depends, HTTPException, status

from app.middleware.auth import get_current_user
from app.nodes.registry import get_node, get_registry_summary
from app.schemas.node_registry import NodeRegistryResponse, NodeTypeDefSchema, NodeValidateRequest, NodeValidateResponse

router = APIRouter(prefix="/api/nodes", tags=["nodes"])


@router.get("/types", response_model=NodeRegistryResponse)
async def get_node_types(
    current_user: dict = Depends(get_current_user),
):
    """Get all registered node types and their schemas."""
    nodes = get_registry_summary()
    return NodeRegistryResponse(
        nodes=[NodeTypeDefSchema(**n) for n in nodes],
        total=len(nodes),
    )


@router.post("/types/{node_type}/validate", response_model=NodeValidateResponse)
async def validate_node_config(
    node_type: str,
    request: NodeValidateRequest,
    current_user: dict = Depends(get_current_user),
):
    """Validate configuration for a specific node type."""
    from app.nodes.registry import get_node as _get_node

    node = _get_node(node_type)
    if not node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown node type: {node_type}",
        )

    errors = await node.validate_config(request.config)
    return NodeValidateResponse(
        valid=len(errors) == 0,
        errors=errors,
    )
