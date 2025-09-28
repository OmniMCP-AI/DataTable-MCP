import logging
from fastmcp import FastMCP
from fastapi import Request
from fastapi.responses import JSONResponse
from importlib import metadata

from fastmcp.server.auth import AuthProvider
from mcp.server.auth.provider import TokenVerifier, AccessToken

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to track current transport mode
_current_transport_mode = "stdio"

class UserIdBearerAuth(AuthProvider):
    async def verify_token(self, token: str) -> AccessToken | None:
        if not token:
            raise ValueError("Token cannot be empty")
        return AccessToken(
            token=token,
            client_id='xxx',
            scopes=[],
        )

user_id_auth = UserIdBearerAuth()



# Create FastMCP server instance
mcp = FastMCP(
    name="DataTableMCP",
    host="0.0.0.0",
    auth=user_id_auth
)

@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request):
    """Health check endpoint for container orchestration."""
    # Request parameter is required by FastAPI but not used in this endpoint
    _ = request
    try:
        version = metadata.version("datatable-mcp")
    except metadata.PackageNotFoundError:
        version = "dev"
    return JSONResponse({
        "status": "healthy",
        "service": "datatable-mcp",
        "version": version,
        "transport": _current_transport_mode
    })