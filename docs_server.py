#!/usr/bin/env python3
"""
Standalone documentation server for DataTable MCP Server
Serves Swagger UI and ReDoc for the OpenAPI schema
"""

import json
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

app = FastAPI(
    title="DataTable MCP Server Documentation",
    description="API documentation for DataTable MCP Server tools",
    version="1.0.0"
)

@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with links to documentation"""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>DataTable MCP Server - Documentation</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                max-width: 800px;
                margin: 50px auto;
                padding: 20px;
                line-height: 1.6;
            }
            h1 { color: #2c3e50; }
            .card {
                border: 1px solid #e1e4e8;
                border-radius: 6px;
                padding: 20px;
                margin: 20px 0;
                background: #f6f8fa;
            }
            a {
                color: #0366d6;
                text-decoration: none;
                font-weight: 500;
            }
            a:hover { text-decoration: underline; }
            .emoji { font-size: 1.5em; margin-right: 10px; }
        </style>
    </head>
    <body>
        <h1>🗃️ DataTable MCP Server Documentation</h1>
        <p>Welcome to the DataTable MCP Server API documentation. Choose your preferred documentation viewer:</p>

        <div class="card">
            <h2><span class="emoji">📖</span>Swagger UI</h2>
            <p>Interactive API documentation with built-in testing capabilities.</p>
            <a href="/docs">Open Swagger UI →</a>
        </div>

        <div class="card">
            <h2><span class="emoji">📚</span>ReDoc</h2>
            <p>Clean, responsive API documentation with advanced search.</p>
            <a href="/redoc">Open ReDoc →</a>
        </div>

        <div class="card">
            <h2><span class="emoji">📄</span>OpenAPI Schema</h2>
            <p>Raw OpenAPI 3.0 JSON specification for tools and integrations.</p>
            <a href="/openapi.json">Download openapi.json →</a>
        </div>

        <hr style="margin: 40px 0;">

        <h3>🛠️ Available Tools: 22</h3>
        <ul>
            <li><strong>Table Lifecycle</strong> (4 tools): create_table, load_table, clone_table, list_tables</li>
            <li><strong>Data Manipulation</strong> (6 tools): append_row, add_column, set_range_values, delete_from_table, rename_columns, clear_range</li>
            <li><strong>Data Query</strong> (3 tools): get_table_data, filter_rows, sort_table</li>
            <li><strong>Export & Import</strong> (2 tools): save_table, export_table</li>
            <li><strong>Advanced Operations</strong> (3 tools): merge_tables, aggregate_data, map_values</li>
            <li><strong>Session Management</strong> (3 tools): cleanup_tables, get_table_info, get_session_stats</li>
        </ul>

        <p><strong>MCP Server:</strong> Running on port 8001 (streamable-http mode)</p>
    </body>
    </html>
    """
    return HTMLResponse(content=html)

@app.get("/openapi.json")
async def get_openapi_json():
    """Serve the OpenAPI JSON schema"""
    try:
        openapi_path = Path(__file__).parent / "openapi.json"
        if openapi_path.exists():
            with open(openapi_path) as f:
                return JSONResponse(content=json.load(f))
        return JSONResponse(
            content={"error": "OpenAPI schema not found. Run: python generate_mcp_openapi.py"},
            status_code=404
        )
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/docs", response_class=HTMLResponse)
async def swagger_ui():
    """Serve Swagger UI for API documentation"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>DataTable MCP Server - Swagger UI</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"/>
        <style>
            body { margin: 0; padding: 0; }
        </style>
    </head>
    <body>
        <div id="swagger-ui"></div>
        <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
        <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
        <script>
            window.onload = function() {
                window.ui = SwaggerUIBundle({
                    url: "/openapi.json",
                    dom_id: '#swagger-ui',
                    deepLinking: true,
                    presets: [
                        SwaggerUIBundle.presets.apis,
                        SwaggerUIStandalonePreset
                    ],
                    plugins: [
                        SwaggerUIBundle.plugins.DownloadUrl
                    ],
                    layout: "StandaloneLayout"
                });
            };
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/redoc", response_class=HTMLResponse)
async def redoc_ui():
    """Serve ReDoc UI for API documentation"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>DataTable MCP Server - ReDoc</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
        <style>
            body { margin: 0; padding: 0; }
        </style>
    </head>
    <body>
        <redoc spec-url="/openapi.json"></redoc>
        <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    print("📚 Starting DataTable MCP Documentation Server...")
    print("=" * 50)
    print("📖 Swagger UI:    http://localhost:8002/docs")
    print("📚 ReDoc:         http://localhost:8002/redoc")
    print("📄 OpenAPI JSON:  http://localhost:8002/openapi.json")
    print("🏠 Home:          http://localhost:8002/")
    print("=" * 50)
    print("\n💡 Tip: Keep the MCP server running on port 8001")
    print("    MCP Server: python main.py --transport streamable-http --port 8001")
    print("\nPress CTRL+C to stop the documentation server\n")

    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="info")