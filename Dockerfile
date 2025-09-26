FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster dependency management
RUN pip install --no-cache-dir uv

# Copy dependency files
COPY pyproject.toml requirements.lock ./

# Install Python dependencies from requirements.lock (excluding local package)
RUN uv pip install --system --no-cache -r requirements.lock --no-deps || \
    uv pip install --system --no-cache \
    fastapi>=0.115.12 \
    fastmcp>=2.3.3 \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    openpyxl>=3.1.0 \
    xlrd>=2.0.1 \
    httpx>=0.28.1 \
    sqlalchemy>=2.0.0 \
    pyarrow>=12.0.0 \
    google-auth>=2.0.0 \
    google-auth-oauthlib>=1.0.0 \
    google-api-python-client>=2.0.0 \
    aiohttp>=3.12.15 \
    python-dotenv>=1.1.1 \
    gspread-asyncio>=2.0.0 \
    motor>=3.7.1 \
    uvicorn>=0.34.2

# Copy application code
COPY . .

# Set default environment variables (removed SPREADSHEET_API as it's no longer needed)

# Debug: Check PORT environment variable
RUN echo "=== Debug: Environment Variables ===" && \
    echo "PORT=${PORT:-8321}" && \
    echo "DATATABLE_MCP_PORT=${DATATABLE_MCP_PORT:-8321}"

# Debug: List files to verify structure
RUN echo "=== Debug: Listing app directory contents ===" && \
    ls -la /app && \
    echo "=== Debug: Checking if main.py exists ===" && \
    ls -la /app/main.py && \
    echo "=== Debug: Checking Python path and imports ===" && \
    python -c "import sys; print('Python path:', sys.path)" && \
    python -c "import core.server; print('Server import successful')" && \
    python -c "import datatable_tools.table_manager; print('Table manager import successful')"

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose port (use default of 8321 if PORT not set)
EXPOSE 8321
# Expose additional port if PORT environment variable is set to a different value

# Health check - for streamable-http mode
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD sh -c 'curl -f http://localhost:${PORT:-8321}/health || exit 1'

# Debug startup
RUN echo "=== Debug: Final startup test ===" && \
    python -c "print('Testing main.py import...'); import main; print('Main.py import successful')"

# Command to run the application
CMD ["python", "main.py", "--transport", "streamable-http", "--port", "8321"]