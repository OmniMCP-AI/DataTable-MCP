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


# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose port 8321
EXPOSE 8321


# Command to run the application
CMD ["python", "main.py", "--transport", "streamable-http", "--port", "8321"]