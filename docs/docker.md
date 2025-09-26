# Docker Configuration for DataTable MCP

## 🐳 Docker Setup

### Quick Start

1. **Build and run with Docker Compose:**
   ```bash
   docker-compose up --build
   ```

2. **Build and run with Docker directly:**
   ```bash
   docker build -t datatable-mcp .
   docker run -p 8321:8321 -e SPREADSHEET_API=http://host.docker.internal:9394 datatable-mcp
   ```

### Environment Configuration

#### Required Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SPREADSHEET_API` | `http://localhost:9394` | URL of the Spreadsheet API service |
| `TEST_USER_ID` | `68501372a3569b6897673a48` | User ID for spreadsheet authentication |

#### Optional Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8321` | Port for the DataTable MCP service |
| `DATATABLE_MCP_PORT` | `8321` | Alternative port configuration |
| `EXAMPLE_SPREADSHEET_ID` | `1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms` | Test spreadsheet ID |

### Configuration Files

- **`.env`** - Local environment configuration (used by Docker)
- **`.env.docker`** - Example Docker environment configuration
- **`docker-compose.yml`** - Docker Compose service definition

### Production Deployment

1. **Copy and configure environment:**
   ```bash
   cp .env.docker .env
   # Edit .env with your production values
   ```

2. **Update SPREADSHEET_API URL:**
   ```bash
   # For production deployment
   SPREADSHEET_API=https://your-spreadsheet-api.com
   ```

3. **Deploy with Docker Compose:**
   ```bash
   docker-compose up -d
   ```

### Docker Networking

- **Default**: Service runs on port 8321
- **Host Access**: Use `host.docker.internal:9394` to access services running on the Docker host
- **Production**: Replace with actual service URLs

### Health Check

The container includes a health check that verifies the service is responding:
```bash
curl -f http://localhost:8321/health
```

### Logs and Monitoring

```bash
# View logs
docker-compose logs -f datatable-mcp

# Check container status
docker-compose ps
```

### Integration Testing in Docker

```bash
# Run integration tests inside container
docker-compose exec datatable-mcp python tests/test_spreadsheet_integration.py
```

### Troubleshooting

1. **Cannot connect to Spreadsheet API:**
   - Verify `SPREADSHEET_API` URL is correct
   - Ensure Spreadsheet API service is running and accessible
   - Check Docker network connectivity

2. **Environment variables not loaded:**
   - Verify `.env` file exists and has correct values
   - Check Docker Compose environment section
   - Restart containers after environment changes

3. **Port conflicts:**
   - Change `PORT` environment variable
   - Update port mapping in docker-compose.yml
   - Ensure no other services using the same port