# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PostgreSQL connection load testing tool that opens and maintains idle database connections for testing connection pool limits and database behavior under high connection loads.

## Development Commands

### Install Dependencies
```bash
uv sync
```

### Run the Tool
```bash
uv run pg-connection-load --url <postgresql_url> --connections <num> --duration <seconds>
# or with environment variable
DATABASE_URL=postgresql://... uv run pg-connection-load --connections 100
```

### Linting
```bash
uv run ruff check pg_connection_load.py
uv run ruff format pg_connection_load.py
```

## Architecture

Single-file Python CLI application using:
- **Click**: Command-line interface framework for argument parsing
- **psycopg2**: PostgreSQL database adapter
- **Threading**: Maintains keepalive thread for connection health checks

Key components:
- `ConnectionPool` class: Manages connection lifecycle with thread-safe operations
- Signal handlers: Graceful shutdown on SIGINT/SIGTERM
- Keepalive mechanism: Sends periodic queries every 30 seconds to prevent connection timeouts