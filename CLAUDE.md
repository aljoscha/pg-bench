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
- **asyncpg**: Asynchronous PostgreSQL database adapter
- **asyncio**: Manages concurrent connections

Key components:
- `ConnectionPool` class: Manages connection lifecycle with async operations
- Signal handlers: Graceful shutdown on SIGINT/SIGTERM