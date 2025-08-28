# pg-connection-load

PostgreSQL connection load testing tool for testing connection pool limits and
database behavior under high connection loads.

## Installation

```bash
uv sync
```

## Usage

```bash
# With URL parameter
uv run pg-connection-load --url "postgresql://user:pass@host/db" --connections 100 --duration 60

# For example, for a local materialize instance
uv run pg_connection_load.py --url "postgres://materialize@localhost:6875/materialize" -n 10000

# With environment variable
export DATABASE_URL="postgresql://user:pass@host/db"
uv run pg-connection-load --connections 100 --duration 60
```

## Parameters

- `--url`: PostgreSQL connection URL (or use DATABASE_URL env var)
- `--connections`: Number of connections to open (default: 10)
- `--duration`: How long to hold connections in seconds (default: 0 = indefinite)
