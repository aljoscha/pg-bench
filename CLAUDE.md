# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PostgreSQL benchmarking suite with two specialized tools:

1. **pg-connection-bench**: Throughput benchmarking tool that measures how many connections per second a database can handle by continuously opening and closing connections with configurable concurrency.

2. **pg-connection-load**: Connection load testing tool that opens and maintains idle database connections for testing connection pool limits and database behavior under high connection loads.

## Development Commands

### Install Dependencies
```bash
uv sync
```

### Run the Throughput Benchmark Tool
```bash
uv run pg-connection-bench --url <postgresql_url> --concurrency <workers> --duration <seconds>
# or with environment variable
DATABASE_URL=postgresql://... uv run pg-connection-bench --concurrency 50
```

### Run the Load Testing Tool
```bash
uv run pg-connection-load --url <postgresql_url> --connections <num> --duration <seconds>
# or with environment variable
DATABASE_URL=postgresql://... uv run pg-connection-load --connections 100
```

### Linting
```bash
uv run ruff check pg_connection_bench.py pg_connection_load.py
uv run ruff format pg_connection_bench.py pg_connection_load.py
```

## Architecture

### pg-connection-bench (Throughput Benchmarking)

Single-file Python CLI application using:
- **Click**: Command-line interface framework for argument parsing
- **asyncpg**: Asynchronous PostgreSQL database adapter for high-performance connections
- **asyncio**: Manages concurrent workers and async operations
- **dataclasses**: Clean data structure for benchmark statistics
- **matplotlib**: Generates histogram visualizations

Key components:
- `ConnectionBenchmark` class: Orchestrates concurrent workers and tracks metrics
- `BenchmarkStats` dataclass: Thread-safe storage for connection metrics and latencies
- Worker coroutines: Continuously open/close connections and measure latency
- Reporter coroutine: Displays real-time statistics every second
- Histogram generation: Logarithmic buckets for latency distribution visualization
- Signal handlers: Graceful shutdown on SIGINT/SIGTERM with summary statistics

### pg-connection-load (Load Testing)

Single-file Python CLI application using:
- **Click**: Command-line interface framework for argument parsing
- **asyncpg**: Asynchronous PostgreSQL database adapter
- **asyncio**: Manages concurrent connections

Key components:
- `ConnectionPool` class: Manages connection lifecycle with async operations
- Signal handlers: Graceful shutdown on SIGINT/SIGTERM

## Code Style

Both tools follow the same coding conventions:
- Async/await pattern for all database operations
- Click for CLI argument parsing
- Ruff for linting and formatting
- Type hints where beneficial
- Comprehensive error handling with graceful degradation