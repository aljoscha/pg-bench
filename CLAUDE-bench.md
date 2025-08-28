# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PostgreSQL connection throughput benchmarking tool that measures how many connections per second a database can handle by continuously opening and closing connections with configurable concurrency.

## Development Commands

### Install Dependencies
```bash
uv sync
```

### Run the Tool
```bash
uv run pg-connection-bench --url <postgresql_url> --concurrency <workers> --duration <seconds>
# or with environment variable
DATABASE_URL=postgresql://... uv run pg-connection-bench --concurrency 50
```

### Linting
```bash
uv run ruff check pg_connection_bench.py
uv run ruff format pg_connection_bench.py
```

## Architecture

Single-file Python CLI application using:
- **Click**: Command-line interface framework for argument parsing
- **asyncpg**: Asynchronous PostgreSQL database adapter for high-performance connections
- **asyncio**: Manages concurrent workers and async operations
- **dataclasses**: Clean data structure for benchmark statistics

Key components:
- `ConnectionBenchmark` class: Orchestrates concurrent workers and tracks metrics
- `BenchmarkStats` dataclass: Thread-safe storage for connection metrics and latencies
- Worker coroutines: Continuously open/close connections and measure latency
- Reporter coroutine: Displays real-time statistics every second
- Histogram generation: Logarithmic buckets for latency distribution visualization
- Signal handlers: Graceful shutdown on SIGINT/SIGTERM with summary statistics