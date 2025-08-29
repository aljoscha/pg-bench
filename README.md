# PostgreSQL Benchmarking Suite

A comprehensive PostgreSQL testing toolkit featuring three specialized tools for
database performance analysis, stress testing, and workload benchmarking.

## Tools Included

### 1. pg-connection-bench - Throughput Benchmarking Tool

A high-performance tool that measures how many connections per second your
PostgreSQL database can handle. Perfect for testing connection pool efficiency
and database connection handling performance.

**Key Features:**
- **Concurrent Testing**: Configure multiple workers to stress-test connection
  handling
- **Real-time Metrics**: Live updates showing connections per second, latency
  statistics
- **Latency Analysis**: Detailed latency percentiles and histogram distribution
- **Graceful Shutdown**: Clean exit on Ctrl+C with comprehensive summary
  statistics
- **Smart Error Handling**: Automatic backoff on connection failures
- **File Descriptor Management**: Automatic adjustment of system limits

### 2. pg-connection-load - Connection Load Testing Tool

Tests PostgreSQL connection pool limits and database behavior under high
connection loads by opening and maintaining idle database connections.

**Key Features:**
- Opens and maintains a specified number of idle connections
- **Batch Opening**: Opens connections in configurable batches to reduce load spikes
- **Pacing Control**: Optional pause between batches for gentler ramp-up
- Tests connection pool limits and maximum connection handling
- Monitors database behavior under sustained connection load
- Clean shutdown with connection cleanup

### 3. pg-workload-bench - Workload Benchmarking Tool

A flexible workload benchmarking tool that uses INI configuration files to define
complex database workloads with setup/teardown queries and multiple concurrent
workload patterns.

**Key Features:**
- **INI Configuration**: Define workloads, setup/teardown queries in simple INI format
- **Multi-Query Workloads**: Execute complex sequences of queries as atomic workloads
- **Concurrency Sweeps**: Test performance across different concurrency levels
- **Real-time Metrics**: Monitor queries per second and latency statistics
- **JSON Output**: Save results for later analysis and comparison
- **Plotting Support**: Generate comparison plots from multiple benchmark runs
- **Persistent Connections**: Reuses connections for realistic workload simulation

## Installation

### Prerequisites

- Python 3.9+
- PostgreSQL database to test against

### Setup

```bash
# Clone the repository
git clone https://github.com/aljoscha/pg-bench
cd pg-bench

# Install dependencies using uv
uv sync
```

## Usage

### Connection Throughput Benchmarking (pg-connection-bench)

Measure how many connections per second your database can handle:

```bash
# Using connection URL directly
uv run pg-connection-bench --url postgresql://user:pass@localhost/dbname --concurrency 50

# Using environment variable
export DATABASE_URL=postgresql://user:pass@localhost/dbname
uv run pg-connection-bench --concurrency 100

# Run for specific duration (30 seconds)
uv run pg-connection-bench --url postgresql://localhost/testdb --concurrency 50 --duration 30
```

**Command-line Options:**
- `--url, -u`: PostgreSQL connection URL (can also use DATABASE_URL env var)
- `--concurrency, -c`: Number of concurrent workers (default: 10)
- `--concurrency-min`: Minimum concurrency for sweep testing
- `--concurrency-max`: Maximum concurrency for sweep testing (doubles each run)
- `--duration, -d`: Test duration in seconds, 0 for indefinite (default: 0)
- `--name, -n`: Optional name for the benchmark run (included in output files)
- `--wait-between-runs`: Wait time in seconds between sweep runs (default: 20)

#### Example Output

```
PostgreSQL Connection Throughput Benchmark
============================================================
Target:       localhost/testdb
Concurrency:  50 workers
Duration:     indefinite
FD Limit:     4096
============================================================

[   1s] Rate:   1250.3 conn/s | Total:    1250 | Failed:     0 | Latency (ms): avg=  39.8 min=  28.3 max=  125.4
[   2s] Rate:   1285.7 conn/s | Total:    2536 | Failed:     0 | Latency (ms): avg=  38.6 min=  27.9 max=  89.2
[   3s] Rate:   1301.2 conn/s | Total:    3837 | Failed:     0 | Latency (ms): avg=  38.2 min=  26.8 max=  78.5
...

^C

Received interrupt signal, shutting down...

============================================================
Benchmark Summary
============================================================
Duration:             15.2 seconds
Concurrency:          50 workers
Total Connections:    19523
Failed Connections:   0
Success Rate:         100.0%
Average Rate:         1284.4 connections/second

Latency Statistics:
  Average:            38.91 ms
  Minimum:            26.83 ms
  Maximum:            125.42 ms

Latency Percentiles (ms):
----------------------------------------
  p50   :    37.82 ms
  p75   :    42.15 ms
  p90   :    48.93 ms
  p95   :    54.28 ms
  p99   :    72.41 ms
  p99.9 :   112.38 ms

Latency Distribution:
------------------------------------------------------------
    26.8 -    30.2 ms: ████████                                  1823 ( 9.3%)
    30.2 -    34.0 ms: ████████████████████                      4521 (23.2%)
    34.0 -    38.3 ms: ████████████████████████████████████████  9142 (46.8%)
    38.3 -    43.2 ms: ████████████████                          3621 (18.6%)
    43.2 -    48.6 ms: ██                                         287 ( 1.5%)
    48.6 -    54.8 ms: █                                           89 ( 0.5%)
    54.8 -    61.6 ms: █                                           23 ( 0.1%)
    61.6 -    69.4 ms: █                                           12 ( 0.1%)
    69.4 -    78.1 ms: █                                            3 ( 0.0%)
    78.1 -   125.4 ms: █                                            2 ( 0.0%)
```

### Connection Load Testing (pg-connection-load)

Test connection pool limits and database behavior under sustained connection load:

```bash
# Basic usage
uv run pg-connection-load --url "postgresql://user:pass@host/db" --connections 100 --duration 60

# Open connections in smaller batches with pauses (gentler on the server)
uv run pg-connection-load --url "postgresql://localhost/testdb" --connections 1000 \
  --batch-size 50 --batch-pause 0.5

# For example, for a local materialize instance
uv run pg-connection-load --url "postgres://materialize@localhost:6875/materialize" -n 10000

# With environment variable
export DATABASE_URL="postgresql://user:pass@host/db"
uv run pg-connection-load --connections 100 --duration 60
```

**Command-line Options:**
- `--url, -u`: PostgreSQL connection URL (or use DATABASE_URL env var)
- `--connections, -n`: Number of connections to open (default: 10)
- `--duration, -d`: How long to hold connections in seconds (default: 0 = indefinite)
- `--batch-size, -b`: Number of connections to open in each batch (default: 20)
- `--batch-pause, -p`: Pause in seconds between batches (default: 0.0, no pause)

#### Plotting Comparison Results

You can create comparison plots from saved benchmark JSON files:

```bash
# Compare multiple benchmark runs
uv run pg-connection-bench plot result1.json result2.json result3.json

# With custom output name
uv run pg-connection-bench plot --output-name "comparison" *.json
```

### Workload Benchmarking (pg-workload-bench)

Run complex database workloads defined in INI configuration files:

```bash
# Run a workload benchmark
uv run pg-workload-bench run --url postgresql://localhost/testdb --workload workload.ini

# With environment variable and custom name
export DATABASE_URL=postgresql://localhost/testdb
uv run pg-workload-bench run --workload workload.ini --name "production-test"

# Run with specific wait time between concurrency levels
uv run pg-workload-bench run --workload workload.ini --wait-between-runs 30
```

**Command-line Options:**
- `--url, -u`: PostgreSQL connection URL (or use DATABASE_URL env var)
- `--workload, -w`: Path to INI workload configuration file
- `--name, -n`: Optional name for the benchmark run
- `--wait-between-runs`: Wait time between concurrency runs (default: 20)

#### INI Configuration Format

```ini
# Global settings (at the top, no section header needed)
duration=5m                  # Test duration per concurrency level
concurrency-min=1            # Start at 1 concurrent worker
concurrency-max=256          # Test up to 256 workers (doubles each run)
# OR use fixed concurrency:
# concurrency=50             # Fixed number of workers

[setup]
query=drop table if exists test_table
query=create table test_table(id serial primary key, data text)
query=create index idx_data on test_table(data)

[teardown]
query=drop table test_table

# Define workloads - each section is a named workload
[simple select]
query=select 1

[insert and count]
query=insert into test_table(data) values ('test')
query=select count(*) from test_table

[complex workload]
query=begin
query=insert into test_table(data) values ('test')
query=select * from test_table where data = 'test' limit 10
query=commit
```

#### Plotting Workload Results

```bash
# Create comparison plots from multiple benchmark runs
uv run pg-workload-bench plot result1.json result2.json result3.json

# With custom output name
uv run pg-workload-bench plot --output-name "workload-comparison" *.json
```

## System Requirements

The throughput benchmarking tool automatically adjusts file descriptor limits when needed. For very high concurrency testing (>1000 workers), you may need to increase system limits:

```bash
# Check current limits
ulimit -n

# Increase limits for current session
ulimit -n 65536

# For permanent changes, edit /etc/security/limits.conf
```
