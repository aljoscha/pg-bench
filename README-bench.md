# pg-connection-bench

A high-performance PostgreSQL connection throughput benchmarking tool that
measures how many connections per second your database can handle.

## Features

- **Concurrent Testing**: Configure multiple workers to stress-test connection handling
- **Real-time Metrics**: Live updates showing connections per second, latency statistics
- **Latency Analysis**: Detailed latency percentiles and histogram distribution
- **Graceful Shutdown**: Clean exit on Ctrl+C with comprehensive summary statistics
- **Smart Error Handling**: Automatic backoff on connection failures
- **File Descriptor Management**: Automatic adjustment of system limits

## Installation

### Prerequisites

- Python 3.8+
- PostgreSQL database to test against

### Setup

```bash
# Clone the repository
git clone https://github.com/aljoscha/pg-connection-bench
cd pg-connection-bench

# Install dependencies using uv
uv sync
```

## Usage

### Basic Usage

```bash
# Using connection URL directly
uv run pg-connection-bench --url postgresql://user:pass@localhost/dbname --concurrency 50

# Using environment variable
export DATABASE_URL=postgresql://user:pass@localhost/dbname
uv run pg-connection-bench --concurrency 100

# Run for specific duration (30 seconds)
uv run pg-connection-bench --url postgresql://localhost/testdb --concurrency 50 --duration 30
```

### Command-line Options

- `--url, -u`: PostgreSQL connection URL (can also use DATABASE_URL env var)
- `--concurrency, -c`: Number of concurrent workers (default: 10)
- `--duration, -d`: Test duration in seconds, 0 for indefinite (default: 0)

### Example Output

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

## System Requirements

The tool automatically adjusts file descriptor limits when needed. For very high concurrency testing (>1000 workers), you may need to increase system limits:

```bash
# Check current limits
ulimit -n

# Increase limits for current session
ulimit -n 65536

# For permanent changes, edit /etc/security/limits.conf
```
