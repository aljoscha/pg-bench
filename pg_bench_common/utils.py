"""General utility functions for PostgreSQL benchmarking tools."""

import re


def generate_power_of_2_range(min_val: int, max_val: int) -> list[int]:
    """Generate a list of powers of 2 between min_val and max_val.

    Also includes the exact min_val and max_val if they are not powers of 2.

    Args:
        min_val: Minimum value (inclusive)
        max_val: Maximum value (inclusive)

    Returns:
        Sorted list of test values including powers of 2 and endpoints
    """
    values = []
    current = 1

    # Find the first power of 2 >= min_val
    while current < min_val:
        current *= 2

    # Collect all powers of 2 up to max_val
    while current <= max_val:
        values.append(current)
        current *= 2

    # Include endpoints if they are not powers of 2
    if min_val not in values and min_val > 0:
        values.insert(0, min_val)

    if max_val not in values and max_val > min_val:
        values.append(max_val)

    return sorted(values)


def parse_duration(duration_str: str) -> int:
    """Parse duration string like '5m', '30s', '1h' into seconds.

    Args:
        duration_str: Duration string with optional unit suffix
                     Supported units: s (seconds), m (minutes), h (hours)
                     Defaults to seconds if no unit provided

    Returns:
        Duration in seconds

    Raises:
        ValueError: If duration format is invalid
    """
    duration_str = duration_str.strip().lower()

    # Check for time units
    match = re.match(r"^(\d+)([smh]?)$", duration_str)
    if not match:
        raise ValueError(f"Invalid duration format: {duration_str}")

    value = int(match.group(1))
    unit = match.group(2) or "s"  # Default to seconds

    multipliers = {"s": 1, "m": 60, "h": 3600}

    return value * multipliers[unit]


def format_duration(seconds: int) -> str:
    """Format seconds into human-readable duration string.

    Args:
        seconds: Duration in seconds

    Returns:
        Human-readable duration string
    """
    if seconds == 0:
        return "infinite"
    elif seconds < 60:
        return f"{seconds} seconds"
    elif seconds < 3600:
        return f"{seconds // 60} minutes"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if minutes == 0:
            return f"{hours} hours"
        else:
            return f"{hours} hours {minutes} minutes"


def sample_latencies(latencies: list[float], max_samples: int = 10000) -> list[float]:
    """Sample latencies for visualization to limit memory usage.

    Args:
        latencies: Full list of latency measurements
        max_samples: Maximum number of samples to return

    Returns:
        Sampled latencies (first max_samples entries, or copy of all if fewer)
    """
    if len(latencies) > max_samples:
        return latencies[:max_samples]
    else:
        return latencies.copy()
