"""Database utilities for PostgreSQL benchmarking tools."""


import asyncpg
import click


def sanitize_url_for_display(url: str) -> str:
    """Remove credentials from URL for display purposes.

    Args:
        url: Database URL potentially containing credentials

    Returns:
        URL with credentials hidden
    """
    return url.split("@")[-1] if "@" in url else url


def format_target_list(urls: list[str]) -> list[str]:
    """Format list of URLs for display by removing credentials.

    Args:
        urls: List of database URLs

    Returns:
        List of sanitized URLs for display
    """
    return [sanitize_url_for_display(url) for url in urls]


async def setup_database_connection(url: str, timeout: int = 30) -> asyncpg.Connection:
    """Create a database connection with standard timeout and test query.

    Args:
        url: Database connection URL
        timeout: Connection timeout in seconds

    Returns:
        Established database connection

    Raises:
        Exception: If connection fails
    """
    conn = await asyncpg.connect(url, timeout=timeout)
    await conn.execute("SELECT 1")
    return conn


async def run_queries_on_connection(
    urls: list[str], queries: list[str], description: str = "queries"
) -> None:
    """Run a list of queries on the first database URL.

    Args:
        urls: List of database URLs (only first one is used)
        queries: List of SQL queries to execute
        description: Description for logging purposes
    """
    if not queries:
        return

    click.echo(f"\nRunning {description}...")
    # Only run on the first server since they share the same data store
    postgres_url = urls[0]
    conn = await setup_database_connection(postgres_url)
    try:
        for query in queries:
            click.echo(f"  Executing: {query[:50]}...")
            await conn.execute(query)
        click.echo(f"{description.capitalize()} completed successfully.\n")
    finally:
        await conn.close()
