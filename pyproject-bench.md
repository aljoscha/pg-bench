[project]
name = "pg-connection-bench"
version = "0.1.0"
description = "PostgreSQL connection throughput benchmarking tool"
requires-python = ">=3.9"
dependencies = [
    "asyncpg>=0.29.0",
    "click>=8.1.7",
    "matplotlib>=3.7.0",
]

[project.optional-dependencies]
dev = [
    "ruff>=0.7.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project.scripts]
pg-connection-bench = "pg_connection_bench:main"

[tool.ruff]
line-length = 88
target-version = "py39"

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "UP",  # pyupgrade
]
ignore = []

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
skip-magic-trailing-comma = false
line-ending = "auto"

[dependency-groups]
dev = [
    "ruff>=0.12.10",
]
