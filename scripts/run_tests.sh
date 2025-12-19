#!/bin/bash
set -x
set -e

# Resolve to the repository root (two levels up from this script)
SCRIPT_DIR="$(dirname "$(dirname "$(readlink -f "$0")")")"

# Move to repo root
cd "$SCRIPT_DIR"

echo "Running Spark tests inside container from: $SCRIPT_DIR"

# Run pytest using uv environment
# --cov=src       → measure coverage for project code
# --cov-report=xml → write coverage.xml for Codecov
# --cov-append    → append if a previous coverage.xml exists
uv run pytest --cov=src --cov-report=xml --cov-append tests/
