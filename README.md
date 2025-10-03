# data\_lakehouse\_ingest

Minimal ingestion stub for CDMHUB/BERDLHUB notebook integration.
Exposes a stable entrypoint for ingestion so notebooks can integrate with a package now and swap in real logic later.

---

## Environment and Python management

This package uses [uv](https://docs.astral.sh/uv/) for Python environment and package management.
See the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) to set up `uv` on your system.

The package requires Python **3.12+**.
`uv` will download and manage Python automatically if your system Python is older.

---

## Installation

To install dependencies (including Python if necessary), run:

```sh
uv sync --dev
```

This will create a project-local virtual environment at `.venv/`.

Activate the environment:

```sh
uv venv
source .venv/bin/activate
```

---

## Usage

After activation, you can import the ingestion stub:

```python
from data_lakehouse_ingest import ingest_from_config

# Either a dict…
cfg = {"input": {"uri": "s3a://demo/path"}, "target": {"table": "demo"}}
print(ingest_from_config(cfg))  # True

# …or a JSON file path
print(ingest_from_config("/path/to/config.json"))  # True
```

---

## Jupyter Notebook integration

If you are using Jupyter in the same container:

1. Install `ipykernel` into the uv-managed `.venv`:

   ```sh
   uv pip install ipykernel
   ```

2. Register the environment as a Jupyter kernel:

   ```sh
   uv run python -m ipykernel install --user --name=data-lakehouse-ingest --display-name "Python (data-lakehouse-ingest)"
   ```

3. In Jupyter Notebook, select the kernel **Python (data-lakehouse-ingest)**.

Now you can use:

```python
from data_lakehouse_ingest import ingest_from_config
```

directly inside notebooks.

---

## Tests

To run tests:

```sh
uv run pytest tests/
```

To generate coverage:

```sh
uv run pytest --cov=src --cov-report=xml tests/
```

The `coverage.xml` file can be used in CI pipelines.

---

## Linting

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting.
To run checks:

```sh
uv run ruff check src tests
```

To auto-fix:

```sh
uv run ruff check --fix src tests
```

---
