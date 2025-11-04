# data\_lakehouse\_ingest

End-to-end ingestion framework for loading data into the BER Data Lakehouse using pyspark and minio.

It reads flat files (CSV, TSV, JSON, XML) from MinIO (S3-compatible), applies schema-based casting and validation (via LinkML or SQL-style schema), and writes curated Delta tables to a Spark-based Lakehouse.

---

## Environment and Python management

This package uses [uv](https://docs.astral.sh/uv/) for Python environment and package management.
See the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) to set up `uv` on your system.

The package requires Python **3.10+**.
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

The following example demonstrates a full ingestion workflow using Spark, MinIO, and LinkML schema integration.

```python
from data_lakehouse_ingest import data_lakehouse_ingest_config
from data_lakehouse_ingest.logger import setup_logger
from data_lakehouse_ingest.config_loader import ConfigLoader

# Initialize Spark and MinIO -- works on Jupyter Hub
spark = get_spark_session()
minio_client = get_minio_client()

# Set up structured logger
pipeline_name = "pangenome"
target_table = "genome"
schema = "pangenome"

logger = setup_logger(
    pipeline_name=pipeline_name,
    target_table=target_table,
    schema=schema
)

# Option 1: Load config from MinIO
cfg_path = "s3a://test-bucket/data-load/config-json/pangenome_ke_genome.json"

# Option 2: Inline JSON config
cfg_path = r'''
{
  "tenant": "pangenome_${dataset}",
  "dataset": "pangenome_linkml",
  "paths": {
    "data_plane": "s3a://test-bucket/",
    "bronze_base": "s3a://test-bucket/pangenome_ke/bronze",
    "silver_base": "s3a://test-bucket/pangenome_linkml/silver"
  },
  "defaults": {
    "tsv": { "header": true, "delimiter": "\t", "inferSchema": false }
  },
  "tables": [
    {
      "name": "genome",
      "linkml_schema": "s3a://test-bucket/data-load/linkml-schema/genome_schema.yml",
      "schema_sql": null,
      "partition_by": null,
      "bronze_path": "s3a://test-bucket/pangenome_ke/bronze/table_genome_V1.1.tsv"
    }
  ]
}
'''

# Run ingestion
report = data_lakehouse_ingest_config(
    cfg_path,
    spark=spark,
    logger=logger,
    minio_client=minio_client
)

print(report)

```

---

## How it works

1. **ConfigLoader** reads the JSON config (inline or from MinIO).
2. **Schema Parsing**: Determines schema from LinkML or `schema_sql`.
3. **Spark Ingestion**: Reads files from the bronze layer using `spark.read`.
4. **Validation**: Applies schema casting and structural validation.
5. **Delta Write**: Writes curated data to the silver layer.
6. **Logging**: All activities are logged with contextual metadata (pipeline, schema, table).


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
from data_lakehouse_ingest import data_lakehouse_ingest_config
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
