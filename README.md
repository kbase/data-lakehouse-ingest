# data\_lakehouse\_ingest

End-to-end ingestion framework for loading data into the BER Data Lakehouse using pyspark and minio.

It reads flat files (CSV, TSV, JSON, XML) from MinIO (S3-compatible), applies optional schema-based casting using SQL-style schema definitions, and writes curated Delta tables to a Spark-based Lakehouse.

---

## Environment and Python management

This package uses [uv](https://docs.astral.sh/uv/) for Python environment and package management.
See the [installation instructions](https://docs.astral.sh/uv/getting-started/installation/) to set up `uv` on your system.

The package requires Python **3.13+**.
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

The following example demonstrates a full ingestion workflow using Spark and MinIO.

```python
# Option 1: Load config from MinIO
cfg_path = "s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/pangenome_ke-source/config-json/pangenome_ke_genome.json"

# Option 2: Inline JSON config
cfg_path = r'''
{
  "tenant": "kbase",
  "dataset": "ke_pangenome",
  "paths": {
    "data_plane": "s3a://cdm-lake/",
    "bronze_base": "s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/pangenome_ke-source/bronze/",
  },
  "defaults": {
    "tsv": { "header": true, "delimiter": "\t", "inferSchema": false }
  },
  "tables": [
    {
      "name": "genome",
      "enabled": true,
      "schema_sql": "genome_id STRING, gtdb_species_clade_id STRING, gtdb_taxonomy_id STRING, ncbi_biosample_id STRING, fna_file_path_nersc STRING, faa_file_path_nersc STRING",
      "partition_by": null,
      "bronze_path": "s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/pangenome_ke-source/bronze/tsv_files/table_genome_V1.1.tsv"
    }
  ]
}
'''

# Run ingestion
report = ingest(
    cfg_path,
)

print(report)

```

---

## How it works

1. **ConfigLoader** reads the JSON config (inline or from MinIO).
2. **Schema Parsing**: Determines schema from `schema_sql`.
3. **Spark Ingestion**: Reads files from the bronze layer using `spark.read`.
4. **Validation**: Applies schema casting and structural validation.
5. **Delta Write**: Writes curated data to the silver layer.
6. **Logging**: All activities are logged with contextual metadata (pipeline, schema, table).

---

## Supported schema_sql Data Types

The schema_sql field uses Spark SQL–compatible primitive data types for defining table schemas.
Only the following data types are currently supported by the ingestion framework:
```
STRING
INT
INTEGER
BIGINT
LONG
DOUBLE
FLOAT
BOOLEAN
DATE
TIMESTAMP
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
from data_lakehouse_ingest import ingest
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
