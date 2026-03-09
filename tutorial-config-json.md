# Config-Driven Data Ingestion Tutorial

### Creating JSON Configurations for the data lakehouse ingest Library

The data lakehouse ingest library allows users to load data into **Delta Lake tables** in the BER Data Lakehouse using a **simple JSON configuration file**.

Instead of writing Spark ingestion code manually, users define:

* where the source data resides
* what schema the table should have
* how the table should be written

The ingestion framework then reads the configuration and performs the ingestion automatically.

This tutorial explains how to create a **valid configuration JSON file** step by step.

---

# 1. Basic Structure of the Config File

A minimal configuration JSON contains the following keys:

```json
{
  "tenant": "tenant_name",
  "dataset": "dataset_name",
  "tables": [
    {
      "name": "table_name",
      "schema_sql": "...",
      "bronze_path": "path_to_source_file"
    }
  ]
}
```

### Required Fields

| Field                    | Description                                    |
| ------------------------ | ---------------------------------------------- |
| `dataset`                | Dataset namespace where tables will be created |
| `tables`                 | List of tables to ingest                       |
| `name`                   | Table name                                     |
| `bronze_path`            | Location of the raw source file                |
| `schema_sql` OR `schema` | Defines the table structure                    |

<br>
<br>

---

# 2. Tenant and Namespace Behavior

The `tenant` field determines where the Delta tables are created.

### Example

```json
"tenant": "globalusers",
"dataset": "ontology_test1"
```

This creates tables in:

```
globalusers.ontology_test1.table_name
```

### Personal Namespace

If the `tenant` field is **not provided**, the framework automatically uses a **personal namespace**.

Example:

Example configuration:

```json
  "dataset": "ontology_test1"
```

If the current user is `akhan`, the resulting table location will be:

```text
u_akhan__ontology_test1.table_name
```

Here:

* `akhan` is the username retrieved from the MinIO client session

* `ontology_test1` is the dataset defined in the configuration JSON

The namespace `u_akhan__ontology_test1` is therefore automatically constructed as:

```text
u_<username>__<dataset>
```

This personal namespace allows users to run ingestion jobs and experiment with datasets without modifying shared tenant namespaces.


<br>
<br>

---

# 3. Paths Configuration (Optional)

The `paths` section allows you to define reusable storage locations.

Example:

```json
"paths": {
  "bronze_base": "s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/ontology-source/bronze"
}
```

### Path Types

| Path          | Purpose                                       |
| ------------- | --------------------------------------------- |
| `bronze_base` | Base path for raw source files                |


<br>
<br>

---

# 4. Default Reader Options (Optional)

You can define default Spark reader options for specific formats.

Example:

```json
"defaults": {
  "tsv": {
    "header": true,
    "delimiter": "\t",
    "inferSchema": false
  }
}
```

This prevents repeating reader options for every table.

Supported formats include:

| Format  |
| ------- |
| CSV     |
| TSV     |
| JSON    |
| XML     |
| Parquet |

<br>
<br>

---

# 5. Defining Tables

Each entry in the `tables` list defines one table to ingest.

Example:

```json
{
  "name": "prefix",
  "enabled": true,
  "schema_sql": "prefix STRING, base STRING",
  "partition_by": null,
  "bronze_path": "prefix.tsv"
}
```

You can make the table clearer by adding a **Required / Optional column** and mentioning the defaults in the description. Here is a clean Markdown version you can drop directly into your tutorial.

### Table Properties

| Field | Required | Description |
|------|----------|-------------|
| `name` | Yes | Name of the Delta table that will be created. |
| `bronze_path` | Yes | Location of the raw source file(s) in the Bronze layer. |
| `schema_sql` | One of (`schema_sql` or `schema`) | SQL-style schema definition (e.g., `"id STRING, name STRING"`). |
| `schema` | One of (`schema_sql` or `schema`) | Structured schema definition that allows metadata such as column comments. |
| `enabled` | Optional | If set to `false`, the table will be skipped during ingestion. Default is `true`. |
| `partition_by` | Optional | Column or list of columns used for Delta table partitioning. |
| `mode` | Optional | Write mode for the Delta table. Supported values: `overwrite` or `append`. Default is `overwrite`. |
| `format` | Optional | Source file format (e.g., `tsv`, `csv`, `json`, `xml`, `parquet`). |

### Notes

- **Default Write Mode**

  If `mode` is not specified, the ingestion framework uses:

  ```json
  "mode": "overwrite"
  ```

* **Format Detection**

  If the file extension is present in `bronze_path`, the ingestion framework can automatically determine the format.

  Example:

  ```json
  "bronze_path": "prefix.tsv"
  ```

* **When `format` Must Be Specified**

  If the path does not include a file extension (for example when reading a folder of files), you must explicitly specify the format.

  Example:

  ```json
  {
    "name": "gene_data",
    "format": "parquet",
    "bronze_path": "s3a://cdm-lake/datasets/gene_data/"
  }
  ```

  This is common when reading directories containing **multiple Parquet files**.

<br>
<br>

---

# 6. Enabling or Skipping Tables

The `enabled` flag allows selective ingestion.

Example:

```json
"enabled": false
```

This table will be **ignored during ingestion**.

This is useful when:

* testing pipelines
* temporarily disabling tables
* debugging ingestion issues

<br>
<br>

---

# 7. Schema Definition Using SQL

The simplest way to define a schema is with `schema_sql`.

Example:

```json
"schema_sql": "subject STRING, predicate STRING, object STRING"
```

Supported data types include:

### Primitive Types

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

### Complex Types

```
ARRAY
```
<br>
<br>

---


# 8. Structured Schema with Metadata

For richer metadata, use the `schema` field instead of `schema_sql`.

Example:

```json
"schema": [
  {
    "column": "prefix",
    "type": "string",
    "nullable": false,
    "comment": "Prefix label used in CURIEs"
  },
  {
    "column": "base",
    "type": "string",
    "nullable": false,
    "comment": "Base IRI associated with the prefix"
  }
]
```

Advantages of structured schema:

* column comments
* nullability control
* richer metadata
* AI-readiness for data catalogs

<br>
<br>

---

# 9. Handling Extra Columns

When a schema is defined using `schema_sql` or `schema`, the ingestion framework automatically ensures that **only the columns defined in the schema are written to the final Delta table**.

If the input data contains additional columns that are **not present in the schema**, those columns are **automatically dropped during ingestion**.

This behavior helps ensure that the final Delta table structure strictly follows the defined schema.

Example schema definition:

```json
"schema_sql": "subject STRING, predicate STRING, object STRING"
```

If the source file contains additional columns such as:

```text
subject, predicate, object, created_at, source_system
```

Only the following columns will be written to the Delta table:

```text
subject, predicate, object
```

The extra columns (`created_at`, `source_system`) will be **automatically removed during ingestion**.

This behavior applies whether the schema is defined using:

* `schema_sql`
* `schema` (structured schema with metadata)

<br>
<br>

---

# 10. Write Modes

Default write mode is:

```text
overwrite
```

To append data instead:

```json
"mode": "append"
```

Example:

```json
{
  "name": "prefix",
  "mode": "append"
}
```

<br>
<br>

---

# 11. Bronze Path Substitution

If `bronze_base` is defined, it can be reused in table paths.

Example:

```json
"bronze_path": "${bronze_base}/prefix.tsv"
```

This helps keep configs shorter and easier to maintain.

<br>
<br>

---

# 12. Using Only File Names

If all files are located in the `bronze_base` directory, the path can be simplified.

Example:

```json
"bronze_path": "statements.tsv"
```

The ingestion framework will resolve it relative to `bronze_base`.

<br>
<br>

---

# 13. Using Wildcards

Wildcards can be used when ingesting multiple files or folders.

Example:

```json
"bronze_path": "s3a://cdm-lake/bronze/run_*/prefix*.tsv"
```

This allows ingestion from:

* multiple runs
* multiple partitions
* batch directories

<br>
<br>

---

# 14. Full Example Configuration

```json
{
  "tenant": "globalusers",
  "dataset": "ontology_test1",

  "paths": {
    "bronze_base": "s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/ontology-source/bronze/run_20250819_020438/tsv_tables"
  },

  "defaults": {
    "tsv": { "header": true, "delimiter": "\t", "inferSchema": false }
  },

  "tables": [
    {
      "name": "entailed_edge",
      "enabled": true,
      "schema_sql": "subject STRING, predicate STRING, object STRING",
      "bronze_path": "${bronze_base}/entailed_edge.tsv"
    },
    {
      "name": "prefix",
      "enabled": true,
      "mode": "append",
      "schema": [
        {
          "column": "prefix",
          "type": "string",
          "nullable": false,
          "comment": "Prefix label used in CURIEs"
        },
        {
          "column": "base",
          "type": "string",
          "nullable": false,
          "comment": "Base IRI associated with the prefix"
        }
      ],
      "bronze_path": "${bronze_base}/prefix.tsv"
    }
  ]
}
```

In this configuration:

* The entailed_edge table uses `schema_sql`, which is a concise SQL-style schema definition.

* The prefix table uses `schema`, a structured schema that allows additional metadata such as column comments and nullability constraints.

<br>
<br>

---

# 15. Running the Ingestion

The configuration JSON can be provided to the ingestion framework in two ways:

## Option 1 — Inline JSON Configuration

You can define the configuration directly in a Python variable.

```python
cfg = """
{
  "dataset": "ontology_test1",
  "tables": [
    {
      "name": "prefix",
      "schema_sql": "prefix STRING, base STRING",
      "bronze_path": "prefix.tsv"
    }
  ]
}
"""

report = ingest(cfg)

report
```

---

## Option 2 — Config File Stored in MinIO

In production pipelines, the configuration JSON is typically stored in the **Bronze layer of the Data Lakehouse** (for example in MinIO or another S3-compatible store).

Example location:

```text
s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/ontology-source/config-json/ontology_config.json
```

You can then run ingestion by providing the path:

```python
cfg_path = "s3a://cdm-lake/tenant-general-warehouse/kbase/datasets/ontology-source/config-json/ontology_config.json"

report = ingest(cfg_path)

report
```

This approach allows configuration files to be **versioned and managed alongside datasets in the data lake**.

---


The ingestion framework will:

1. Read the config
2. Load data from the Bronze layer
3. Apply schema enforcement
4. Write curated Delta tables to the Silver layer
5. Generate an ingestion report

<br>
<br>

---

# 16. Best Practices

- Store configs alongside datasets in the Bronze layer
- Use `${bronze_base}` to avoid repeating paths
- Use `enabled=false` when testing ingestion pipelines
- Add column comments for data catalog compatibility


