import logging
import pytest

from data_lakehouse_ingest.utils.delta_comments import (
    _escape_sql_string,
    _get_table_coltypes,
    _try_alter_column_comment,
    apply_comments_from_table_schema,
)


class FakeRow(dict):
    pass


class FakeCatalog:
    def __init__(self, exists: bool):
        self._exists = exists

    def tableExists(self, full_table_name: str) -> bool:
        return self._exists


class FakeSpark:
    def __init__(self, *, table_exists=True, describe_rows=None, sql_side_effects=None):
        self.catalog = FakeCatalog(table_exists)
        self._describe_rows = describe_rows or []
        self._sql_side_effects = list(sql_side_effects or [])
        self.sql_calls = []

    def sql(self, query: str):
        self.sql_calls.append(query)

        if self._sql_side_effects:
            effect = self._sql_side_effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
            return effect

        if query.strip().upper().startswith("DESCRIBE "):
            return type("DF", (), {"collect": lambda _self: self._describe_rows})()

        return type("DF", (), {"collect": lambda _self: []})()


def test_escape_sql_string_doubles_single_quotes():
    assert _escape_sql_string("Bob's column") == "Bob''s column"


def test_get_table_coltypes_parses_describe_output_and_skips_headers():
    rows = [
        FakeRow(col_name="gene_id", data_type="string"),
        FakeRow(col_name="# Partitioning", data_type=""),
        FakeRow(col_name="gene_cluster_id", data_type="string"),
        FakeRow(col_name="partition", data_type="string"),
        FakeRow(col_name="`weird`", data_type="int"),
        FakeRow(col_name="Detailed Table Information", data_type=""),
        FakeRow(col_name="after_details", data_type="string"),
    ]
    spark = FakeSpark(describe_rows=rows)
    ct = _get_table_coltypes(spark, "db.tbl")
    assert ct == {"gene_id": "string", "gene_cluster_id": "string", "weird": "int"}


def test_try_alter_column_comment_uses_modern_syntax_if_supported():
    spark = FakeSpark()
    logger = logging.getLogger("test")

    ok = _try_alter_column_comment(spark, "db.tbl", "gene_id", "Bob's column", logger)

    assert ok is True
    assert any(
        "ALTER TABLE db.tbl ALTER COLUMN `gene_id` COMMENT 'Bob''s column'" in q
        for q in spark.sql_calls
    )


def test_try_alter_column_comment_falls_back_to_change_column_when_alter_fails():
    rows = [
        FakeRow(col_name="gene_id", data_type="string"),
        FakeRow(col_name="gene_cluster_id", data_type="string"),
    ]
    spark = FakeSpark(
        describe_rows=rows,
        sql_side_effects=[Exception("ALTER COLUMN not supported")],
    )
    logger = logging.getLogger("test")

    ok = _try_alter_column_comment(spark, "db.tbl", "gene_id", "c", logger)

    assert ok is True
    assert any("DESCRIBE db.tbl" in q for q in spark.sql_calls)
    assert any(
        "ALTER TABLE db.tbl CHANGE COLUMN `gene_id` `gene_id` string COMMENT 'c'" in q
        for q in spark.sql_calls
    )


def test_try_alter_column_comment_fallback_warns_and_returns_false_if_column_missing(caplog):
    rows = [FakeRow(col_name="other_col", data_type="string")]
    spark = FakeSpark(
        describe_rows=rows,
        sql_side_effects=[Exception("ALTER COLUMN not supported")],
    )
    logger = logging.getLogger("test")

    with caplog.at_level(logging.WARNING):
        ok = _try_alter_column_comment(spark, "db.tbl", "gene_id", "c", logger)

    assert ok is False
    assert "skipping comment" in caplog.text.lower()


def test_apply_comments_returns_failed_if_table_missing_when_required():
    spark = FakeSpark(table_exists=False)
    report = apply_comments_from_table_schema(
        spark,
        "db.tbl",
        table_schema=[{"column": "gene_id", "comment": "x"}],
        logger=logging.getLogger("test"),
        require_existing_table=True,
    )
    assert report["status"] == "failed"
    assert "Table does not exist" in report["error"]


def test_apply_comments_skips_missing_or_empty_comments_and_applies_valid_ones():
    spark = FakeSpark()
    schema = [
        {"column": "gene_id", "comment": "Gene id"},
        {"column": "no_comment"},
        {"name": "blank_comment", "comment": "   "},
        {"comment": "missing col"},
    ]

    report = apply_comments_from_table_schema(
        spark, "db.tbl", schema, logger=logging.getLogger("test")
    )

    assert report["status"] == "success"
    assert report["applied"] == 1
    assert report["skipped"] == 3
    assert report["failed"] == 0
    assert any(d.get("status") == "applied" and d.get("column") == "gene_id" for d in report["details"])
