import logging

from data_lakehouse_ingest.utils.delta_comments import (
    _escape_sql_string,
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
    """Escapes single quotes by doubling them for safe SQL string literals."""
    assert _escape_sql_string("Bob's column") == "Bob''s column"


def test_try_alter_column_comment_uses_alter_column_syntax_and_escapes_quotes():
    """Applies column comments using ALTER COLUMN syntax and escapes quotes."""
    spark = FakeSpark()
    logger = logging.getLogger("test")

    ok = _try_alter_column_comment(spark, "db.tbl", "gene_id", "Bob's column", logger)

    assert ok is True
    assert len(spark.sql_calls) == 1
    assert "ALTER TABLE db.tbl ALTER COLUMN `gene_id` COMMENT 'Bob''s column'" in spark.sql_calls[0]


def test_try_alter_column_comment_logs_error_and_returns_false_on_failure(caplog):
    """Logs an error and returns False if Spark SQL execution fails."""
    spark = FakeSpark(sql_side_effects=[Exception("boom")])
    logger = logging.getLogger("test")

    with caplog.at_level(logging.ERROR):
        ok = _try_alter_column_comment(spark, "db.tbl", "gene_id", "c", logger)

    assert ok is False
    assert len(spark.sql_calls) == 1
    assert "Failed to set comment for db.tbl.gene_id" in caplog.text


def test_apply_comments_returns_failed_if_table_missing_when_required():
    """Returns a failed status when the target table does not exist."""
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
    """Applies only valid comments and skips missing or empty definitions."""
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
    assert any(
        d.get("status") == "applied" and d.get("column") == "gene_id" for d in report["details"]
    )
    assert any(
        "ALTER TABLE db.tbl ALTER COLUMN `gene_id` COMMENT 'Gene id'" in q for q in spark.sql_calls
    )


def test_apply_comments_marks_failed_when_alter_comment_fails_for_a_column():
    """Counts a failed comment application when ALTER COLUMN raises."""
    spark = FakeSpark(sql_side_effects=[Exception("nope")])
    schema = [{"column": "gene_id", "comment": "x"}]

    report = apply_comments_from_table_schema(
        spark, "db.tbl", schema, logger=logging.getLogger("test")
    )

    assert report["status"] == "failed"
    assert report["applied"] == 0
    assert report["skipped"] == 0
    assert report["failed"] == 1
    assert report["details"][0]["status"] == "failed"
    assert report["details"][0]["column"] == "gene_id"
