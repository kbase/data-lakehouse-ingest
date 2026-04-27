import logging

import pytest

from data_lakehouse_ingest.utils.delta_comments import (
    _escape_sql_string,
    _try_alter_column_comment,
    _try_set_table_comment,
    apply_comments_from_table_schema,
    apply_table_comment,
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


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("Bob's column", "Bob's column"),
        ('Column "status"', 'Column \\"status\\"'),
        (r"C:\data\input", r"C:\\data\\input"),
    ],
)
def test_escape_sql_string_handles_quotes_and_backslashes(raw_value, expected):
    """Escapes only the characters that must be escaped for COMMENT "..." strings."""
    assert _escape_sql_string(raw_value) == expected


def test_try_alter_column_comment_uses_alter_column_syntax_and_double_quoted_comment():
    """Applies column comments using ALTER COLUMN syntax and COMMENT "..."."""
    spark = FakeSpark()
    logger = logging.getLogger("test")

    ok = _try_alter_column_comment(spark, "db.tbl", "gene_id", "Bob's column", logger)

    assert ok is True
    assert len(spark.sql_calls) == 1
    assert 'ALTER TABLE db.tbl ALTER COLUMN `gene_id` COMMENT "Bob\'s column"' in spark.sql_calls[0]


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
        'ALTER TABLE db.tbl ALTER COLUMN `gene_id` COMMENT "Gene id"' in q for q in spark.sql_calls
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


def test_apply_comments_serializes_dict_comment_and_applies_it():
    """Serializes dict comments to JSON and applies them as escaped SQL column comments."""
    spark = FakeSpark()
    schema = [
        {
            "column": "gene_id",
            "comment": {
                "description": "Gene identifier",
                "source": "/api/v1",
                "flags": {"demo": True},
            },
        }
    ]

    report = apply_comments_from_table_schema(
        spark, "db.tbl", schema, logger=logging.getLogger("test")
    )

    assert report["status"] == "success"
    assert report["applied"] == 1
    assert report["skipped"] == 0
    assert report["failed"] == 0

    sql_text = spark.sql_calls[0]
    assert 'ALTER TABLE db.tbl ALTER COLUMN `gene_id` COMMENT "' in sql_text
    assert '\\"description\\": \\"Gene identifier\\"' in sql_text
    assert '\\"source\\": \\"/api/v1\\"' in sql_text
    assert '\\"flags\\": {\\"demo\\": true}' in sql_text


def test_apply_comments_preserves_unicode_in_dict_comment():
    """Preserves Unicode characters when serializing dict comments to JSON."""
    spark = FakeSpark()
    schema = [
        {
            "column": "species",
            "comment": {"description": "Café naïve β-cell 漢字"},
        }
    ]

    report = apply_comments_from_table_schema(
        spark, "db.tbl", schema, logger=logging.getLogger("test")
    )

    assert report["status"] == "success"
    sql_text = spark.sql_calls[0]
    assert "Café naïve β-cell 漢字" in sql_text
    assert "\\u" not in sql_text


def test_apply_comments_skips_non_string_non_dict_comment():
    """Skips comments that are neither strings nor dicts."""
    spark = FakeSpark()
    schema = [
        {"column": "gene_id", "comment": 123},
    ]

    report = apply_comments_from_table_schema(
        spark, "db.tbl", schema, logger=logging.getLogger("test")
    )

    assert report["status"] == "success"
    assert report["applied"] == 0
    assert report["skipped"] == 1
    assert report["failed"] == 0
    assert report["details"][0]["status"] == "skipped"
    assert report["details"][0]["reason"] == "no comment"


def test_try_set_table_comment_uses_comment_on_table_syntax():
    """Applies a table-level comment using COMMENT ON TABLE syntax when supported."""
    spark = FakeSpark()
    logger = logging.getLogger("test")

    ok = _try_set_table_comment(spark, "db.tbl", "Table level comment", logger)

    assert ok is True
    assert len(spark.sql_calls) == 1
    assert 'COMMENT ON TABLE db.tbl IS "Table level comment"' in spark.sql_calls[0]


def test_try_set_table_comment_falls_back_to_tblproperties_when_comment_on_table_fails(caplog):
    """Falls back to ALTER TABLE ... SET TBLPROPERTIES when COMMENT ON TABLE fails."""
    spark = FakeSpark(sql_side_effects=[Exception("syntax not supported"), []])
    logger = logging.getLogger("test")

    with caplog.at_level(logging.WARNING):
        ok = _try_set_table_comment(spark, "db.tbl", "Table level comment", logger)

    assert ok is True
    assert len(spark.sql_calls) == 2
    assert 'COMMENT ON TABLE db.tbl IS "Table level comment"' in spark.sql_calls[0]
    assert (
        'ALTER TABLE db.tbl SET TBLPROPERTIES ("comment" = "Table level comment")'
        in spark.sql_calls[1]
    )
    assert "COMMENT ON TABLE failed for db.tbl; trying TBLPROPERTIES fallback" in caplog.text


def test_try_set_table_comment_returns_false_when_both_attempts_fail(caplog):
    """Returns False and logs an error when both table comment SQL strategies fail."""
    spark = FakeSpark(sql_side_effects=[Exception("first fail"), Exception("second fail")])
    logger = logging.getLogger("test")

    with caplog.at_level(logging.ERROR):
        ok = _try_set_table_comment(spark, "db.tbl", "Table level comment", logger)

    assert ok is False
    assert len(spark.sql_calls) == 2
    assert "Failed to set table comment for db.tbl" in caplog.text


def test_apply_table_comment_returns_failed_if_table_missing_when_required():
    """Returns a failed report when the target table does not exist."""
    spark = FakeSpark(table_exists=False)

    report = apply_table_comment(
        spark,
        "db.tbl",
        "Table level comment",
        logger=logging.getLogger("test"),
        require_existing_table=True,
    )

    assert report["table"] == "db.tbl"
    assert report["status"] == "failed"
    assert "Table does not exist" in report["error"]


def test_apply_table_comment_skips_when_comment_is_missing():
    """Skips table comment application when no table comment is provided."""
    spark = FakeSpark()

    report = apply_table_comment(
        spark,
        "db.tbl",
        None,
        logger=logging.getLogger("test"),
    )

    assert report == {
        "table": "db.tbl",
        "status": "skipped",
        "reason": "no table comment",
    }
    assert spark.sql_calls == []


def test_apply_table_comment_applies_string_comment():
    """Applies a plain string table-level comment successfully."""
    spark = FakeSpark()

    report = apply_table_comment(
        spark,
        "db.tbl",
        "Reference table for genomes",
        logger=logging.getLogger("test"),
    )

    assert report == {
        "table": "db.tbl",
        "status": "success",
        "applied": True,
    }
    assert len(spark.sql_calls) == 1
    assert 'COMMENT ON TABLE db.tbl IS "Reference table for genomes"' in spark.sql_calls[0]


def test_apply_table_comment_serializes_dict_comment_and_preserves_unicode():
    """Serializes dict-based table comments to JSON and preserves Unicode characters."""
    spark = FakeSpark()

    report = apply_table_comment(
        spark,
        "db.tbl",
        {"description": "Café naïve β-cell 漢字", "owner": "arkinlab"},
        logger=logging.getLogger("test"),
    )

    assert report == {
        "table": "db.tbl",
        "status": "success",
        "applied": True,
    }

    sql_text = spark.sql_calls[0]
    assert 'COMMENT ON TABLE db.tbl IS "' in sql_text
    assert "Café naïve β-cell 漢字" in sql_text
    assert '\\"description\\": \\"Café naïve β-cell 漢字\\"' in sql_text
    assert "\\u" not in sql_text


def test_apply_table_comment_skips_non_string_non_dict_comment():
    """Skips invalid table comments that are neither strings nor dicts."""
    spark = FakeSpark()

    report = apply_table_comment(
        spark,
        "db.tbl",
        123,
        logger=logging.getLogger("test"),
    )

    assert report == {
        "table": "db.tbl",
        "status": "skipped",
        "reason": "no table comment",
    }
    assert spark.sql_calls == []
