from unittest.mock import MagicMock, patch
from dataclasses import asdict

from data_lakehouse_ingest.orchestrator.table_batch_processor import process_tables
from data_lakehouse_ingest.orchestrator.models import (
    TableProcessSuccess,
    TableProcessFailure,
    ProcessStatus,
    InputSource,
    WriteMode,
)
from data_lakehouse_ingest.orchestrator.schema_utils import SchemaSource


def make_success(name: str = "table") -> TableProcessSuccess:
    """
    Create a minimal TableProcessSuccess object for use in tests.
    Allows overriding the table name.
    """
    return TableProcessSuccess(
        name=name,
        tenant=None,
        target_table=f"db.{name}",
        mode=WriteMode.OVERWRITE,
        format=None,
        schema_source=SchemaSource.SCHEMA_SQL,
        input_source=InputSource.DATAFRAME,
        bronze_path=None,
        rows_in=1,
        rows_written=1,
        extra_columns_dropped=[],
        elapsed_sec=0.1,
        status=ProcessStatus.SUCCESS,
        table_comment_report=None,
        column_comments_report=None,
    )


def make_failure(name: str = "bad_table", traceback: str | None = None) -> TableProcessFailure:
    """
    Create a minimal TableProcessFailure object for testing error handling.
    Optionally includes traceback information.
    """
    return TableProcessFailure(
        name=name,
        error="boom",
        phase="write",
        bronze_path="s3a://bucket/input.csv",
        format="csv",
        input_source=InputSource.BRONZE,
        status=ProcessStatus.FAILED,
        traceback=traceback,
    )


def test_process_tables_success():
    """
    Test that process_tables correctly iterates over tables,
    delegates to process_table(), and aggregates reports.
    """

    # --- Mocks ---
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "table1"}, {"name": "table2"}]}

    started_at = "2025-01-01T00:00:00Z"

    # Mock process_table to return different rows for each call
    mock_report_1 = TableProcessSuccess(
        name="table1",
        tenant=None,
        target_table="db.table1",
        mode=WriteMode.OVERWRITE,
        format="csv",
        schema_source=SchemaSource.SCHEMA_SQL,
        input_source=InputSource.BRONZE,
        bronze_path="s3a://bucket/table1.csv",
        rows_in=10,
        rows_written=10,
        extra_columns_dropped=[],
        elapsed_sec=1.0,
        status=ProcessStatus.SUCCESS,
        table_comment_report=None,
        column_comments_report=None,
    )

    mock_report_2 = TableProcessSuccess(
        name="table2",
        tenant=None,
        target_table="db.table2",
        mode=WriteMode.OVERWRITE,
        format="csv",
        schema_source=SchemaSource.SCHEMA_SQL,
        input_source=InputSource.BRONZE,
        bronze_path="s3a://bucket/table2.csv",
        rows_in=20,
        rows_written=20,
        extra_columns_dropped=[],
        elapsed_sec=1.2,
        status=ProcessStatus.SUCCESS,
        table_comment_report=None,
        column_comments_report=None,
    )

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        side_effect=[mock_report_1, mock_report_2],
    ) as mock_process_table:
        table_reports, error_list = process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
        )

    # --- Assertions ---
    assert table_reports == [asdict(mock_report_1), asdict(mock_report_2)]
    assert error_list == []

    # Ensure process_table was called twice (once per table)
    assert mock_process_table.call_count == 2


def test_process_tables_with_exception():
    """
    Test that when process_table raises an exception, the error entry is created
    and appended to both table_reports and error_list.
    """

    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "boom_table"}]}
    started_at = "2025-01-01T00:00:00Z"

    # Mock process_table to raise an exception
    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        side_effect=Exception("Test error"),
    ):
        with patch(
            "data_lakehouse_ingest.orchestrator.table_batch_processor.error_entry_for_exception",
            return_value={"status": "error", "table": "boom_table", "error": "Test error"},
        ) as mock_error_entry:
            table_reports, error_list = process_tables(
                spark=spark,
                logger=logger,
                loader=loader,
                ctx=ctx,
                started_at=started_at,
                minio_client=minio_client,
            )

    # --- Assertions ---
    assert len(table_reports) == 1
    assert len(error_list) == 1

    assert table_reports[0] == {"status": "error", "table": "boom_table", "error": "Test error"}

    assert error_list[0] == table_reports[0]

    # Ensure error_entry_for_exception was used
    mock_error_entry.assert_called_once()


def test_process_tables_passes_df_override_when_present():
    """
    If dataframes are provided and the table name matches, process_tables should pass
    the matching dataframe via df_override to process_table().
    """
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    df1 = MagicMock(name="df_table1")
    df2 = MagicMock(name="df_table2")

    ctx = {"tables": [{"name": "table1"}, {"name": "table2"}]}
    started_at = "2025-01-01T00:00:00Z"
    dataframes = {"table1": df1, "table2": df2}

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        side_effect=[make_success("table1"), make_success("table2")],
    ) as mock_process_table:
        process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
            dataframes=dataframes,
        )

    # Validate each call got the correct df_override
    first_call_kwargs = mock_process_table.call_args_list[0].kwargs
    second_call_kwargs = mock_process_table.call_args_list[1].kwargs

    assert first_call_kwargs["df_override"] is df1
    assert second_call_kwargs["df_override"] is df2


def test_process_tables_df_override_none_when_missing_or_no_dataframes():
    """
    If dataframes are not provided OR table name doesn't exist in dataframes,
    df_override should be None.
    """
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "table1"}]}
    started_at = "2025-01-01T00:00:00Z"

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        return_value=make_success("table1"),
    ) as mock_process_table:
        process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
            dataframes=None,  # explicit
        )

    kwargs = mock_process_table.call_args.kwargs
    assert kwargs["df_override"] is None

    # Now: table name not found in dataframes dict
    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        return_value=make_success("table1"),
    ) as mock_process_table2:
        process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
            dataframes={"other_table": MagicMock()},
        )

    kwargs2 = mock_process_table2.call_args.kwargs
    assert kwargs2["df_override"] is None


def test_process_tables_table_without_name_does_not_crash_df_lookup():
    """
    If a table dict has no 'name', the code uses default "" for lookup.
    Ensure it still calls process_table and passes df_override correctly (None unless "" exists).
    """
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"format": "csv"}]}  # no name
    started_at = "2025-01-01T00:00:00Z"

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        return_value=make_success("table1"),
    ) as mock_process_table:
        process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
            dataframes={"table1": MagicMock()},
        )

    kwargs = mock_process_table.call_args.kwargs
    assert kwargs["df_override"] is None


def test_process_tables_continues_after_one_table_fails():
    """
    Ensure one failing table does not prevent subsequent tables from processing.
    Also validates error_entry_for_exception is called for the failing one.
    """
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "bad"}, {"name": "good"}]}
    started_at = "2025-01-01T00:00:00Z"

    good_report = make_success("good")
    bad_error_entry = {"status": "error", "table": "bad", "error": "boom"}

    def side_effect(*args, **kwargs):
        if kwargs["table"]["name"] == "bad":
            raise Exception("boom")
        return good_report

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        side_effect=side_effect,
    ) as mock_process_table:
        with patch(
            "data_lakehouse_ingest.orchestrator.table_batch_processor.error_entry_for_exception",
            return_value=bad_error_entry,
        ) as mock_err:
            table_reports, error_list = process_tables(
                spark=spark,
                logger=logger,
                loader=loader,
                ctx=ctx,
                started_at=started_at,
                minio_client=minio_client,
            )

    # bad -> error entry, good -> success
    assert table_reports == [bad_error_entry, asdict(good_report)]
    assert error_list == [bad_error_entry]

    assert mock_process_table.call_count == 2
    mock_err.assert_called_once()  # error entry only for the failing table


def test_process_tables_passes_through_required_context_fields():
    """
    Verifies process_tables passes ctx and started_at through correctly to process_table.
    """
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "table1"}], "pipeline": "p1"}
    started_at = "2025-01-01T00:00:00Z"

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        return_value=make_success("table1"),
    ) as mock_process_table:
        process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
        )

    kwargs = mock_process_table.call_args.kwargs
    assert kwargs["ctx"] is ctx
    assert kwargs["run_started_at_iso"] == started_at
    assert kwargs["minio_client"] is minio_client


def test_process_tables_adds_error_entry_when_process_table_returns_failure():
    """
    Ensures that when process_table returns a TableProcessFailure,
    an appropriate error entry is added to the error_list.
    """
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "bad_table"}]}
    started_at = "2025-01-01T00:00:00Z"

    failure_report = make_failure("bad_table")

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        return_value=failure_report,
    ):
        table_reports, error_list = process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
        )

    assert table_reports == [asdict(failure_report)]
    assert error_list == [
        {
            "phase": "write",
            "table": "bad_table",
            "error": "boom",
        }
    ]


def test_process_tables_adds_traceback_when_failure_report_contains_it():
    """
    Verifies that traceback information from a TableProcessFailure
    is included in the generated error entry.
    """
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "bad_table"}]}
    started_at = "2025-01-01T00:00:00Z"

    failure_report = make_failure("bad_table", traceback="Traceback details")

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        return_value=failure_report,
    ):
        table_reports, error_list = process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
        )

    assert table_reports == [asdict(failure_report)]
    assert error_list == [
        {
            "phase": "write",
            "table": "bad_table",
            "error": "boom",
            "traceback": "Traceback details",
        }
    ]
