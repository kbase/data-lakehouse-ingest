from data_lakehouse_ingest.orchestrator.error_utils import error_entry_for_exception

def test_error_entry_for_exception_returns_expected_dict():
    exc = ValueError("bad data")
    table = {"name": "test_table"}
    entry = error_entry_for_exception(table, exc)
    assert entry == {"name": "test_table", "error": "bad data", "status": "failed"}
