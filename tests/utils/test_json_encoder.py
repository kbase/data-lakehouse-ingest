import json
import datetime
import decimal
import uuid
from pathlib import Path

from data_lakehouse_ingest.utils.json_encoder import PipelineJSONEncoder


def test_json_encoder_serializes_supported_types():
    # Prepare test data
    now_dt = datetime.datetime(2025, 11, 12, 19, 40, 15)
    today_date = datetime.date(2025, 11, 12)
    dec_value = decimal.Decimal("123.45")
    path_value = Path("/tmp/test/file.txt")
    uuid_value = uuid.uuid4()

    data = {
        "dt": now_dt,
        "date": today_date,
        "dec": dec_value,
        "path": path_value,
        "uuid": uuid_value,
    }

    # Serialize using PipelineJSONEncoder
    result = json.loads(json.dumps(data, cls=PipelineJSONEncoder))

    # Assertions
    assert result["dt"] == now_dt.isoformat()
    assert result["date"] == today_date.isoformat()
    assert result["dec"] == float(dec_value)
    assert result["path"] == str(path_value)
    assert result["uuid"] == str(uuid_value)


def test_json_encoder_fallback_to_string():
    class CustomObj:
        def __str__(self):
            return "custom-object"

    obj = CustomObj()

    data = {"obj": obj}

    result = json.loads(json.dumps(data, cls=PipelineJSONEncoder))

    assert result["obj"] == "custom-object"


def test_json_encoder_handles_unserializable_object():
    # Object whose __str__() raises an exception
    class BadObj:
        def __str__(self):
            raise ValueError("cannot stringify")

    obj = BadObj()

    result = json.loads(json.dumps({"bad": obj}, cls=PipelineJSONEncoder))

    assert result["bad"] == "<Unserializable object of type BadObj>"
