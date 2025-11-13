"""
Provides a custom JSON encoder for structured logging and report generation.
Ensures that common non-serializable objects (e.g., datetime, Decimal, Path)
are converted into consistent, human-readable formats (ISO 8601, float, string).
"""

import json
import datetime
import decimal
from pathlib import Path
import uuid


class PipelineJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that safely handles common non-serializable objects
    encountered in ingestion reports and structured logs.

    This encoder ensures that all logged or serialized data structures can be
    converted to valid JSON without raising a TypeError. It provides consistent
    and human-readable representations for complex Python objects that the
    default `json` encoder cannot handle.

    Converts:
        - datetime, date → ISO 8601 strings
            Ensures consistent timestamp formatting (e.g., "2025-11-12T19:40:15Z")
            for both `datetime.datetime` and `datetime.date` instances, which are
            not natively JSON serializable.

        - Decimal → float
            Converts high-precision `decimal.Decimal` values into standard
            floating-point numbers, allowing numeric data to remain usable in
            downstream consumers (e.g., Spark jobs, APIs, dashboards).

        - Path → string
            Converts `pathlib.Path` objects (local filesystem or S3-style URIs)
            into their string equivalents for clear, portable representation.

        - UUID → string
            Converts `uuid.UUID` objects into canonical string form to preserve
            identity fields (useful for dataset or transaction identifiers).

        - Fallback → str(obj)
            For any other unrecognized type, gracefully converts the object to a
            string via `str(obj)` to avoid serialization errors. If string
            conversion fails, it falls back to a generic placeholder indicating
            the object’s type.

    Example:
        >>> from data_lakehouse_ingest.utils.json_encoder import PipelineJSONEncoder
        >>> import json, datetime, decimal, uuid, pathlib
        >>> data = {
        ...     "timestamp": datetime.datetime.utcnow(),
        ...     "amount": decimal.Decimal("123.45"),
        ...     "path": pathlib.Path("/tmp/data/file.csv"),
        ...     "id": uuid.uuid4()
        ... }
        >>> print(json.dumps(data, cls=PipelineJSONEncoder, indent=2))
    """

    def default(self, obj):
        # datetime and date objects
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()

        # Decimal (e.g., numeric precision from Spark or DB values)
        if isinstance(obj, decimal.Decimal):
            return float(obj)

        # Path objects (local or S3-style)
        if isinstance(obj, Path):
            return str(obj)

        # UUIDs
        if isinstance(obj, uuid.UUID):
            return str(obj)

        # Fallback: string conversion for any other complex type
        try:
            return str(obj)
        except Exception:
            return f"<Unserializable object of type {type(obj).__name__}>"
