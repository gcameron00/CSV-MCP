import json

from csv_mcp import engine
from csv_mcp.config import settings

URI_SCHEME = "csv://"


def list_resources() -> list[dict]:
    """Return one entry per CSV in watch_dir. Rebuilt fresh on every call."""
    if not settings.watch_dir.exists():
        return []

    files = sorted(
        f for f in settings.watch_dir.iterdir()
        if f.is_file() and f.suffix.lower() in settings.allowed_extensions
    )

    return [
        {
            "uri": f"{URI_SCHEME}{f.name}",
            "name": f.name,
            "description": f"CSV file: {f.name}",
            "mime_type": "text/csv",
        }
        for f in files
    ]


def read_resource(uri: str) -> str:
    """Return schema + row count + 5 sample rows as a JSON string."""
    if not uri.startswith(URI_SCHEME):
        raise ValueError(f"Unsupported URI scheme: {uri!r}. Expected csv://{{filename}}")

    filename = uri[len(URI_SCHEME):]
    path = settings.watch_dir / filename

    if not path.exists():
        raise FileNotFoundError(f"No such file in watch directory: {filename!r}")

    schema = engine.get_schema(path)
    row_count = engine.get_row_count(path)
    sample = engine.run_query(path, "SELECT * FROM data LIMIT 5", 5)

    return json.dumps(
        {
            "filename": filename,
            "schema": [{"column": r["column_name"], "type": r["column_type"]} for r in schema],
            "row_count": row_count,
            "sample": sample,
        },
        default=str,
    )
