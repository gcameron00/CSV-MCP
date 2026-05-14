import csv
from pathlib import Path

from csv_mcp import engine
from csv_mcp.config import settings
from csv_mcp.tools.read import _VALID_OPS, _resolve

_INVERSE_OP = {
    "=": "!=",
    "!=": "=",
    "<": ">=",
    ">": "<=",
    "<=": ">",
    ">=": "<",
}


def write_file(filename: str, data: list[dict]) -> Path:
    path = settings.watch_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        if not data:
            return path
        writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)

    return path


def delete_rows(filename: str, col: str, op: str, value: str) -> Path:
    if op not in _VALID_OPS:
        raise ValueError(f"op must be one of {sorted(_VALID_OPS)}, got {op!r}")

    path = _resolve(filename)
    q = col.replace('"', '""')

    if op == "contains":
        sql = f'SELECT * FROM data WHERE "{q}" NOT LIKE ?'
        param = f"%{value}%"
    elif op == "startswith":
        sql = f'SELECT * FROM data WHERE "{q}" NOT LIKE ?'
        param = f"{value}%"
    elif op == "endswith":
        sql = f'SELECT * FROM data WHERE "{q}" NOT LIKE ?'
        param = f"%{value}"
    else:
        sql = f'SELECT * FROM data WHERE "{q}" {_INVERSE_OP[op]} ?'
        param = value

    surviving = engine.fetch_all(path, sql, params=[param])
    return write_file(filename, surviving)
