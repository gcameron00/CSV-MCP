from pathlib import Path

from csv_mcp import engine
from csv_mcp.config import settings

_VALID_OPS = {"=", "!=", "<", ">", "<=", ">=", "contains", "startswith", "endswith"}


def _resolve(filename: str) -> Path:
    return settings.watch_dir / filename


def get_schema(filename: str) -> list[dict]:
    return engine.get_schema(_resolve(filename))


def get_sample(filename: str, n: int) -> list[dict]:
    n = min(n, settings.max_rows)
    return engine.run_query(_resolve(filename), f"SELECT * FROM data LIMIT {n}", n)


def query(filename: str, sql: str) -> list[dict]:
    return engine.run_query(_resolve(filename), sql, settings.max_rows)


def get_stats(filename: str) -> list[dict]:
    path = _resolve(filename)
    schema = engine.get_schema(path)
    cols = [row["column_name"] for row in schema]

    exprs = []
    for i, col in enumerate(cols):
        q = col.replace('"', '""')
        exprs += [
            f'MIN("{q}") AS _min_{i}',
            f'MAX("{q}") AS _max_{i}',
            f'AVG(TRY_CAST("{q}" AS DOUBLE)) AS _mean_{i}',
            f'COUNT(*) - COUNT("{q}") AS _nulls_{i}',
        ]

    row = engine.run_query(path, f"SELECT {', '.join(exprs)} FROM data", 1)[0]

    return [
        {
            "column": col,
            "min": row[f"_min_{i}"],
            "max": row[f"_max_{i}"],
            "mean": row[f"_mean_{i}"],
            "null_count": row[f"_nulls_{i}"],
        }
        for i, col in enumerate(cols)
    ]


def filter_rows(filename: str, col: str, op: str, value: str) -> list[dict]:
    if op not in _VALID_OPS:
        raise ValueError(f"op must be one of {sorted(_VALID_OPS)}, got {op!r}")

    path = _resolve(filename)
    q = col.replace('"', '""')

    if op == "contains":
        sql = f'SELECT * FROM data WHERE "{q}" LIKE ?'
        param = f"%{value}%"
    elif op == "startswith":
        sql = f'SELECT * FROM data WHERE "{q}" LIKE ?'
        param = f"{value}%"
    elif op == "endswith":
        sql = f'SELECT * FROM data WHERE "{q}" LIKE ?'
        param = f"%{value}"
    else:
        sql = f'SELECT * FROM data WHERE "{q}" {op} ?'
        param = value

    return engine.run_query(path, sql, settings.max_rows, params=[param])


def merge_files(filename_a: str, filename_b: str, how: str, on: str | None = None) -> list[dict]:
    return engine.merge(
        _resolve(filename_a),
        _resolve(filename_b),
        how,
        on,
        settings.max_rows,
    )
