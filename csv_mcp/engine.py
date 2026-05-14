import duckdb
from pathlib import Path


def _connect(path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE VIEW data AS SELECT * FROM read_csv_auto(?)", [str(path)])
    return con


def run_query(path: Path, sql: str, max_rows: int, params: list | None = None) -> list[dict]:
    with _connect(path) as con:
        rel = con.execute(sql, params or [])
        columns = [desc[0] for desc in rel.description]
        rows = rel.fetchmany(max_rows)
        return [dict(zip(columns, row)) for row in rows]


def merge(path_a: Path, path_b: Path, how: str, on: str | None, max_rows: int) -> list[dict]:
    con = duckdb.connect()
    con.execute("CREATE VIEW a AS SELECT * FROM read_csv_auto(?)", [str(path_a)])
    con.execute("CREATE VIEW b AS SELECT * FROM read_csv_auto(?)", [str(path_b)])

    if how == "concat":
        sql = "SELECT * FROM a UNION ALL SELECT * FROM b"
    elif how in ("inner", "left", "right"):
        if not on:
            raise ValueError(f"'on' is required for a {how} join")
        q = on.replace('"', '""')
        sql = f'SELECT * FROM a {how.upper()} JOIN b USING ("{q}")'
    else:
        raise ValueError(f"how must be 'concat', 'inner', 'left', or 'right', got {how!r}")

    rel = con.execute(sql)
    columns = [desc[0] for desc in rel.description]
    rows = rel.fetchmany(max_rows)
    con.close()
    return [dict(zip(columns, row)) for row in rows]


def fetch_all(path: Path, sql: str, params: list | None = None) -> list[dict]:
    """Fetch every matching row — no max_rows cap. For internal write operations only."""
    with _connect(path) as con:
        rel = con.execute(sql, params or [])
        columns = [desc[0] for desc in rel.description]
        return [dict(zip(columns, row)) for row in rel.fetchall()]


def get_schema(path: Path) -> list[dict]:
    with _connect(path) as con:
        rel = con.execute("DESCRIBE SELECT * FROM data")
        columns = [desc[0] for desc in rel.description]
        return [dict(zip(columns, row)) for row in rel.fetchall()]


def get_row_count(path: Path) -> int:
    with _connect(path) as con:
        return con.execute("SELECT COUNT(*) FROM data").fetchone()[0]
