import duckdb
from pathlib import Path


def _connect(path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE VIEW data AS SELECT * FROM read_csv_auto(?)", [str(path)])
    return con


def run_query(path: Path, sql: str, max_rows: int) -> list[dict]:
    with _connect(path) as con:
        rel = con.execute(sql)
        columns = [desc[0] for desc in rel.description]
        rows = rel.fetchmany(max_rows)
        return [dict(zip(columns, row)) for row in rows]


def get_schema(path: Path) -> list[dict]:
    with _connect(path) as con:
        rel = con.execute("DESCRIBE SELECT * FROM data")
        columns = [desc[0] for desc in rel.description]
        return [dict(zip(columns, row)) for row in rel.fetchall()]


def get_row_count(path: Path) -> int:
    with _connect(path) as con:
        return con.execute("SELECT COUNT(*) FROM data").fetchone()[0]
