from csv_mcp import engine


def test_get_schema(tmp_csv_dir):
    schema = engine.get_schema(tmp_csv_dir / "people.csv")
    names = [r["column_name"] for r in schema]
    assert names == ["name", "age", "city"]


def test_get_row_count(tmp_csv_dir):
    assert engine.get_row_count(tmp_csv_dir / "people.csv") == 3


def test_run_query_basic(tmp_csv_dir):
    rows = engine.run_query(tmp_csv_dir / "people.csv", "SELECT * FROM data", 100)
    assert len(rows) == 3


def test_run_query_where(tmp_csv_dir):
    rows = engine.run_query(
        tmp_csv_dir / "people.csv",
        "SELECT * FROM data WHERE city = 'New York'",
        100,
    )
    assert len(rows) == 2
    assert all(r["city"] == "New York" for r in rows)


def test_run_query_max_rows(tmp_csv_dir):
    rows = engine.run_query(tmp_csv_dir / "people.csv", "SELECT * FROM data", 2)
    assert len(rows) == 2


def test_run_query_params(tmp_csv_dir):
    rows = engine.run_query(
        tmp_csv_dir / "people.csv",
        "SELECT * FROM data WHERE name = ?",
        100,
        params=["Alice"],
    )
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_fetch_all(tmp_csv_dir):
    rows = engine.fetch_all(tmp_csv_dir / "people.csv", "SELECT * FROM data")
    assert len(rows) == 3


def test_merge_concat(tmp_csv_dir):
    rows = engine.merge(
        tmp_csv_dir / "people.csv",
        tmp_csv_dir / "people.csv",
        "concat",
        None,
        100,
    )
    assert len(rows) == 6


def test_merge_inner_join(tmp_csv_dir):
    rows = engine.merge(
        tmp_csv_dir / "people.csv",
        tmp_csv_dir / "scores.csv",
        "inner",
        "name",
        100,
    )
    assert len(rows) == 3
    assert "score" in rows[0]


def test_merge_invalid_how(tmp_csv_dir):
    import pytest
    with pytest.raises(ValueError):
        engine.merge(tmp_csv_dir / "people.csv", tmp_csv_dir / "scores.csv", "cross", None, 100)


def test_merge_join_missing_on(tmp_csv_dir):
    import pytest
    with pytest.raises(ValueError):
        engine.merge(tmp_csv_dir / "people.csv", tmp_csv_dir / "scores.csv", "inner", None, 100)
