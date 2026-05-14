import pytest

from csv_mcp import config
from csv_mcp.tools import read


def test_get_schema(tmp_csv_dir):
    schema = read.get_schema("people.csv")
    names = [r["column_name"] for r in schema]
    assert "name" in names and "age" in names and "city" in names


def test_get_sample(tmp_csv_dir):
    rows = read.get_sample("people.csv", 2)
    assert len(rows) == 2


def test_get_sample_capped_at_max_rows(tmp_csv_dir, monkeypatch):
    monkeypatch.setattr(config.settings, "max_rows", 1)
    rows = read.get_sample("people.csv", 100)
    assert len(rows) == 1


def test_query(tmp_csv_dir):
    rows = read.query("people.csv", "SELECT * FROM data WHERE city = 'New York'")
    assert len(rows) == 2


def test_query_respects_max_rows(tmp_csv_dir, monkeypatch):
    monkeypatch.setattr(config.settings, "max_rows", 1)
    rows = read.query("people.csv", "SELECT * FROM data")
    assert len(rows) == 1


def test_get_stats_columns(tmp_csv_dir):
    stats = read.get_stats("people.csv")
    cols = [s["column"] for s in stats]
    assert cols == ["name", "age", "city"]


def test_get_stats_numeric(tmp_csv_dir):
    stats = read.get_stats("people.csv")
    age = next(s for s in stats if s["column"] == "age")
    assert int(age["min"]) == 25
    assert int(age["max"]) == 35
    assert age["null_count"] == 0


def test_get_stats_no_nulls(tmp_csv_dir):
    stats = read.get_stats("people.csv")
    assert all(s["null_count"] == 0 for s in stats)


def test_filter_rows_eq(tmp_csv_dir):
    rows = read.filter_rows("people.csv", "city", "=", "New York")
    assert len(rows) == 2
    assert all(r["city"] == "New York" for r in rows)


def test_filter_rows_neq(tmp_csv_dir):
    rows = read.filter_rows("people.csv", "city", "!=", "New York")
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"


def test_filter_rows_contains(tmp_csv_dir):
    # "Alice" and "Charlie" both contain "li"
    rows = read.filter_rows("people.csv", "name", "contains", "li")
    assert len(rows) == 2


def test_filter_rows_startswith(tmp_csv_dir):
    rows = read.filter_rows("people.csv", "name", "startswith", "A")
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_filter_rows_endswith(tmp_csv_dir):
    # "Alice" and "Charlie" both end with "e"
    rows = read.filter_rows("people.csv", "name", "endswith", "e")
    assert len(rows) == 2


def test_filter_rows_invalid_op(tmp_csv_dir):
    with pytest.raises(ValueError):
        read.filter_rows("people.csv", "city", "LIKE", "New York")


def test_merge_concat(tmp_csv_dir):
    rows = read.merge_files("people.csv", "people.csv", "concat")
    assert len(rows) == 6


def test_merge_inner_join(tmp_csv_dir):
    rows = read.merge_files("people.csv", "scores.csv", "inner", on="name")
    assert len(rows) == 3
    assert "score" in rows[0]
