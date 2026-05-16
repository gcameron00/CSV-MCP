import csv

import pytest

from csv_mcp.tools import write as write_tools


def test_write_file_round_trip(tmp_csv_dir):
    data = [{"x": "a", "y": "1"}, {"x": "b", "y": "2"}]
    path = write_tools.write_file("output.csv", data)
    assert path.exists()
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows == data


def test_write_file_empty(tmp_csv_dir):
    path = write_tools.write_file("empty.csv", [])
    assert path.exists()
    assert path.read_text(encoding="utf-8") == ""


def test_write_file_no_double_crlf(tmp_csv_dir):
    data = [{"col": "val"}]
    path = write_tools.write_file("crlf_check.csv", data)
    raw = path.read_bytes()
    assert b"\r\r\n" not in raw


def test_delete_rows_eq(tmp_csv_dir):
    path = write_tools.delete_rows("people.csv", "city", "=", "New York")
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"


def test_delete_rows_contains(tmp_csv_dir):
    # Removes Alice and Charlie (both contain "li"), leaving Bob
    path = write_tools.delete_rows("people.csv", "name", "contains", "li")
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"


def test_delete_rows_preserves_all_columns(tmp_csv_dir):
    path = write_tools.delete_rows("people.csv", "city", "=", "London")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    assert set(fieldnames) == {"name", "age", "city"}
    assert len(rows) == 2


def test_delete_rows_invalid_op(tmp_csv_dir):
    with pytest.raises(ValueError):
        write_tools.delete_rows("people.csv", "city", "INVALID", "London")


def test_append_rows_to_existing(tmp_csv_dir):
    new_rows = [{"name": "Diana", "age": "28", "city": "Paris"}]
    write_tools.append_rows("people.csv", new_rows)
    with open(tmp_csv_dir / "people.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 4
    assert rows[-1]["name"] == "Diana"


def test_append_rows_creates_file(tmp_csv_dir):
    data = [{"x": "1"}, {"x": "2"}]
    path = write_tools.append_rows("new.csv", data)
    assert path.exists()
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2


def test_append_rows_no_duplicate_header(tmp_csv_dir):
    row = [{"name": "Diana", "age": "28", "city": "Paris"}]
    write_tools.append_rows("people.csv", row)
    write_tools.append_rows("people.csv", row)
    raw = (tmp_csv_dir / "people.csv").read_text(encoding="utf-8")
    assert raw.count("name,age,city") == 1


def test_append_rows_empty_noop(tmp_csv_dir):
    original = (tmp_csv_dir / "people.csv").read_bytes()
    write_tools.append_rows("people.csv", [])
    assert (tmp_csv_dir / "people.csv").read_bytes() == original
