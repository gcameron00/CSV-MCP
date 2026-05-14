import json

import pytest

from csv_mcp import config, resources


def test_list_resources_finds_csvs(tmp_csv_dir):
    result = resources.list_resources()
    uris = {r["uri"] for r in result}
    assert "csv://people.csv" in uris
    assert "csv://scores.csv" in uris


def test_list_resources_sorted(tmp_csv_dir):
    result = resources.list_resources()
    names = [r["name"] for r in result]
    assert names == sorted(names)


def test_list_resources_missing_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(config.settings, "watch_dir", tmp_path / "does_not_exist")
    assert resources.list_resources() == []


def test_list_resources_uri_scheme(tmp_csv_dir):
    result = resources.list_resources()
    assert all(r["uri"].startswith("csv://") for r in result)


def test_read_resource_shape(tmp_csv_dir):
    payload = json.loads(resources.read_resource("csv://people.csv"))
    assert payload["filename"] == "people.csv"
    assert payload["row_count"] == 3
    assert len(payload["sample"]) <= 5
    col_names = [c["column"] for c in payload["schema"]]
    assert col_names == ["name", "age", "city"]


def test_read_resource_sample_limit(tmp_csv_dir):
    payload = json.loads(resources.read_resource("csv://people.csv"))
    assert len(payload["sample"]) == 3  # file has 3 rows, all fit in the 5-row cap


def test_read_resource_invalid_scheme(tmp_csv_dir):
    with pytest.raises(ValueError, match="Unsupported URI scheme"):
        resources.read_resource("file://people.csv")


def test_read_resource_not_found(tmp_csv_dir):
    with pytest.raises(FileNotFoundError):
        resources.read_resource("csv://nonexistent.csv")
