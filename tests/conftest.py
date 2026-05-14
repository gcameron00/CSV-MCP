import csv
import pytest

from csv_mcp import config


@pytest.fixture
def tmp_csv_dir(tmp_path, monkeypatch):
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()

    with open(csv_dir / "people.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "age", "city"])
        writer.writeheader()
        writer.writerows([
            {"name": "Alice", "age": "30", "city": "New York"},
            {"name": "Bob", "age": "25", "city": "London"},
            {"name": "Charlie", "age": "35", "city": "New York"},
        ])

    with open(csv_dir / "scores.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "score"])
        writer.writeheader()
        writer.writerows([
            {"name": "Alice", "score": "88"},
            {"name": "Bob", "score": "72"},
            {"name": "Charlie", "score": "95"},
        ])

    monkeypatch.setattr(config.settings, "watch_dir", csv_dir)
    return csv_dir
