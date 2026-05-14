import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent


@dataclass
class Settings:
    watch_dir: Path
    allowed_extensions: list[str]
    max_rows: int


def _load() -> Settings:
    config_path = Path(os.environ.get("CSV_MCP_CONFIG", _REPO_ROOT / "config.toml"))

    raw: dict = {}
    if config_path.exists():
        with open(config_path, "rb") as f:
            raw = tomllib.load(f).get("server", {})

    watch_dir = Path(raw.get("watch_dir", _REPO_ROOT / "data" / "csvs"))
    if not watch_dir.is_absolute():
        watch_dir = _REPO_ROOT / watch_dir

    return Settings(
        watch_dir=watch_dir,
        allowed_extensions=raw.get("allowed_extensions", [".csv"]),
        max_rows=int(raw.get("max_rows", 1000)),
    )


settings = _load()
