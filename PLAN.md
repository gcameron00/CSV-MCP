# CSV MCP Server — Build Plan

## Context

Greenfield Python MCP server exposing CSV files as resources and tools. The model should be able to discover, query, filter, and edit CSVs without loading full files into context. Build proceeds bottom-up: config and query engine first, then tools, then MCP wiring, then tests.

**Stack choices:**
- Official MCP Python SDK (`mcp` package)
- DuckDB for all query execution
- watchdog for filesystem events
- stdio transport first; HTTP+SSE deferred to v2
- Config via `config.toml`
- pytest with CSV fixtures for unit tests

---

## Phase 1 — Project scaffold

**Goal:** Runnable package with no logic yet.

Files to create:
- `pyproject.toml` — dependencies: `mcp`, `duckdb`, `watchdog`, `pytest`, `tomllib` (stdlib 3.11+)
- `csv_mcp/__init__.py`
- `csv_mcp/__main__.py` — entry point, calls `server.run()`
- `data/csvs/.gitkeep` — watched directory placeholder
- `config.toml` — default config file with `watch_dir`, `allowed_extensions`, `max_rows`

---

## Phase 2 — Config

**Goal:** Single source of truth for startup config, read once at import time.

Files to create:
- `csv_mcp/config.py`
  - Reads `config.toml` (from repo root or path set by `CSV_MCP_CONFIG` env var)
  - Exposes a `Settings` dataclass: `watch_dir: Path`, `allowed_extensions: list[str]`, `max_rows: int`
  - Falls back to sensible defaults if keys are missing

---

## Phase 3 — DuckDB engine

**Goal:** Thin, reusable wrapper around DuckDB that all tools will call.

Files to create:
- `csv_mcp/engine.py`
  - `run_query(path: Path, sql: str, max_rows: int) -> list[dict]`
    - Opens a DuckDB in-memory connection, registers the CSV as a view named `data`, runs `sql`, returns rows as dicts, caps at `max_rows`
  - `get_schema(path: Path) -> list[dict]`
    - Runs `DESCRIBE SELECT * FROM data` and returns column name + type
  - `get_row_count(path: Path) -> int`

This is the only place DuckDB is touched — tools call engine functions, not DuckDB directly.

---

## Phase 4 — Read tools

**Goal:** All six read tools implemented and individually callable.

Files to create:
- `csv_mcp/tools/read.py`
  - `get_schema(filename)` → calls `engine.get_schema()`
  - `get_sample(filename, n)` → `SELECT * FROM data LIMIT n`
  - `query(filename, sql)` → `engine.run_query()` with user SQL
  - `get_stats(filename)` → one `SELECT` per column for min/max/mean/null_count, assembled into a dict
  - `filter_rows(filename, col, op, value)` → builds a parameterised `WHERE` clause, calls `engine.run_query()`
  - `merge_files(filename_a, filename_b, how)` → `how=concat` uses `UNION ALL`, join modes use `JOIN` with DuckDB multi-file syntax

Each function takes bare filenames and resolves paths via `config.watch_dir` internally.

---

## Phase 5 — Write tools

**Goal:** Persist results back to disk. Two tools, both explicit.

Files to create:
- `csv_mcp/tools/write.py`
  - `write_file(filename, data: list[dict]) -> Path`
    - Writes data to `config.watch_dir / filename` as CSV (using `csv` stdlib)
    - Uses `newline=''` and `encoding='utf-8'` to avoid platform line-ending issues (Windows)
    - Returns the written path
  - `delete_rows(filename, col, op, value) -> Path`
    - Calls `filter_rows` with the inverted operator to get surviving rows
    - Passes result to `write_file` with the same filename
    - Original is only overwritten when the same filename is given — caller's explicit choice

---

## Phase 6 — MCP resources

**Goal:** `resources/list` and `resources/read` handlers wired up.

Files to create:
- `csv_mcp/resources.py`
  - `list_resources() -> list[Resource]`
    - Reads `config.watch_dir` fresh each call, returns one `Resource` per file matching `allowed_extensions`
  - `read_resource(uri: str) -> str`
    - Parses `csv://{filename}` URI, calls `engine.get_schema()`, `engine.get_row_count()`, and `engine.run_query(LIMIT 5)`, returns JSON payload

---

## Phase 7 — Filesystem watcher

**Goal:** Push `resources/list_changed` when files are added or removed.

Files to create:
- `csv_mcp/watcher.py`
  - Uses `watchdog.observers.Observer` + a custom `FileSystemEventHandler`
  - On `created` or `deleted` events for matching extensions, calls the MCP server's `notify_resources_changed()` method
  - Started as a daemon thread in `server.run()`

---

## Phase 8 — MCP server wiring

**Goal:** All tools and resources registered; stdio transport running.

Files to create / edit:
- `csv_mcp/server.py`
  - Creates an `mcp.Server` instance
  - Registers all 8 tools via `@server.tool()` decorators, wrapping the functions from `tools/read.py` and `tools/write.py`
  - Registers `list_resources` and `read_resource` handlers
  - `run()` function: sets stdio to binary mode (Windows compatibility), starts the watcher thread, then calls `mcp.run(server, transport="stdio")`
- `csv_mcp/__main__.py`
  - Calls `csv_mcp.server.run()`

---

## Phase 9 — Tests

**Goal:** Confidence that each tool does what it says.

Files to create:
- `tests/conftest.py` — `tmp_csv_dir` fixture that writes a few small CSV files to a temp directory; patches `config.watch_dir`
- `tests/test_engine.py` — `run_query`, `get_schema`, `get_row_count` against fixture CSVs
- `tests/test_read_tools.py` — one test per read tool
- `tests/test_write_tools.py` — `write_file` round-trips; `delete_rows` removes expected rows
- `tests/test_resources.py` — `list_resources` returns correct entries; `read_resource` returns correct payload shape

---

## Build order rationale

```
Phase 1 (scaffold)
    └── Phase 2 (config)
            └── Phase 3 (engine)   ← all tools depend on this
                    ├── Phase 4 (read tools)
                    │       └── Phase 5 (write tools)  ← delete_rows calls filter_rows
                    └── Phase 6 (resources)            ← read resource uses engine
                            └── Phase 7 (watcher)      ← watcher notifies on resource list change
                                    └── Phase 8 (server wiring)
                                            └── Phase 9 (tests)
```

Each phase is independently testable before the next begins.

---

## Cross-platform notes (Windows 11 + macOS)

- **CSV writing**: always `open(path, 'w', newline='', encoding='utf-8')` — prevents double `\r\n` on Windows
- **stdio binary mode**: set at startup in `__main__.py` before the MCP SDK takes over stdin/stdout
- **Paths**: `pathlib.Path` throughout, no string concatenation or hardcoded separators
- **watchdog**: uses `ReadDirectoryChangesW` on Windows, `FSEvents` on macOS — no code differences needed, but macOS may have a short delay on first event

---

## Verification

After Phase 8:
1. `uv run python -m csv_mcp` should start without errors
2. Add a CSV to `data/csvs/` — Claude Desktop should show it in resources
3. Ask Claude to run `query` with a `SELECT` — should return rows
4. Ask Claude to `delete_rows` — file should update on disk; resource list should refresh

After Phase 9:
```bash
uv run pytest -v
```
All tests pass with no real filesystem side effects (tmp_dir fixture).
