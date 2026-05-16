# CSV MCP Server

A local [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for working with CSV files. Exposes CSV files as resources and provides tools for querying, filtering, and editing them — letting an MCP client (e.g. Claude Desktop) reason about tabular data without ever loading full files into the context window.

---

## Architecture

```
MCP client
    │
    ├── resources/list         ← dynamic, rebuilt fresh each call
    ├── resources/read         ← csv://{filename} → schema + row count + 5 sample rows
    │       └── [Filesystem watcher] monitors watch_dir, pushes resources/list_changed
    │
    ├── Read tools
    │   ├── get_schema         filename → columns + types
    │   ├── get_sample         filename, n → first N rows
    │   ├── query              SQL via DuckDB (no full load into memory)
    │   ├── get_stats          min · max · mean · null counts per column
    │   ├── filter_rows        col, op, value → matching rows
    │   └── merge_files        join or concatenate two CSVs
    │
    └── Write tools
        ├── write_file         save a result back to disk
        └── delete_rows        filter out matching rows (writes via write_file)
```

**Transport:** `stdio` (local) · `HTTP+SSE` (remote)

---

## Resources

### `resources/list`
Returns one resource entry per CSV in the watched directory. The list is rebuilt fresh on every call — no stale cache. When a file is added or removed, the filesystem watcher pushes a `resources/list_changed` notification to the client.

### `resources/read` — `csv://{filename}`
Returns a lightweight payload sufficient for the model to reason about a file without blowing the context window:

| Field | Description |
|---|---|
| `schema` | List of `{column, type}` objects |
| `row_count` | Total number of data rows |
| `sample` | First 5 rows |

---

## Tools

### Read tools

| Tool | Inputs | Description |
|---|---|---|
| `get_schema` | `filename` | Returns column names and inferred types. Redundant with the resource payload but useful for re-checking types mid-task. |
| `get_sample` | `filename`, `n` | Returns the first N rows. |
| `query` | `filename`, `sql` | Runs a SQL query via DuckDB directly against the CSV file — no full file load into memory. The most powerful read tool. |
| `get_stats` | `filename` | Returns min, max, mean, and null count for each column. |
| `filter_rows` | `filename`, `col`, `op`, `value` | Convenience wrapper for simple equality/comparison filters. Use `query` for anything more complex. |
| `merge_files` | `filename_a`, `filename_b`, `how`, `on` (optional) | Join or concatenate two CSV files. `how` supports `concat`, `inner`, `left`, `right`. `on` is the join key column and is required for anything other than `concat`. |

### Write tools

Write tools have side effects and are visually separated in the architecture. Both are explicit — no silent overwrites.

| Tool | Inputs | Description |
|---|---|---|
| `write_file` | `filename`, `data` | Overwrites `filename` with the provided rows. If the filename is new, the watcher pushes a `list_changed` notification. **Destructive — replaces the entire file.** |
| `append_rows` | `filename`, `data` | Appends rows to an existing file without touching existing content. Creates the file if it doesn't exist. |
| `delete_rows` | `filename`, `col`, `op`, `value` | Removes matching rows and writes the survivors back to the same file. **Destructive — overwrites the original in place.** |

---

## Server configuration

Configured via `config.toml` in the repo root. Set the `CSV_MCP_CONFIG` environment variable to point at an alternate path.

```toml
[server]
watch_dir = "data/csvs"
allowed_extensions = [".csv"]
max_rows = 1000
```

| Key | Description | Default |
|---|---|---|
| `watch_dir` | Directory to monitor for CSV files | `data/csvs` |
| `allowed_extensions` | File extensions treated as CSVs | `[".csv"]` |
| `max_rows` | Hard row cap for query results returned to the client | `1000` |

---

## Transport

| Mode | Status | Use case |
|---|---|---|
| `stdio` | Implemented | Local use — launch the server as a subprocess from an MCP client config (e.g. Claude Desktop) |
| `HTTP+SSE` | v2 | Remote or multi-client use — server runs independently and clients connect over HTTP |

---

## Getting started

### Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- [DuckDB](https://duckdb.org/) (installed as a dependency)

### Install

```bash
uv sync --dev
```

### Run (stdio)

```bash
uv run python -m csv_mcp
```

### Configure in Claude Desktop

Add this to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "csv": {
      "command": "uv",
      "args": ["run", "python", "-m", "csv_mcp"],
      "cwd": "/path/to/CSV-MCP"
    }
  }
}
```

Place your CSV files in `./data/csvs/` and they will appear automatically as resources.

---

## What's not in v1

These are intentionally out of scope for the initial release:

- **Authentication** — the server is local-only by default; HTTP+SSE transport is unauthenticated
- **Pagination** — query results are capped at `max_rows`; large result sets are truncated
- **Column operations** — add, rename, or transform columns
- **Format export** — CSV-to-JSON, CSV-to-Parquet, etc.

These are v2 territory once the core read/write loop is solid.

---

## Project structure

```
CSV-MCP/
├── csv_mcp/
│   ├── __main__.py       # entry point
│   ├── server.py         # MCP server wiring — tools, resources, notification loop
│   ├── resources.py      # list_resources, read_resource business logic
│   ├── watcher.py        # filesystem watcher (watchdog), fires on_change callback
│   ├── engine.py         # DuckDB wrapper — only file that touches DuckDB
│   ├── tools/
│   │   ├── read.py       # get_schema, get_sample, query, get_stats, filter_rows, merge_files
│   │   └── write.py      # write_file, delete_rows
│   └── config.py         # loads config.toml → Settings dataclass
├── data/
│   └── csvs/             # default watched directory
├── tests/
├── config.toml           # server configuration
└── pyproject.toml
```
