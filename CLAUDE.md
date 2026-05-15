# CSV MCP Server — AI development context

## What this project is

A Python MCP server that exposes CSV files as resources and tools to MCP clients (e.g. Claude Desktop). The core value proposition is letting a model reason about tabular data without loading full files into context — `query` via DuckDB is the workhorse tool.

## Key design constraints

- **Resources are lightweight**: `resources/read` returns schema + row count + 5 sample rows only. Do not change this to return full file contents.
- **DuckDB queries are memory-safe**: `query` runs SQL directly against files on disk. Don't replace this with pandas `read_csv` or similar in-memory approaches.
- **DuckDB DDL cannot use `?` parameters**: `CREATE VIEW ... FROM read_csv_auto(?)` raises a `BinderException`. Paths must be embedded as escaped string literals — see `engine._q()`. This applies to any DDL statement.
- **`fetch_all` bypasses `max_rows`**: Internal write operations (e.g. `delete_rows`) use `engine.fetch_all()` which has no row cap. `max_rows` is a client-response limit only, not a constraint on disk operations.
- **Write tools are destructive**: both `write_file` and `delete_rows` overwrite the target file in place. There is no undo. If you need to preserve the original, use `write_file` with a different filename first.
- **Filesystem watcher is authoritative**: `resources/list` is rebuilt dynamically on every call from the watched directory. Don't cache the file list.
- **`max_rows` is a hard cap**: Query results returned to the client must respect this limit. Don't silently return more rows.

## What's deliberately out of scope (v1)

Do not add these unless explicitly asked:
- Authentication on the HTTP+SSE transport
- Pagination for large query results
- Column add/rename/transform operations
- CSV-to-other-format export

## Tech stack

- Python 3.11+
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
- DuckDB for query execution
- `watchdog` for filesystem events
- `uv` for package management

## Entry point

`python -m csv_mcp` — see `csv_mcp/__main__.py`

## Transport modes

- `stdio` for local Claude Desktop integration
- `HTTP+SSE` for remote/multi-client use
