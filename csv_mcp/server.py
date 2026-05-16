import asyncio
import json
from typing import Any

import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.session import ServerSession
from mcp.server.stdio import stdio_server
from pydantic import AnyUrl

from csv_mcp import resources as res
from csv_mcp import watcher
from csv_mcp.tools import read
from csv_mcp.tools import write as write_tools

_server = Server("csv-mcp")

# Captured from the first request handler call; used by the notification sender task.
_active_session: ServerSession | None = None


# --- Resource handlers ---

@_server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    global _active_session
    _active_session = _server.request_context.session
    return [
        types.Resource(
            uri=AnyUrl(r["uri"]),
            name=r["name"],
            description=r["description"],
            mimeType=r["mime_type"],
        )
        for r in res.list_resources()
    ]


@_server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    global _active_session
    _active_session = _server.request_context.session
    return res.read_resource(str(uri))


# --- Tool registry ---

@_server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="get_schema",
            description="Get column names and types for a CSV file.",
            inputSchema={
                "type": "object",
                "properties": {"filename": {"type": "string"}},
                "required": ["filename"],
            },
        ),
        types.Tool(
            name="get_sample",
            description="Get the first N rows of a CSV file.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "n": {"type": "integer", "default": 10},
                },
                "required": ["filename"],
            },
        ),
        types.Tool(
            name="query",
            description=(
                "Run a SQL query against a CSV file using DuckDB. "
                "The file is registered as a view named 'data'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "sql": {"type": "string"},
                },
                "required": ["filename", "sql"],
            },
        ),
        types.Tool(
            name="get_stats",
            description="Get min, max, mean, and null count for every column in a CSV file.",
            inputSchema={
                "type": "object",
                "properties": {"filename": {"type": "string"}},
                "required": ["filename"],
            },
        ),
        types.Tool(
            name="filter_rows",
            description=(
                "Return rows where a column matches a condition. "
                "op must be one of: =, !=, <, >, <=, >=, contains, startswith, endswith."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "col": {"type": "string"},
                    "op": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["filename", "col", "op", "value"],
            },
        ),
        types.Tool(
            name="merge_files",
            description=(
                "Join or concatenate two CSV files. "
                "how must be one of: concat, inner, left, right. "
                "'on' (the join key column name) is required for inner/left/right."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "filename_a": {"type": "string"},
                    "filename_b": {"type": "string"},
                    "how": {"type": "string"},
                    "on": {"type": "string"},
                },
                "required": ["filename_a", "filename_b", "how"],
            },
        ),
        types.Tool(
            name="write_file",
            description="Save a list of row objects to a CSV file in the watch directory.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "data": {"type": "array", "items": {"type": "object"}},
                },
                "required": ["filename", "data"],
            },
        ),
        types.Tool(
            name="delete_rows",
            description=(
                "Remove rows matching a condition and write the result back to the same file. "
                "op must be one of: =, !=, <, >, <=, >=, contains, startswith, endswith."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "col": {"type": "string"},
                    "op": {"type": "string"},
                    "value": {"type": "string"},
                },
                "required": ["filename", "col", "op", "value"],
            },
        ),
        types.Tool(
            name="append_rows",
            description="Append rows to an existing CSV file without overwriting it. Creates the file if it does not exist.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {"type": "string"},
                    "data": {"type": "array", "items": {"type": "object"}},
                },
                "required": ["filename", "data"],
            },
        ),
    ]


# --- Tool dispatch ---

@_server.call_tool()
async def handle_call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    match name:
        case "get_schema":
            result = read.get_schema(arguments["filename"])
        case "get_sample":
            result = read.get_sample(arguments["filename"], arguments.get("n", 10))
        case "query":
            result = read.query(arguments["filename"], arguments["sql"])
        case "get_stats":
            result = read.get_stats(arguments["filename"])
        case "filter_rows":
            result = read.filter_rows(
                arguments["filename"],
                arguments["col"],
                arguments["op"],
                arguments["value"],
            )
        case "merge_files":
            result = read.merge_files(
                arguments["filename_a"],
                arguments["filename_b"],
                arguments["how"],
                arguments.get("on"),
            )
        case "append_rows":
            path = write_tools.append_rows(arguments["filename"], arguments["data"])
            result = {"written": str(path)}
        case "write_file":
            path = write_tools.write_file(arguments["filename"], arguments["data"])
            result = {"written": str(path)}
        case "delete_rows":
            path = write_tools.delete_rows(
                arguments["filename"],
                arguments["col"],
                arguments["op"],
                arguments["value"],
            )
            result = {"written": str(path)}
        case _:
            raise ValueError(f"Unknown tool: {name!r}")

    return [types.TextContent(type="text", text=json.dumps(result, default=str))]


# --- Notification sender ---

async def _notification_sender(queue: asyncio.Queue) -> None:
    """Drain the change queue and push resources/list_changed to the active session."""
    while True:
        await queue.get()
        if _active_session is not None:
            try:
                await _active_session.send_resource_list_changed()
            except Exception:
                pass  # client disconnected or session closed


# --- Entry point ---

async def _run() -> None:
    notification_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def on_change() -> None:
        loop.call_soon_threadsafe(notification_queue.put_nowait, True)

    observer = watcher.start(on_change)
    sender_task = asyncio.create_task(_notification_sender(notification_queue))

    try:
        async with stdio_server() as (read_stream, write_stream):
            await _server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="csv-mcp",
                    server_version="0.1.0",
                    capabilities=_server.get_capabilities(
                        notification_options=NotificationOptions(resources_changed=True),
                        experimental_capabilities={},
                    ),
                ),
            )
    finally:
        sender_task.cancel()
        observer.stop()


def run() -> None:
    asyncio.run(_run())
