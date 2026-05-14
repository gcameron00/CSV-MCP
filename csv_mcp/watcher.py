from collections.abc import Callable
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from csv_mcp.config import settings


class _CSVEventHandler(FileSystemEventHandler):
    def __init__(self, extensions: list[str], on_change: Callable[[], None]) -> None:
        self._extensions = extensions
        self._on_change = on_change

    def on_created(self, event) -> None:
        if not event.is_directory and Path(event.src_path).suffix.lower() in self._extensions:
            self._on_change()

    def on_deleted(self, event) -> None:
        if not event.is_directory and Path(event.src_path).suffix.lower() in self._extensions:
            self._on_change()


def start(on_change: Callable[[], None]) -> Observer:
    """Start watching watch_dir in a daemon thread.

    on_change() is called whenever a matching file is created or deleted.
    The caller is responsible for making on_change() thread-safe.
    """
    settings.watch_dir.mkdir(parents=True, exist_ok=True)

    handler = _CSVEventHandler(settings.allowed_extensions, on_change)
    observer = Observer()
    observer.schedule(handler, str(settings.watch_dir), recursive=False)
    observer.daemon = True
    observer.start()
    return observer
