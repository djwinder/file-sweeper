from __future__ import annotations

import builtins
import fnmatch
import os
import typing
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app: typer.Typer = typer.Typer(help="Sweep old files from a directory (local now, S3 later).")
console = Console()


@dataclass(frozen=True)
class Target:
    path: Path
    size: int
    mtime: datetime


def _iter_targets(root: Path, pattern: str, older_than_days: int) -> Iterable[Target]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if not fnmatch.fnmatch(name, pattern):
                continue
            p = Path(dirpath) / name
            try:
                stat = p.stat()
            except OSError:
                continue
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            if mtime <= cutoff:
                yield Target(p, stat.st_size, mtime)


def _delete(t: Target, dry_run: bool) -> tuple[Target, Exception | None]:
    try:
        if not dry_run:
            t.path.unlink(missing_ok=True)
        return (t, None)
    except Exception as e:  # noqa: BLE001
        return (t, e)


def list_cmd(
    root: Path = typer.Argument(  # noqa: B008
        ..., exists=True, file_okay=False, readable=True, help="Root directory"
    ),
    older_than: int = typer.Option(30, min=0, help="Age threshold in days"),
    pattern: str = typer.Option("*", help="Glob pattern (e.g., '*.log' or '*.gz')"),
) -> None:
    """List candidate files."""
    items = sorted(_iter_targets(root, pattern, older_than), key=lambda t: t.mtime)
    table = Table(title="Candidates", show_lines=False)
    table.add_column("Path")
    table.add_column("Size (bytes)", justify="right")
    table.add_column("Modified (UTC)")
    for t in items:
        table.add_row(str(t.path), str(t.size), t.mtime.isoformat())
    console.print(table)
    console.print(f"[bold]{len(items)}[/bold] files matched.")


def sweep_cmd(
    root: Path = typer.Argument(..., exists=True, file_okay=False, readable=True),  # noqa: B008
    older_than: int = typer.Option(30, min=0),
    pattern: str = typer.Option("*"),
    concurrency: int = typer.Option(8, min=1, help="Delete worker threads"),
    dry_run: bool = typer.Option(True, help="Show actions without deleting"),
) -> None:
    """Delete matching files older than N days (dry-run by default)."""
    candidates = list(_iter_targets(root, pattern, older_than))
    if not candidates:
        console.print("No files to delete.")
        raise typer.Exit(code=0)

    console.print(f"Found {len(candidates)} files. Dry run: {dry_run}. Concurrency: {concurrency}.")
    errors: builtins.list[Exception] = []

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(_delete, t, dry_run) for t in candidates]
        for fut in as_completed(futures):
            t, err = fut.result()
            if err:
                errors.append(err)
                console.print(f"[red]ERROR[/red] {t.path}: {err}")
            else:
                action = "Would delete" if dry_run else "Deleted"
                console.print(f"{action}: {t.path}")

    if errors:
        console.print(f"[red]{len(errors)}[/red] errors occurred.", style="red")
        raise typer.Exit(code=1)

    console.print("Done.")


@typing.no_type_check
@app.command("sweep")
def sweep_cli(
    root: Path = typer.Argument(..., exists=True, file_okay=False, readable=True),  # noqa: B008
    older_than: int = typer.Option(30, min=0),
    pattern: str = typer.Option("*"),
    concurrency: int = typer.Option(8, min=1, help="Delete worker threads"),
    dry_run: bool = typer.Option(True, help="Show actions without deleting"),
) -> None:
    return sweep_cmd(root, older_than, pattern, concurrency, dry_run)


@typing.no_type_check
@app.command("list")
def list_cli(
    root: Path = typer.Argument(  # noqa: B008
        ..., exists=True, file_okay=False, readable=True, help="Root directory"
    ),
    older_than: int = typer.Option(30, min=0, help="Age threshold in days"),
    pattern: str = typer.Option("*", help="Glob pattern (e.g., '*.log' or '*.gz')"),
) -> None:
    return list_cmd(root, older_than, pattern)


if __name__ == "__main__":
    app()
