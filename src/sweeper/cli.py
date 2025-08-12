from __future__ import annotations

import builtins
import fnmatch
import os
import shutil
import typing
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated

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


def get_seconds(older_than: str) -> int:
    """Convert a string like '7d' or '12h' or '30m' to seconds."""
    unit = older_than[-1]
    try:
        value = int(older_than[:-1])
    except ValueError:
        raise typer.BadParameter(f"Invalid older-than value: {older_than}") from None
    if unit == "d":
        return value * 86400
    elif unit == "h":
        return value * 3600
    elif unit == "m":
        return value * 60
    else:
        raise typer.BadParameter(f"Invalid time unit in older-than: {unit}. Use d, h, or m.")


def _iter_targets(root: Path, pattern: str, older_than: str) -> Iterable[Target]:
    # Create function to take the string and return seconds
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=get_seconds(older_than))
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if not fnmatch.fnmatch(name, pattern):
                continue
            p = Path(dirpath) / name
            try:
                stat = p.stat()
            except OSError:
                raise OSError(f"Error accessing {p}") from None
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


def _copy(t: Target, archive_path: Path, dry_run: bool) -> tuple[Target, Exception | None]:
    try:
        if not dry_run:
            archive_target = archive_path / t.path.name
            archive_path.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(t.path, archive_target)
        return (t, None)
    except Exception as e:  # noqa: BLE001
        return (t, e)


def list_cmd(
    root: Annotated[
        Path,
        typer.Argument(..., exists=True, file_okay=False, readable=True, help="Root directory"),
    ],
    older_than: Annotated[
        str, typer.Option("--older-than", help="Age threshold e.g. 7d|12h|30m")
    ] = "30d",
    pattern: Annotated[
        str, typer.Option("--pattern", help="Glob pattern (e.g., '*.log' or '*.gz')]")
    ] = "*.log",
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
    root: Annotated[
        Path,
        typer.Argument(..., exists=True, file_okay=False, readable=True, help="Root directory"),
    ],
    older_than: Annotated[
        str, typer.Option("--older-than", help="Age threshold e.g. 7d|12h|30m")
    ] = "30d",
    pattern: Annotated[
        str, typer.Option("--pattern", help="Glob pattern (e.g., '*.log' or '*.gz')]")
    ] = "*.log",
    concurrency: Annotated[
        int, typer.Option("--concurrency", min=1, help="Delete worker threads")
    ] = 8,
    dry_run: Annotated[bool, typer.Option(help="Dry run / no changes")] = True,
    archive_path: Annotated[
        Path | None, typer.Option("--archive-to", help="Path to archive directory")
    ] = None,
) -> None:
    """Archive and Delete matching files older than N days (dry-run by default)."""
    candidates = list(_iter_targets(root, pattern, older_than))
    if not candidates:
        console.print("No files to archive and delete.")
        raise typer.Exit(code=0)

    console.print(f"Found {len(candidates)} files. Dry run: {dry_run}. Concurrency: {concurrency}.")
    errors: builtins.list[Exception] = []

    # Achiving logic
    if archive_path:
        console.print("Starting archiving...")
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = [ex.submit(_copy, t, archive_path, dry_run) for t in candidates]
            for fut in as_completed(futures):
                t, err = fut.result()
                if err:
                    errors.append(err)
                    console.print(f"[red]ERROR[/red] {t.path}: {err}")
                else:
                    action = "Would archive" if dry_run else "Archived"
                    console.print(f"{action}: {archive_path / t.path.name}")

    # Deletion logic
    console.print("Starting deletion...")
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
    root: Annotated[
        Path,
        typer.Argument(..., exists=True, file_okay=False, readable=True, help="Root directory"),
    ],
    older_than: Annotated[
        str, typer.Option("--older-than", help="Age threshold e.g. 7d|12h|30m")
    ] = "30d",
    pattern: Annotated[
        str, typer.Option("--pattern", help="Glob pattern (e.g., '*.log' or '*.gz')]")
    ] = "*.log",
    concurrency: Annotated[
        int, typer.Option("--concurrency", min=1, help="Delete worker threads")
    ] = 8,
    dry_run: Annotated[bool, typer.Option(help="Dry run / no changes")] = True,
    archive_path: Annotated[
        Path | None, typer.Option("--archive-to", help="Path to archive directory")
    ] = None,
) -> None:
    return sweep_cmd(root, older_than, pattern, concurrency, dry_run, archive_path)


@typing.no_type_check
@app.command("list")
def list_cli(
    root: Annotated[
        Path,
        typer.Argument(..., exists=True, file_okay=False, readable=True, help="Root directory"),
    ],
    older_than: Annotated[
        str, typer.Option("--older-than", help="Age threshold e.g. 7d|12h|30m")
    ] = "30d",
    pattern: Annotated[
        str, typer.Option("--pattern", help="Glob pattern (e.g., '*.log' or '*.gz')]")
    ] = "*.log",
) -> None:
    return list_cmd(root, older_than, pattern)


if __name__ == "__main__":
    app()
