from __future__ import annotations

import os
import time
from pathlib import Path

from typer.testing import CliRunner

from sweeper.cli import app

runner = CliRunner()


def test_list_and_sweep(tmp_path: Path) -> None:
    # Create files: two old, one new
    (tmp_path / "keep.txt").write_text("new")
    old = tmp_path / "old.log"
    old.write_text("old")
    older = tmp_path / "older.log"
    older.write_text("older")

    # Make old files older than 1 day
    day_seconds = 24 * 3600
    now = time.time()
    os.utime(old, (now - 2 * day_seconds, now - 2 * day_seconds))
    os.utime(older, (now - 3 * day_seconds, now - 3 * day_seconds))

    # List
    res = runner.invoke(
        app,
        ["list", str(tmp_path), "--older-than", "1", "--pattern", "*.log"],
        env={"COLUMNS": "200"},
    )
    assert res.exit_code == 0
    assert "old.log" in res.stdout and "older.log" in res.stdout
    assert "keep.txt" not in res.stdout

    # Dry run sweep
    res = runner.invoke(
        app, ["sweep", str(tmp_path), "--older-than", "1", "--pattern", "*.log", "--dry-run"]
    )
    assert res.exit_code == 0
    assert "Would delete" in res.stdout

    # Real sweep
    res = runner.invoke(
        app, ["sweep", str(tmp_path), "--older-than", "1", "--pattern", "*.log", "--no-dry-run"]
    )
    assert res.exit_code == 0
    assert not (tmp_path / "old.log").exists()
    assert not (tmp_path / "older.log").exists()
    assert (tmp_path / "keep.txt").exists()
