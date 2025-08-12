from __future__ import annotations

import os
import time
from pathlib import Path

from typer.testing import CliRunner

from sweeper.cli import app

runner = CliRunner()


def test_list_and_sweep(tmp_path: Path) -> None:
    # Create files
    (tmp_path / "keep.txt").write_text("new")
    newish = tmp_path / "newish.log"
    newish.write_text("newish")
    old = tmp_path / "old.log"
    old.write_text("old")
    older = tmp_path / "older.log"
    older.write_text("older")

    archive_path = Path("/tmp/sweeper_archive")

    # Make old files older than 1 day
    day_seconds = 24 * 3600
    now = time.time()
    os.utime(newish, (now - 3600, now - 3600))  # 1 hour old
    os.utime(old, (now - 2 * day_seconds, now - 2 * day_seconds))
    os.utime(older, (now - 3 * day_seconds, now - 3 * day_seconds))

    # List
    res = runner.invoke(
        app,
        ["list", str(tmp_path), "--older-than", "30m", "--pattern", "*.log"],
        env={"COLUMNS": "200"},
    )
    assert res.exit_code == 0
    assert "newish.log" in res.stdout and "old.log" in res.stdout and "older.log" in res.stdout
    assert "keep.txt" not in res.stdout

    # Dry run sweep
    res = runner.invoke(
        app, ["sweep", str(tmp_path), "--older-than", "1d", "--pattern", "*.log", "--dry-run"]
    )
    assert res.exit_code == 0
    assert "Would delete" in res.stdout

    # Real sweep with archiving
    res = runner.invoke(
        app,
        [
            "sweep",
            str(tmp_path),
            "--older-than",
            "1h",
            "--pattern",
            "*.log",
            "--no-dry-run",
            "--archive-to",
            "/tmp/sweeper_archive",
        ],
    )
    assert res.exit_code == 0
    assert "Archived: /tmp/sweeper_archive/old.log" in res.stdout
    assert (archive_path / "old.log").exists()
    assert not (tmp_path / "old.log").exists()

    # Recreate files
    newish.write_text("newish")
    old.write_text("old")
    older.write_text("older")
    os.utime(newish, (now - 3600, now - 3600))  # 1 hour old
    os.utime(old, (now - 2 * day_seconds, now - 2 * day_seconds))
    os.utime(older, (now - 3 * day_seconds, now - 3 * day_seconds))

    # Real sweep without archiving
    res = runner.invoke(
        app, ["sweep", str(tmp_path), "--older-than", "1d", "--pattern", "*.log", "--no-dry-run"]
    )
    assert res.exit_code == 0
    assert not (tmp_path / "old.log").exists()
    assert not (tmp_path / "older.log").exists()
    assert (tmp_path / "keep.txt").exists()
