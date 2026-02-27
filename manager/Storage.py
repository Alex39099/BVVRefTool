#  Copyright (c) 2026. Alexander Schmid
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ScraperRunningStatus(StrEnum):
    RUNNING = 'RUNNING'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


class SnapshotSource(StrEnum):
    BVV_LICENSES_EXCEL = "BVV_Licenses_Excel"
    BVV_REGISTRATIONS = "BVV_Registrations"
    BVV_COURSES = "BVV_Courses"


@dataclass(frozen=True)
class Snapshot:
    run_id: int
    source: str
    raw_data: bytes
    id: int
    collected_at: datetime


class SnapshotRepository:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")  # enforce foreign key constraints, i.e. run_id must be present
        return conn

    def _init_db(self):
        conn = self._get_connection()
        c = conn.cursor()

        # Scraper runs
        c.execute("""
        CREATE TABLE IF NOT EXISTS scraper_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            collected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL
        )
        """)

        # Raw snapshots
        c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            raw_data BLOB NOT NULL,
            FOREIGN KEY(run_id) REFERENCES scraper_runs(run_id) ON DELETE CASCADE,
            UNIQUE (run_id, source)
        )
        """)
        conn.commit()
        conn.close()

    def create_run(self, status: ScraperRunningStatus = ScraperRunningStatus.RUNNING) -> int:
        conn = self._get_connection()
        c = conn.cursor()
        run_id = c.execute(
            "INSERT INTO scraper_runs (status) VALUES (?)",
            (status.value,)
        ).lastrowid
        conn.commit()
        conn.close()
        return run_id

    def delete_run(self, run_id: int) -> None:
        """
        Deletes a scraper run and all associated snapshots.
        :param run_id: scraper run id
        """
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "DELETE FROM scraper_runs WHERE run_id=?",
            (run_id,)
        )
        # Snapshots are removed automatically via ON DELETE CASCADE
        conn.commit()
        conn.close()

    def delete_runs_older_than(self, cutoff: datetime) -> int:
        """
        Deletes all runs and the associated snapshots older than cutoff.
        :param cutoff: datetime object
        :return: number of runs deleted
        """
        conn = self._get_connection()
        c = conn.cursor()

        cutoff_str = cutoff.isoformat(sep=" ")
        c.execute(
            "DELETE FROM scraper_runs WHERE collected_at < ?",
            (cutoff_str,)
        )
        # Snapshots are removed automatically via ON DELETE CASCADE
        deleted = c.rowcount
        conn.commit()
        conn.close()
        return deleted

    def update_run_status(self, run_id: int, status: ScraperRunningStatus):
        conn = self._get_connection()
        c = conn.cursor()
        c.execute(
            "UPDATE scraper_runs SET status=? WHERE run_id=?",
            (status.value, run_id)
        )
        conn.commit()
        conn.close()

    # --- Snapshot operations ---
    def save_snapshot(self, run_id: int, source: SnapshotSource, raw_data: bytes) -> int:
        """
        Saves a snapshot. Note: There cannot be more than one snapshot with the same run_id and source.
        :param run_id: id of the scraper run, use self.create_run to create a new run
        :param source: source of the scraping
        :param raw_data: raw_data of the scrape operation, e.g. response content
        :return: snapshot_id
        """
        conn = self._get_connection()
        c = conn.cursor()
        try:
            snapshot_id = c.execute(
                "INSERT INTO snapshots (run_id, source, raw_data) VALUES (?, ?, ?)",
                (run_id, source, raw_data)
            ).lastrowid
            conn.commit()
            return snapshot_id
        except sqlite3.IntegrityError as e:
            raise ValueError(f"Snapshot for run_id={run_id} and source='{source}' already exists") from e
        finally:
            conn.close()

    def get_recent_snapshots(self, source: SnapshotSource, limit: int = 2) -> list[Snapshot]:
        """
        Get recent snapshots, newest first
        :param source: SnapshotSource
        :param limit: max number of returns
        :return: all stored snapshots up to limit, newest snapshots first
        """
        if limit <= 0:
            raise ValueError("limit must be positive")

        conn = self._get_connection()
        c = conn.cursor()
        rows = c.execute(
            f"""
            SELECT s.run_id, s.raw_data, s.snapshot_id, r.collected_at
            FROM snapshots s
            JOIN scraper_runs r ON s.run_id = r.run_id
            WHERE s.source=?
            ORDER BY r.collected_at DESC
            LIMIT {limit}
            """,
            (source,)
        ).fetchall()
        conn.close()

        snapshots = []
        for row in rows:
            s = Snapshot(
                run_id=row[0],
                source=source,
                raw_data=row[1],
                id=row[2],
                collected_at=datetime.fromisoformat(row[3])
            )
            snapshots.append(s)

        return snapshots
