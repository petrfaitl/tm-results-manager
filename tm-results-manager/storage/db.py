# Python
import sqlite3
from datetime import datetime
from typing import Dict
from ..config import DB_FILE


def init_db(db_path: str = DB_FILE):
    """Initialize SQLite database and table with safe migrations."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            region TEXT NOT NULL,
            meet_name TEXT NOT NULL,
            url TEXT NOT NULL,
            processed_timestamp TEXT NOT NULL,
            downloaded BOOLEAN DEFAULT FALSE,
            file_path TEXT,
            uploaded BOOLEAN DEFAULT FALSE,
            processed_by_target BOOLEAN DEFAULT FALSE,
            meet_date TEXT,
            meet_year INTEGER,
            location TEXT,
            course TEXT,
            UNIQUE(region, meet_name)
        )
        """
    )

    def add_col(name, type_):
        try:
            cur.execute(f"ALTER TABLE meets ADD COLUMN {name} {type_}")
        except sqlite3.OperationalError:
            pass

    for col, typ in [
        ("meet_date", "TEXT"),
        ("meet_year", "INTEGER"),
        ("location", "TEXT"),
        ("course", "TEXT"),
    ]:
        add_col(col, typ)

    conn.commit()
    return conn


def load_log(conn) -> Dict[str, Dict[str, dict]]:
    """Read existing rows into a nested dict keyed by region -> meet_name."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT region, meet_name, url, processed_timestamp, downloaded, file_path,
               uploaded, processed_by_target, meet_date, meet_year, location, course
        FROM meets
        """
    )
    log: Dict[str, Dict[str, dict]] = {}
    for row in cur.fetchall():
        (
            region,
            meet_name,
            url,
            processed,
            downloaded,
            file_path,
            uploaded,
            processed_by_target,
            meet_date,
            meet_year,
            location,
            course,
        ) = row
        log.setdefault(region, {})[meet_name] = {
            "url": url,
            "processed_timestamp": processed,
            "downloaded": downloaded,
            "file_path": file_path,
            "uploaded": uploaded,
            "processed_by_target": processed_by_target,
            "meet_date": meet_date,
            "meet_year": meet_year,
            "location": location,
            "course": course,
        }
    return log


def update_log(conn, regions, downloaded_files=None):
    """UPSERT meets while preserving manual flags in existing rows."""
    if downloaded_files is None:
        downloaded_files = {}
    cur = conn.cursor()
    now = datetime.now().isoformat()

    for region, meets in regions.items():
        for meet in meets:
            meet_name = meet["meet_name"]
            file_path = downloaded_files.get(meet_name)
            downloaded = file_path is not None

            cur.execute(
                """
                INSERT INTO meets 
                (region, meet_name, url, processed_timestamp, downloaded, file_path, uploaded, processed_by_target,
                 meet_date, meet_year, location, course)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(region, meet_name) DO UPDATE SET
                  url=excluded.url,
                  processed_timestamp=excluded.processed_timestamp,
                  downloaded=excluded.downloaded,
                  file_path=COALESCE(excluded.file_path, meets.file_path),
                  uploaded=meets.uploaded,
                  processed_by_target=meets.processed_by_target,
                  meet_date=COALESCE(excluded.meet_date, meets.meet_date),
                  meet_year=COALESCE(excluded.meet_year, meets.meet_year),
                  location=COALESCE(excluded.location, meets.location),
                  course=COALESCE(excluded.course, meets.course)
                """,
                (
                    region,
                    meet_name,
                    meet["link"],
                    now,
                    downloaded,
                    file_path,
                    False,  # for initial insert only
                    False,  # for initial insert only
                    meet.get("meet_date"),
                    meet.get("meet_year"),
                    meet.get("location"),
                    meet.get("course"),
                ),
            )
    conn.commit()
