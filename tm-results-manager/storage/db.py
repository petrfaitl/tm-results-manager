# Python
import sqlite3
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from ..config import DB_FILE

BUSY_TIMEOUT_MS = 5000


def init_db(db_path: str = DB_FILE):
    conn = sqlite3.connect(db_path)
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    cur = conn.cursor()
    # Existing meets table created elsewhere in your project — ensure new columns exist
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
            meet_date_start TEXT,
            meet_date_end TEXT,
            parsed BOOLEAN DEFAULT FALSE,
            UNIQUE(region, meet_name)
        )
        """
    )
    # Migrations for added columns (ignore if exist)
    for col_def in [
        ("meet_date_start", "TEXT"),
        ("meet_date_end", "TEXT"),
        ("parsed", "BOOLEAN"),
    ]:
        try:
            cur.execute(f"ALTER TABLE meets ADD COLUMN {col_def[0]} {col_def[1]}")
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_code TEXT NOT NULL,   -- HY3/CL2 team code
            team_name TEXT NOT NULL,
            team_type TEXT NOT NULL,
            region_code TEXT,
            region TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            postal_code TEXT,
            UNIQUE(team_code, team_name)
        )
        """
    )

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_teams_identity
        ON teams (team_code, team_name)
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS swimmers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,            -- FK to teams.id (nullable)
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            gender TEXT NOT NULL,
            birth_date TEXT,            -- DDMMYYYY or NULL
            mm_number TEXT,             -- swimmer identifier in HY3
            FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE SET NULL
        )
        """
    )

    # Add uniqueness across your chosen identity (no meet_id included)
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_swimmers_identity
        ON swimmers (
            first_name,
            last_name,
            gender,
            birth_date,
            mm_number,
            team_id
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meet_team_swimmer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meet_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            swimmer_id INTEGER NOT NULL,
            UNIQUE(meet_id, team_id, swimmer_id),
            FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE,
            FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE CASCADE,
            FOREIGN KEY(swimmer_id) REFERENCES swimmers(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meet_swimmer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meet_id INTEGER NOT NULL,
            swimmer_id INTEGER NOT NULL,
            UNIQUE(meet_id, swimmer_id),
            FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE,
            FOREIGN KEY(swimmer_id) REFERENCES swimmers(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS meet_team (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        meet_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL,
        UNIQUE(meet_id, team_id),
        FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE,
        FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE CASCADE
    )
    """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS parse_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meet_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',
            message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS error_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            file_path TEXT,
            meet_id INTEGER,
            region TEXT,
            error_type TEXT NOT NULL,
            message TEXT,
            context_json TEXT,
            FOREIGN KEY(meet_id) REFERENCES meets(id) ON DELETE SET NULL
        )
        """
    )

    conn.commit()
    return conn


# Python
def load_log(conn):
    """
    Load existing meets into a nested dict: region -> meet_name -> metadata.
    Matches the structure expected by exporter and downloader.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT region, meet_name, url, processed_timestamp, downloaded, file_path,
               uploaded, processed_by_target, meet_date, meet_year, location, course,
               meet_date_start, meet_date_end, parsed
        FROM meets
        """
    )
    rows = cur.fetchall()
    log_data = {}
    for (
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
        meet_date_start,
        meet_date_end,
        parsed,
    ) in rows:
        log_data.setdefault(region, {})
        log_data[region][meet_name] = {
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
            "meet_date_start": meet_date_start,
            "meet_date_end": meet_date_end,
            "parsed": bool(parsed),
        }
    return log_data


def update_log(conn, regions, downloaded_files=None):
    """
    Upsert meets from web parsing/downloading stage.
    - Preserves uploaded and processed_by_target for existing rows.
    - Updates url, processed_timestamp, downloaded, file_path, and metadata from regions.
    """
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
                    False,
                    False,
                    meet.get("meet_date"),
                    meet.get("meet_year"),
                    meet.get("location"),
                    meet.get("course"),
                ),
            )
    conn.commit()


def _retry_write(
    conn: sqlite3.Connection, sql: str, params: tuple = (), attempts: int = 3
):
    for i in range(attempts):
        try:
            conn.execute(sql, params)
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and i < attempts - 1:
                time.sleep(0.2 * (i + 1))
                continue
            raise


def get_meet_by_id(conn, meet_id: int) -> Optional[dict]:
    cur = conn.cursor()
    cur.execute(
        """SELECT id, region, meet_name, url, file_path FROM meets WHERE id=?""",
        (meet_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "region": row[1],
        "meet_name": row[2],
        "url": row[3],
        "file_path": row[4],
    }


# def get_swimmer_by_id(conn, swimmer_id: int) -> Optional[dict]:
#     cur = conn.cursor()
#     cur.execute(
#         """SELECT id,team_id, first_name, last_name, gender, birth_date, mm_number FROM swimmers WHERE id=?""",
#         (swimmer_id,),
#     )
#     row = cur.fetchone()
#     if not row:
#         return None
#     return {
#         "id": row[0],
#         "team_id": row[1],
#         "first_name": row[2],
#         "last_name": row[3],
#         "gender": row[4],
#         "birth_date": row[5],
#         "mm_number": row[6],
#     }


# Formatting date for meet_date column
def _pretty_date_from_ddmmyyyy(ddmmyyyy: Optional[str]) -> Optional[str]:
    if not ddmmyyyy or len(ddmmyyyy) != 8:
        return None
    try:
        # ddmmyyyy to datetime
        from datetime import datetime

        d = datetime.strptime(ddmmyyyy, "%d%m%Y")
        return d.strftime("%d %b %Y")  # e.g., 12 Oct 2025
    except Exception:
        return None


# Python
def update_meet_from_hy3(conn, meet_row: dict, meet_data: dict):
    """
    Update meet metadata from HY3 file:
    - meet_name: overwrite from file
    - meet_date_start/end: store as pretty "DD Mon YYYY"
    - meet_year: from meet_date_start (source)
    - course/location: fill if available (do not blank existing non-null)
    - meet_date: human-friendly display date from meet_date_start (e.g., "12 Oct 2025")
    """
    cur = conn.cursor()

    # meet_data carries start/end as DDMMYYYY (per parser)
    ddmmyyyy_start = meet_data.get("meet_date_start") or None
    ddmmyyyy_end = meet_data.get("meet_date_end") or None

    # Compute pretty formats
    pretty_meet_date = _pretty_date_from_ddmmyyyy(ddmmyyyy_start)
    pretty_start = _pretty_date_from_ddmmyyyy(ddmmyyyy_start)
    pretty_end = _pretty_date_from_ddmmyyyy(ddmmyyyy_end)
    meet_year = meet_data.get("meet_year")

    _retry_write(
        conn,
        """
        UPDATE meets
        SET meet_name       = COALESCE(?, meet_name),
            meet_date_start = COALESCE(?, meet_date_start),
            meet_date_end   = COALESCE(?, meet_date_end),
            meet_date       = COALESCE(?, meet_date),
            course          = COALESCE(?, course),
            location        = COALESCE(?, location),
            meet_year       = ?,
            parsed          = 1
        WHERE id = ?
        """,
        (
            (meet_data.get("meet_name") or "").strip() or None,
            pretty_start,  # now pretty
            pretty_end,  # now pretty
            pretty_meet_date,  # display date
            meet_data.get("course"),
            meet_data.get("location_text"),
            meet_year,
            meet_row["id"],
        ),
    )

    conn.commit()


def insert_teams(conn, meet_id: int, teams: List[dict]) -> Dict[str, int]:
    """
    Upsert teams globally (no meet_id in teams), return mapping team_code -> teams.id.
    Also optionally link meet_team if you created that table.
    """
    cur = conn.cursor()
    code_to_pk: Dict[str, int] = {}

    for t in teams:
        t_code = t.get("team_code", "")  # parser now emits "team_code"
        t_name = t.get("team_name", "")

        # Upsert globally
        _retry_write(
            conn,
            """
            INSERT OR IGNORE INTO teams (team_code, team_name, team_type, region_code, region, address_1, address_2, city, postal_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                t_code,
                t_name,
                t.get("team_type", "Other"),
                t.get("region_code", ""),
                t.get("region", ""),
                t.get("address_1", ""),
                t.get("address_2", ""),
                t.get("city", ""),
                t.get("postal_code", ""),
            ),
        )

        # Fetch id
        cur.execute(
            "SELECT id FROM teams WHERE team_code=? AND team_name=?",
            (t_code, t_name),
        )
        row = cur.fetchone()

        if row:
            team_pk = row[0]
            code_to_pk[t_code] = team_pk

            # print(f"Inserted/Found team {t_code} ({t_name}) with ID {team_pk}")

    conn.commit()
    return code_to_pk


def insert_swimmers(
    conn, meet_id: int, swimmers: List[dict], team_id_map: Dict[str, int]
) -> List[int]:
    """
    Upsert swimmers globally (no meet_id in swimmers), then return their IDs to link to the meet.
    Identity is defined by: first_name, last_name, gender, birth_date, mm_number, team_code.
    """
    cur = conn.cursor()
    swimmer_ids: List[int] = []

    for s in swimmers:
        team_pk = team_id_map.get(s.get("team_code", ""))

        # Insert or ignore if identity already exists
        _retry_write(
            conn,
            """
            INSERT OR IGNORE INTO swimmers (
                team_id, first_name, last_name, gender, birth_date, mm_number
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                team_pk,
                s.get("first_name", ""),
                s.get("last_name", ""),
                s.get("gender", ""),
                s.get("birth_date") or None,
                s.get("mm_number", ""),
            ),
        )

        # Select the ID (existing or newly inserted)
        cur.execute(
            """
            SELECT id FROM swimmers
            WHERE first_name=? AND last_name=? AND gender=?
                  AND IFNULL(birth_date,'')=IFNULL(?, '') AND mm_number=? AND IFNULL(team_id, 0)=IFNULL(?, 0)
            """,
            (
                s.get("first_name", ""),
                s.get("last_name", ""),
                s.get("gender", ""),
                s.get("birth_date") or None,
                s.get("mm_number", ""),
                team_pk,
            ),
        )
        row = cur.fetchone()
        if row:
            swimmer_ids.append(row[0])

    conn.commit()
    return swimmer_ids


def link_meet_teams(conn, meet_id: int, team_ids: List[int]) -> None:
    """Link a meet to its teams in meet_team; logs when team_ids is empty."""
    if not team_ids:
        log_error(
            conn,
            file_path=None,
            error_type="LinkWarning",
            message=f"No teams to link for meet_id={meet_id}",
            context={"meet_id": meet_id},
        )
        return
    for tid in team_ids:
        if tid is None:
            log_error(
                conn,
                file_path=None,
                error_type="LinkWarning",
                message=f"Skipping None team_id for meet_id={meet_id}",
                context={"meet_id": meet_id},
            )
            continue
        _retry_write(
            conn,
            "INSERT OR IGNORE INTO meet_team (meet_id, team_id) VALUES (?, ?)",
            (meet_id, tid),
        )
    conn.commit()


# Python
def link_meet_swimmers(conn, meet_id: int, swimmer_ids: List[int]) -> None:
    if not swimmer_ids:
        log_error(
            conn,
            file_path=None,
            error_type="LinkWarning",
            message=f"No swimmers to link for meet_id={meet_id}",
            context={"meet_id": meet_id},
        )
        return
    for sid in swimmer_ids:
        if sid is None:
            log_error(
                conn,
                file_path=None,
                error_type="LinkWarning",
                message=f"Skipping None swimmer_id for meet_id={meet_id}",
                context={"meet_id": meet_id},
            )
            continue
        _retry_write(
            conn,
            "INSERT OR IGNORE INTO meet_swimmer (meet_id, swimmer_id) VALUES (?, ?)",
            (meet_id, sid),
        )
    conn.commit()


# Python
def link_meet_teams_swimmers(conn, meet_id: int, swimmer_ids: List[int]) -> None:
    cur = conn.cursor()
    if not swimmer_ids:
        log_error(
            conn,
            file_path=None,
            error_type="LinkWarning",
            message=f"No swimmer_ids for meet_team_swimmer linking meet_id={meet_id}",
            context={"meet_id": meet_id},
        )
        return
    for sid in swimmer_ids:
        cur.execute("SELECT team_id FROM swimmers WHERE id=?", (sid,))
        row = cur.fetchone()
        team_id = row[0] if row else None
        if team_id is None:
            log_error(
                conn,
                file_path=None,
                error_type="LinkWarning",
                message=f"Swimmer {sid} has no team_id; cannot link meet_team_swimmer",
                context={"meet_id": meet_id, "swimmer_id": sid},
            )
            continue
        _retry_write(
            conn,
            "INSERT OR IGNORE INTO meet_team_swimmer (meet_id, team_id, swimmer_id) VALUES (?, ?, ?)",
            (meet_id, team_id, sid),
        )
    conn.commit()


def log_error(
    conn,
    file_path: str,
    error_type: str,
    message: str,
    context: Optional[dict] = None,
    meet_id: Optional[int] = None,
    region: Optional[str] = None,
):
    _retry_write(
        conn,
        """INSERT INTO error_log (timestamp, file_path, meet_id, region, error_type, message, context_json)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            datetime.now().isoformat(),
            file_path,
            meet_id,
            region,
            error_type,
            message,
            json.dumps(context) if context else None,
        ),
    )
    conn.commit()


def get_parse_queue(conn) -> List[dict]:
    cur = conn.cursor()
    cur.execute(
        """SELECT id, meet_id, file_path, status, message FROM parse_queue WHERE status IN ('queued','retry') ORDER BY id ASC"""
    )
    rows = cur.fetchall()
    return [
        {
            "id": r[0],
            "meet_id": r[1],
            "file_path": r[2],
            "status": r[3],
            "message": r[4],
        }
        for r in rows
    ]


def update_parse_queue_status(
    conn, queue_id: int, status: str, message: Optional[str] = None
):
    _retry_write(
        conn,
        "UPDATE parse_queue SET status=?, message=?, updated_at=? WHERE id=?",
        (status, message, datetime.now().isoformat(), queue_id),
    )
    conn.commit()


def enqueue_for_parse(conn, meet_id: int, file_path: str):
    _retry_write(
        conn,
        "INSERT INTO parse_queue (meet_id, file_path, status, created_at, updated_at) VALUES (?, ?, 'queued', ?, ?)",
        (meet_id, file_path, datetime.now().isoformat(), datetime.now().isoformat()),
    )
    conn.commit()


def mark_parsed(conn, meet_id: int):
    _retry_write(conn, "UPDATE meets SET parsed=1 WHERE id=?", (meet_id,))
    conn.commit()
