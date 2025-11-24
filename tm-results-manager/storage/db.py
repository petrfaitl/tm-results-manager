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
            parsed BOOLEAN DEFAULT FALSE
        )
        """
    )
    # Unique index across your chosen identity (no meet_id included)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_meets_url ON meets(url)")
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_meets_canonical ON meets(meet_name, meet_date_start)"
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_meets_file_path ON meets(file_path)"
    )

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
    # Python

    def _find_meet_by_file_path(conn, file_path: str):
        cur = conn.cursor()
        cur.execute("SELECT id, url FROM meets WHERE file_path = ?", (file_path,))
        return cur.fetchone()  # (id, url) or None

    for region, meets in regions.items():
        for meet in meets:
            meet_name = meet["meet_name"]
            file_path = downloaded_files.get(meet_name)
            # print(f"Updating log for meet: {meet_name}, file_path={file_path}")
            downloaded = file_path is not None
            url = meet["link"]

            # If we have a file_path, ensure we don't collide with existing row
            if downloaded and file_path:
                found = _find_meet_by_file_path(conn, file_path)
                if found:
                    # print(found)
                    existing_id, existing_url = found
                    if existing_url != url:
                        # Consolidate: update existing row (found by file_path) with this meet’s metadata and url
                        cur.execute(
                            """
                            UPDATE meets
                            SET url = ?,
                                region = COALESCE(?, region),
                                meet_name = COALESCE(?, meet_name),
                                processed_timestamp = ?,
                                downloaded = 1,
                                meet_date = COALESCE(?, meet_date),
                                meet_year = COALESCE(?, meet_year),
                                location = COALESCE(?, location),
                                course = COALESCE(?, course)
                            WHERE id = ?
                            """,
                            (
                                url,
                                region,
                                meet_name,
                                now,
                                meet.get("meet_date"),
                                meet.get("meet_year"),
                                meet.get("location"),
                                meet.get("course"),
                                existing_id,
                            ),
                        )
                        # Skip the usual INSERT/UPSERT for this meet
                        log_error(
                            conn,
                            file_path=file_path,
                            error_type="FilePathCollision",
                            message="Consolidating rows by file_path",
                            context={
                                "existing_id": existing_id,
                                "existing_url": existing_url,
                                "incoming_url": url,
                            },
                        )

                        continue

            # Normal upsert by URL
            cur.execute(
                """
                INSERT INTO meets 
                (region, meet_name, url, processed_timestamp, downloaded, file_path, uploaded, processed_by_target,
                meet_date, meet_year, location, course)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                region=excluded.region,
                meet_name=excluded.meet_name,
                processed_timestamp=excluded.processed_timestamp,
                downloaded=excluded.downloaded OR meets.downloaded,
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
                    url,
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


# def _find_meet_by_file_path(conn, file_path: str):
#     cur = conn.cursor()
#     cur.execute("SELECT id, url FROM meets WHERE file_path = ?", (file_path,))
#     return cur.fetchone()  # (id, url) or None


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


def _pretty_date_token(token: str) -> str | None:
    # Accept formats like 02Nov2024 and return "02 Nov 2024"
    if not token or len(token) != 9:
        return None
    try:
        d = datetime.strptime(token, "%d%b%Y")
        return d.strftime("%d %b %Y")
    except ValueError:
        return None


def _iso_from_token(token: Optional[str]) -> Optional[str]:
    # token like 02Nov2024 -> 2024-11-02
    if not token or len(token) != 9:
        return None
    try:
        d = datetime.strptime(token, "%d%b%Y")
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None


# Python
def _pretty_from_iso(iso: Optional[str]) -> Optional[str]:
    if not iso:
        return None
    try:
        d = datetime.strptime(iso, "%Y-%m-%d")
        return d.strftime("%d %b %Y")
    except Exception:
        return iso  # fallback to raw if unexpected


def _iso_from_ddmmyyyy(ddmmyyyy: Optional[str]) -> Optional[str]:
    if not ddmmyyyy or len(ddmmyyyy) != 8:
        return None
    try:
        d = datetime.strptime(ddmmyyyy, "%d%m%Y")
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None


def find_meet_by_canonical(
    conn, meet_name: str, meet_date_start_iso: str
) -> Optional[int]:
    """
    Return the meets.id that matches a canonical (meet_name + meet_date_start ISO).
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM meets WHERE meet_name=? AND meet_date_start=?",
        (meet_name, meet_date_start_iso),
    )
    row = cur.fetchone()
    return row[0] if row else None


# Python
def update_meet_from_hy3(conn, meet_row: dict, meet_data: dict):
    """
    Update meet metadata from HY3 file:
    - Overwrite meet_name from file (canonical)
    - Store dates in ISO (YYYY-MM-DD): meet_date_start, meet_date_end, meet_date (mirrors start)
    - Set meet_year from start date (already computed by parser and passed in meet_data)
    - Fill course/location if provided (do not blank existing non-null values)
    - Flag parsed=1
    - If another row already has the same canonical identity (meet_name + meet_date_start ISO),
      merge this row into the existing one and stop.
    """
    cur = conn.cursor()

    # Parser supplies DDMMYYYY; convert to ISO for storage and canonical matching
    ddmmyyyy_start = meet_data.get("meet_date_start") or None
    ddmmyyyy_end = meet_data.get("meet_date_end") or None

    iso_start = _iso_from_ddmmyyyy(ddmmyyyy_start)
    iso_end = _iso_from_ddmmyyyy(ddmmyyyy_end)

    # Canonical duplicate detection and merge
    if meet_data.get("meet_name") and iso_start:
        other_id = find_meet_by_canonical(
            conn, meet_data["meet_name"].strip(), iso_start
        )
        if other_id and other_id != meet_row["id"]:
            # Merge current row into the existing canonical row and stop
            log_error(
                conn,
                file_path=(
                    meet_row.get("file_path") if isinstance(meet_row, dict) else None
                ),
                error_type="CanonicalMerge",
                message=f"Merging meet {meet_row['id']} into canonical {other_id}",
                context={
                    "source_id": meet_row["id"],
                    "target_id": other_id,
                    "meet_name": meet_data["meet_name"],
                    "meet_date_start_iso": iso_start,
                },
            )
            merge_meets(conn, source_id=meet_row["id"], target_id=other_id)
            return

    meet_year = meet_data.get("meet_year")

    try:
        _retry_write(
            conn,
            """
            UPDATE meets
            SET meet_name       = COALESCE(?, meet_name),
                meet_date_start = COALESCE(?, meet_date_start),
                meet_date_end   = COALESCE(?, meet_date_end),
                meet_date       = COALESCE(?, meet_date),   -- mirror start date (ISO)
                course          = COALESCE(?, course),
                location        = COALESCE(?, location),
                meet_year       = ?,
                parsed          = 1
            WHERE id = ?
            """,
            (
                (meet_data.get("meet_name") or "").strip() or None,
                iso_start,
                iso_end,
                iso_start,
                meet_data.get("course"),
                meet_data.get("location_text"),
                meet_year,
                meet_row["id"],
            ),
        )
        conn.commit()
    except sqlite3.IntegrityError as e:
        # Likely a conflict on ux_meets_canonical (meet_name, meet_date_start)
        if meet_data.get("meet_name") and iso_start:
            other_id = find_meet_by_canonical(
                conn, meet_data["meet_name"].strip(), iso_start
            )
            if other_id and other_id != meet_row["id"]:
                log_error(
                    conn,
                    file_path=(
                        meet_row.get("file_path")
                        if isinstance(meet_row, dict)
                        else None
                    ),
                    error_type="CanonicalMerge",
                    message=f"IntegrityError on canonical update; merging {meet_row['id']} -> {other_id}",
                    context={
                        "source_id": meet_row["id"],
                        "target_id": other_id,
                        "error": str(e),
                    },
                )
                merge_meets(conn, source_id=meet_row["id"], target_id=other_id)
                return
        # If not canonical-related, re-raise
        raise


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
        t_type = t.get("team_type", "")

        # Upsert globally
        _retry_write(
            conn,
            """
            INSERT INTO teams (team_code, team_name, team_type, region_code, region, address_1, address_2, city, postal_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(team_code, team_name) DO UPDATE SET
                team_type=excluded.team_type,
                region_code=COALESCE(excluded.region_code, teams.region_code),
                region=COALESCE(excluded.region, teams.region),
                address_1=COALESCE(excluded.address_1, teams.address_1),
                address_2=COALESCE(excluded.address_2, teams.address_2),
                city=COALESCE(excluded.city, teams.city),
                postal_code=COALESCE(excluded.postal_code, teams.postal_code)
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


def merge_meets(conn, source_id: int, target_id: int):
    """
    Merge 'source' meet row into 'target':
    - Keep target row, delete source.
    - Prefer target.file_path; if missing, move source.file_path over.
    - Consolidate flags.
    - Repoint link tables and parse_queue to target_id.
    """
    cur = conn.cursor()

    # Pull both rows
    cur.execute(
        "SELECT id, region, meet_name, url, processed_timestamp, downloaded, file_path, uploaded, processed_by_target, meet_date, meet_year, location, course, meet_date_start, meet_date_end, parsed FROM meets WHERE id=?",
        (target_id,),
    )
    target = cur.fetchone()
    cur.execute(
        "SELECT id, region, meet_name, url, processed_timestamp, downloaded, file_path, uploaded, processed_by_target, meet_date, meet_year, location, course, meet_date_start, meet_date_end, parsed FROM meets WHERE id=?",
        (source_id,),
    )
    source = cur.fetchone()

    if not target or not source:
        return

    # Consolidate file_path/flags/fields into target
    target_file = target[6]
    source_file = source[6]
    new_file = target_file or source_file
    downloaded = int((target[5] or 0) or (source[5] or 0))
    uploaded = int((target[7] or 0) or (source[7] or 0))
    processed_by_target = int((target[8] or 0) or (source[8] or 0))
    meet_date = target[9] or source[9]
    meet_year = target[10] or source[10]
    location = target[11] or source[11]
    course = target[12] or source[12]
    meet_date_start = target[13] or source[13]
    meet_date_end = target[14] or source[14]
    parsed = int((target[15] or 0) or (source[15] or 0))

    # Update target row
    _retry_write(
        conn,
        """
        UPDATE meets
        SET downloaded=?,
            file_path=?,
            uploaded=?,
            processed_by_target=?,
            meet_date=COALESCE(?, meet_date),
            meet_year=COALESCE(?, meet_year),
            location=COALESCE(?, location),
            course=COALESCE(?, course),
            meet_date_start=COALESCE(?, meet_date_start),
            meet_date_end=COALESCE(?, meet_date_end),
            parsed=?
        WHERE id=?
    """,
        (
            downloaded,
            new_file,
            uploaded,
            processed_by_target,
            meet_date,
            meet_year,
            location,
            course,
            meet_date_start,
            meet_date_end,
            parsed,
            target_id,
        ),
    )

    # Repoint links and queue to target
    for table, col in [
        ("meet_team", "meet_id"),
        ("meet_swimmer", "meet_id"),
        ("meet_team_swimmer", "meet_id"),
        ("parse_queue", "meet_id"),
        ("error_log", "meet_id"),
    ]:
        _retry_write(
            conn, f"UPDATE {table} SET {col}=? WHERE {col}=?", (target_id, source_id)
        )

    # Finally, delete the source row
    _retry_write(conn, "DELETE FROM meets WHERE id=?", (source_id,))
    conn.commit()


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
