# Python
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from ..parsing.hy3_parser import parse_hy3_zip
from ..storage import db as dbmod


def ingest_zip(conn, zip_path: str, meet_row: Optional[dict] = None) -> None:
    if not meet_row:
        # No meet record to attach results to; log and bail
        dbmod.log_error(
            conn,
            file_path=zip_path,
            error_type="IngestError",
            message="Meet row not found for queued item",
            context={"zip_path": zip_path},
        )
        return

    meet, teams, swimmers, warnings = parse_hy3_zip(Path(zip_path))

    if not meet:
        for w in warnings:
            dbmod.log_error(
                conn,
                file_path=zip_path,
                error_type=w.get("type", "Parse"),
                message=w.get("message", ""),
                context=w,
            )
        return

    dbmod.update_meet_from_hy3(conn, meet_row, meet)

    team_id_map = dbmod.insert_teams(conn, meet_row["id"], teams)

    dbmod.link_meet_teams(conn, meet_row["id"], list(team_id_map.values()))

    swimmer_ids = dbmod.insert_swimmers(conn, meet_row["id"], swimmers, team_id_map)

    dbmod.link_meet_swimmers(conn, meet_row["id"], swimmer_ids)
    dbmod.link_meet_teams_swimmers(conn, meet_row["id"], swimmer_ids)

    for w in warnings:
        dbmod.log_error(
            conn,
            file_path=zip_path,
            meet_id=meet_row["id"],
            error_type=w.get("type", "Warn"),
            message=w.get("message", ""),
            context=w,
        )
    dbmod.mark_parsed(conn, meet_row["id"])


def ingest_queue(conn) -> None:
    """Process parse_queue sequentially."""
    items = dbmod.get_parse_queue(conn)

    for item in items:
        try:
            dbmod.update_parse_queue_status(conn, item["id"], "processing")

            ingest_zip(
                conn,
                item["file_path"],
                meet_row=dbmod.get_meet_by_id(conn, item["meet_id"]),
            )
            dbmod.update_parse_queue_status(conn, item["id"], "done")
        except Exception as e:
            dbmod.update_parse_queue_status(conn, item["id"], "error", str(e))

            dbmod.log_error(
                conn,
                file_path=item["file_path"],
                meet_id=item["meet_id"],
                error_type="IngestError",
                message=str(e),
                context=None,
            )
