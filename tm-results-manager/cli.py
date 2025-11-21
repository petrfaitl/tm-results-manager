# Python
import json
import os
from pathlib import Path
import click
from datetime import datetime
from .config import URL, DEFAULT_OUTPUT_DIR, CSV_FILE
from .http import fetch_page
from .parsing.community_page import parse_meets
from .storage.db import (
    init_db,
    load_log,
    update_log,
    enqueue_for_parse,
    _pretty_from_iso,
)
from .pipeline.downloader import download_files
from .pipeline.exporter import export_to_csv
from .pipeline.ingest_results import ingest_queue
from .parsing.hy3_parser import REGION_CODES_PATH  # for region list


def _collect_queue_summary(conn, queue_ids: list[int]) -> dict:
    cur = conn.cursor()
    if not queue_ids:
        return {"total": 0, "done": 0, "error": 0, "processing": 0, "queued": 0}

    qmarks = ",".join(["?"] * len(queue_ids))
    cur.execute(
        f"SELECT status, COUNT(*) FROM parse_queue WHERE id IN ({qmarks}) GROUP BY status",
        queue_ids,
    )
    stats = {
        "total": len(queue_ids),
        "done": 0,
        "error": 0,
        "processing": 0,
        "queued": 0,
    }
    for status, cnt in cur.fetchall():
        stats[status] = cnt
    return stats


def _collect_recent_errors_for_queue(
    conn, queue_ids: list[int], limit_per_meet: int = 2
) -> list[tuple[str, str]]:
    # Return list of (file_path, message) pairs for the given queue items
    if not queue_ids:
        return []
    cur = conn.cursor()
    # Find meet_ids for those queue items
    qmarks = ",".join(["?"] * len(queue_ids))
    cur.execute(
        f"SELECT id, meet_id, file_path FROM parse_queue WHERE id IN ({qmarks})",
        queue_ids,
    )
    rows = cur.fetchall()
    by_meet = {}
    for _, meet_id, file_path in rows:
        by_meet.setdefault(meet_id, file_path)

    errors = []
    for meet_id, file_path in by_meet.items():
        cur.execute(
            """
            SELECT message FROM error_log
            WHERE meet_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (meet_id, limit_per_meet),
        )
        for (msg,) in cur.fetchall():
            errors.append((file_path or "<unknown>", msg or ""))
    return errors


@click.group(help="TM Results Manager CLI")
def cli():
    pass


@cli.command("run")
@click.option(
    "--region", multiple=True, help="Specify region(s) (e.g., 'Bay of Plenty')"
)
@click.option("--all-regions", is_flag=True, help="Process all regions")
@click.option("--download", is_flag=True, help="Download result files")
@click.option("--export-csv", is_flag=True, help="Export results to CSV")
@click.option(
    "--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for downloads"
)
@click.option("--csv-file", default=CSV_FILE, help="CSV output file")
@click.option(
    "--enqueue-parse", is_flag=True, help="Enqueue downloaded files for parsing"
)
@click.option(
    "--parse-now", is_flag=True, help="Run the parse queue immediately after enqueueing"
)
@click.option(
    "--process-new", is_flag=True, help="Download and parse meets where downloaded=0"
)
def run(
    region,
    all_regions,
    download,
    export_csv,
    output_dir,
    csv_file,
    enqueue_parse,
    parse_now,
    process_new,
):
    """
    Main workflow:
    - Default: fetch community page, parse, optionally download, update DB, optional CSV export.
    - --process-new: skip page fetch; download and parse only new (downloaded=0) meets in the DB.
    """
    conn = init_db()
    cur = conn.cursor()

    if process_new:
        # Optional filters
        if region:
            # multiple region flags accepted; normalize into tuple for SQL IN
            regions = tuple(region)
            placeholders = ",".join(["?"] * len(regions))
            cur.execute(
                f"""
                SELECT id, region, meet_name, url
                FROM meets
                WHERE downloaded=0 AND region IN ({placeholders})
            """,
                regions,
            )
        elif all_regions:
            cur.execute(
                """
                SELECT id, region, meet_name, url
                FROM meets
                WHERE downloaded=0
            """
            )
        else:
            # No filters: process all new
            cur.execute(
                """
                SELECT id, region, meet_name, url
                FROM meets
                WHERE downloaded=0
            """
            )

        rows = cur.fetchall()
        if not rows:
            click.echo("No new meets to process (downloaded=0).")
            conn.close()
            return

        # Build a minimal regions dict so we can reuse download_files
        regions_payload = {}
        for _id, reg, meet_name, url in rows:
            regions_payload.setdefault(reg, []).append(
                {
                    "meet_name": meet_name,
                    "link": url,
                    # placeholders; download_files and update_log expect these keys
                    "meet_date": None,
                    "meet_year": None,
                    "location": None,
                    "course": None,
                }
            )

        # Use existing log to allow downloader to skip if anything marked downloaded already
        log_data = load_log(conn)

        # Download only these meets
        downloaded_files = download_files(regions_payload, output_dir, log_data)

        # Update DB for these meets
        update_log(conn, regions_payload, downloaded_files)

        # Enqueue just the newly downloaded files
        now = datetime.now().isoformat()
        queue_ids = []
        for reg, meets in regions_payload.items():
            for m in meets:
                fp = downloaded_files.get(m["meet_name"])
                if not fp:
                    continue
                cur.execute("SELECT id FROM meets WHERE url=? LIMIT 1", (m["link"],))
                meet_row = cur.fetchone()
                if not meet_row:
                    continue
                meet_id = meet_row[0]
                cur.execute(
                    "INSERT INTO parse_queue (meet_id, file_path, status, created_at, updated_at) VALUES (?, ?, 'queued', ?, ?)",
                    (meet_id, fp, now, now),
                )
                conn.commit()
                cur.execute("SELECT last_insert_rowid()")
                qrow = cur.fetchone()
                if qrow:
                    queue_ids.append(qrow[0])

        # Parse now
        ingest_queue(conn)

        # Success/failure summary
        stats = _collect_queue_summary(conn, queue_ids)
        errors = _collect_recent_errors_for_queue(conn, queue_ids)
        if stats.get("error", 0) > 0 or errors:
            click.echo(
                f"Completed with errors: processed={stats.get('total',0)}, done={stats.get('done',0)}, errors={stats.get('error',0)}"
            )
            for fp, msg in errors[:10]:
                click.echo(f"- {fp}: {msg}")
        else:
            click.echo(
                f"Success: processed={stats.get('total',0)}, done={stats.get('done',0)}"
            )

        conn.close()
        return

    """
    Fetch the Swimming NZ community page, parse meets, optionally download files,
    update the database, and optionally export to CSV.
    """
    html = fetch_page(URL)
    if not html:
        return

    conn = init_db()
    regions = parse_meets(html)
    log_data = load_log(conn)

    # Filter regions if specified
    if not all_regions and region:
        regions = {r: regions[r] for r in region if r in regions}

    downloaded_files = {}
    if download:
        downloaded_files = download_files(regions, output_dir, log_data)

    # Update DB with parsed web data and download results
    update_log(conn, regions, downloaded_files)

    # Optionally enqueue newly downloaded files for parsing
    if download and enqueue_parse:
        cur = conn.cursor()
        for region_name, meets in regions.items():
            for m in meets:
                meet_name = m["meet_name"]
                file_path = downloaded_files.get(meet_name)
                if not file_path:
                    continue
                cur.execute(
                    "SELECT id FROM meets WHERE region=? AND meet_name=?",
                    (region_name, meet_name),
                )
                row = cur.fetchone()
                if row:
                    enqueue_for_parse(conn, row[0], file_path)
    if download and enqueue_parse and parse_now:
        ingest_queue(conn)
    if export_csv:
        export_to_csv(regions, log_data, csv_file)

    # Optional: console view
    for reg, meets in regions.items():
        print(f"\nRegion: {reg}")
        for m in meets:
            print(
                f"  Meet: {m['meet_name']}, Date: {_pretty_from_iso(m["meet_date"])}, Year: {m.get('meet_year')}, Location: {m.get('location')}, Course: {m.get('course')}"
            )

    conn.close()


def _read_regions_from_codes() -> list[str]:
    try:
        data = json.loads(Path(REGION_CODES_PATH).read_text(encoding="utf-8"))
        return sorted(list(data.keys()))
    except Exception:
        # fallback to known list if file missing
        return sorted(
            [
                "Auckland",
                "Bay of Plenty",
                "Canterbury",
                "West Coast",
                "Hawkes Bay / Poverty Bay",
                "Manawatu",
                "National & International",
                "Nelson Marlborough",
                "Northland",
                "Otago",
                "Southland",
                "Taranaki",
                "Waikato",
                "Wellington",
            ]
        )


def _prompt_region() -> str:
    regions = _read_regions_from_codes()
    return click.prompt(
        "Select region", type=click.Choice(regions, case_sensitive=True)
    )


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _move_zip_into_region(src_file: Path, region: str, downloads_root: Path) -> Path:
    """Copy zip into results/<Region> and return destination path. Leaves source in place; caller may delete/move."""
    _ensure_dir(downloads_root)
    region_dir = downloads_root / region.replace("/", "_")
    _ensure_dir(region_dir)
    dest = region_dir / src_file.name
    if src_file.resolve() != dest.resolve():
        dest.write_bytes(src_file.read_bytes())
    return dest


def _default_import_dir() -> Path:
    return Path.cwd() / "import_files"


@cli.command("parse-files")
@click.option(
    "--all-downloaded", is_flag=True, help="Parse all downloaded but unparsed meets"
)
@click.option(
    "--region",
    type=str,
    help="Region name. In manual modes, uses this region without prompting. In downloaded modes, filters meets by this region.",
)
@click.option(
    "--meet", "meet_name", type=str, help="Parse a specific meet by exact name"
)
@click.option("--manual", is_flag=True, help="Manually import a local zip and parse it")
@click.option(
    "--manual-bulk",
    is_flag=True,
    help="Import and parse all zips in import_files (use --region to avoid per-file prompts)",
)
@click.option(
    "--keep-import",
    is_flag=True,
    help="Keep original zips in import_files; otherwise move to import_files/processed",
)
def parse_files(all_downloaded, region, meet_name, manual, manual_bulk, keep_import):
    """
    Parse HY3 result files from downloaded zips (or manual imports).
    """
    conn = init_db()
    cur = conn.cursor()

    def choose_region() -> str:
        return region if region else _prompt_region()

    def enqueue_and_parse_for_file(region_name: str, src_zip: Path) -> int | None:
        # 1) Move/copy into results/<Region>
        downloads_root = Path(DEFAULT_OUTPUT_DIR)
        dest_zip = _move_zip_into_region(src_zip, region_name, downloads_root)

        # 2) Upsert a placeholder meet entry (temporary meet_name from filename stem)
        temp_meet_name = dest_zip.stem
        now = datetime.now().isoformat()
        cur.execute(
            """
            INSERT INTO meets (region, meet_name, url, processed_timestamp, downloaded, file_path, uploaded, processed_by_target)
            VALUES (?, ?, ?, ?, 1, ?, 0, 0)
            ON CONFLICT(file_path) DO UPDATE SET
              downloaded=1,
              file_path=excluded.file_path,
              processed_timestamp=excluded.processed_timestamp
            """,
            (
                region_name,
                temp_meet_name,
                f"manual://{dest_zip.name}",
                now,
                str(dest_zip),
            ),
        )
        conn.commit()

        # 3) Fetch ID and enqueue
        cur.execute(
            "SELECT id FROM meets WHERE region=? AND meet_name=?",
            (region_name, temp_meet_name),
        )
        row = cur.fetchone()
        if not row:
            click.echo(f"Failed to upsert meet row for {dest_zip.name}")
            return None
        meet_id = row[0]

        # Enqueue
        cur.execute(
            "INSERT INTO parse_queue (meet_id, file_path, status, created_at, updated_at) VALUES (?, ?, 'queued', ?, ?)",
            (meet_id, str(dest_zip), now, now),
        )
        conn.commit()
        # Return new queue id
        cur.execute("SELECT last_insert_rowid()")
        qid_row = cur.fetchone()
        queue_id = qid_row[0] if qid_row else None

        # 4) After enqueue, move original into import_files/processed unless keeping
        if not keep_import:
            processed_dir = src_zip.parent / "processed"
            _ensure_dir(processed_dir)
            try:
                src_zip.replace(processed_dir / src_zip.name)
            except Exception:
                try:
                    (processed_dir / src_zip.name).write_bytes(src_zip.read_bytes())
                    src_zip.unlink(missing_ok=True)
                except Exception:
                    pass

        return queue_id

    # Manual single-file mode
    if manual:
        chosen_region = choose_region()
        default_dir = _default_import_dir()
        _ensure_dir(default_dir)
        zips = sorted([p for p in default_dir.glob("*.zip")])
        if not zips:
            click.echo(f"No .zip files found in {default_dir}")
            conn.close()
            return

        choices = {str(i + 1): z for i, z in enumerate(zips)}
        click.echo("Available zips:")
        for i, z in choices.items():
            click.echo(f"{i}) {z.name}")
        choice = click.prompt(
            "Choose a file number",
            type=click.Choice(list(choices.keys()), case_sensitive=False),
        )
        src_zip = choices[choice]

        queue_id = enqueue_and_parse_for_file(chosen_region, src_zip)
        ingest_queue(conn)

        # Summary
        qids = [queue_id] if queue_id else []
        stats = _collect_queue_summary(conn, qids)
        errors = _collect_recent_errors_for_queue(conn, qids)
        if stats.get("error", 0) > 0 or errors:
            click.echo(
                f"Completed with errors: processed={stats.get('total',0)}, done={stats.get('done',0)}, errors={stats.get('error',0)}"
            )
            for fp, msg in errors:
                click.echo(f"- {fp}: {msg}")
        else:
            click.echo(
                f"Success: processed={stats.get('total',0)}, done={stats.get('done',0)}"
            )

        conn.close()
        return

    # Manual bulk mode
    if manual_bulk:
        default_dir = _default_import_dir()
        _ensure_dir(default_dir)
        zips = sorted([p for p in default_dir.glob("*.zip")])
        if not zips:
            click.echo(f"No .zip files found in {default_dir}")
            conn.close()
            return

        click.echo(f"Found {len(zips)} zip(s) in {default_dir}")
        queue_ids: list[int] = []
        for src_zip in zips:
            chosen_region = choose_region()
            click.echo(f"Processing: {src_zip.name} -> Region: {chosen_region}")
            qid = enqueue_and_parse_for_file(chosen_region, src_zip)
            if qid:
                queue_ids.append(qid)

        ingest_queue(conn)

        # Summary
        stats = _collect_queue_summary(conn, queue_ids)
        errors = _collect_recent_errors_for_queue(conn, queue_ids)
        if stats.get("error", 0) > 0 or errors:
            click.echo(
                f"Completed with errors: processed={stats.get('total',0)}, done={stats.get('done',0)}, errors={stats.get('error',0)}"
            )
            for fp, msg in errors[:10]:
                click.echo(f"- {fp}: {msg}")
        else:
            click.echo(
                f"Success: processed={stats.get('total',0)}, done={stats.get('done',0)}"
            )

        conn.close()
        return

    # Existing modes (downloaded-based)
    if meet_name:
        cur.execute(
            "SELECT id, file_path FROM meets WHERE meet_name=? AND downloaded=1",
            (meet_name,),
        )
    elif region:
        cur.execute(
            "SELECT id, file_path FROM meets WHERE region=? AND downloaded=1", (region,)
        )
    elif all_downloaded:
        cur.execute(
            "SELECT id, file_path FROM meets WHERE downloaded=1 AND (parsed IS NULL OR parsed=0)"
        )
    else:
        print(
            "Nothing to do. Provide --all-downloaded, --region, --meet, --manual, or --manual-bulk."
        )
        conn.close()
        return

    rows = cur.fetchall()
    to_enqueue = [(r[0], r[1]) for r in rows if r[1]]
    queue_ids: list[int] = []
    now = datetime.now().isoformat()
    for meet_id, file_path in to_enqueue:
        cur.execute(
            "INSERT INTO parse_queue (meet_id, file_path, status, created_at, updated_at) VALUES (?, ?, 'queued', ?, ?)",
            (meet_id, file_path, now, now),
        )
        conn.commit()
        cur.execute("SELECT last_insert_rowid()")
        qrow = cur.fetchone()
        if qrow:
            queue_ids.append(qrow[0])

    ingest_queue(conn)

    stats = _collect_queue_summary(conn, queue_ids)
    errors = _collect_recent_errors_for_queue(conn, queue_ids)
    if stats.get("error", 0) > 0 or errors:
        click.echo(
            f"Completed with errors: processed={stats.get('total',0)}, done={stats.get('done',0)}, errors={stats.get('error',0)}"
        )
        for fp, msg in errors[:10]:
            click.echo(f"- {fp}: {msg}")
    else:
        click.echo(
            f"Success: processed={stats.get('total',0)}, done={stats.get('done',0)}"
        )

    conn.close()


if __name__ == "__main__":
    cli()
