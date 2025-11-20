# Python
import json
import os
from pathlib import Path
import click
from .config import URL, DEFAULT_OUTPUT_DIR, CSV_FILE
from .http import fetch_page
from .parsing.community_page import parse_meets
from .storage.db import (
    init_db,
    load_log,
    update_log,
    enqueue_for_parse,
)
from .pipeline.downloader import download_files
from .pipeline.exporter import export_to_csv
from .pipeline.ingest_results import ingest_queue
from .parsing.hy3_parser import REGION_CODES_PATH  # for region list


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
def run(region, all_regions, download, export_csv, output_dir, csv_file, enqueue_parse):
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

    if export_csv:
        export_to_csv(regions, log_data, csv_file)

    # Optional: console view
    for reg, meets in regions.items():
        print(f"\nRegion: {reg}")
        for m in meets:
            print(
                f"  Meet: {m['meet_name']}, Date: {m.get('meet_date')}, Year: {m.get('meet_year')}, Location: {m.get('location')}, Course: {m.get('course')}"
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
    help="Parse downloaded meets for a specific meet region label (from download stage)",
)
@click.option(
    "--meet", "meet_name", type=str, help="Parse a specific meet by exact name"
)
@click.option("--manual", is_flag=True, help="Manually import a local zip and parse it")
@click.option(
    "--manual-bulk",
    is_flag=True,
    help="Import and parse all zips in import_files (prompt region per file)",
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

    def enqueue_and_parse_for_file(region_name: str, src_zip: Path):
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
            ON CONFLICT(region, meet_name) DO UPDATE SET
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
            return
        meet_id = row[0]
        enqueue_for_parse(conn, meet_id, str(dest_zip))

        # 4) After enqueue, move original into import_files/processed unless keeping
        if not keep_import:
            processed_dir = src_zip.parent / "processed"
            _ensure_dir(processed_dir)
            try:
                src_zip.replace(processed_dir / src_zip.name)
            except Exception:
                # Fallback: copy then delete
                try:
                    (processed_dir / src_zip.name).write_bytes(src_zip.read_bytes())
                    src_zip.unlink(missing_ok=True)
                except Exception:
                    pass

    # Manual single-file mode
    if manual:
        chosen_region = _prompt_region()
        default_dir = _default_import_dir()
        _ensure_dir(default_dir)

        # Let user pick a single zip
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
            type=click.Choice(list(choices.keys()), case_insensitive=False),
        )
        src_zip = choices[choice]

        enqueue_and_parse_for_file(chosen_region, src_zip)
        ingest_queue(conn)
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
        for src_zip in zips:
            click.echo(f"\nProcessing: {src_zip.name}")
            chosen_region = _prompt_region()
            enqueue_and_parse_for_file(chosen_region, src_zip)

        ingest_queue(conn)
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
    for meet_id, file_path in to_enqueue:
        enqueue_for_parse(conn, meet_id, file_path)

    ingest_queue(conn)
    conn.close()


# # Python
# import click
# from .config import URL, DEFAULT_OUTPUT_DIR, CSV_FILE
# from .http import fetch_page
# from .parsing.community_page import parse_meets
# from .storage.db import (
#     init_db,
#     load_log,
#     update_log,
#     enqueue_for_parse,
# )
# from .pipeline.downloader import download_files
# from .pipeline.exporter import export_to_csv
# from .pipeline.ingest_results import ingest_queue


# @click.group(help="TM Results Manager CLI")
# def cli():
#     pass


# @cli.command("run")
# @click.option(
#     "--region", multiple=True, help="Specify region(s) (e.g., 'Bay of Plenty')"
# )
# @click.option("--all-regions", is_flag=True, help="Process all regions")
# @click.option("--download", is_flag=True, help="Download result files")
# @click.option("--export-csv", is_flag=True, help="Export results to CSV")
# @click.option(
#     "--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for downloads"
# )
# @click.option("--csv-file", default=CSV_FILE, help="CSV output file")
# @click.option(
#     "--enqueue-parse", is_flag=True, help="Enqueue downloaded files for parsing"
# )
# def run(region, all_regions, download, export_csv, output_dir, csv_file, enqueue_parse):
#     """
#     Fetch the Swimming NZ community page, parse meets, optionally download files,
#     update the database, and optionally export to CSV.
#     """
#     html = fetch_page(URL)
#     if not html:
#         return

#     conn = init_db()
#     regions = parse_meets(html)
#     log_data = load_log(conn)

#     # Filter regions if specified
#     if not all_regions and region:
#         regions = {r: regions[r] for r in region if r in regions}

#     downloaded_files = {}
#     if download:
#         downloaded_files = download_files(regions, output_dir, log_data)

#         # Optionally enqueue newly downloaded files for parsing
#         if enqueue_parse:
#             # downloaded_files is a dict: meet_name -> file_path
#             # We need to map meet_name to DB row id; update_log will upsert rows
#             # so after update_log we can enqueue by joining on region+meet_name
#             pass  # see note below

#     # Update DB with parsed web data and download results
#     update_log(conn, regions, downloaded_files)

#     # Now that meets are upserted, we can enqueue the downloaded files if requested
#     if download and enqueue_parse:
#         cur = conn.cursor()
#         for region_name, meets in regions.items():
#             for m in meets:
#                 meet_name = m["meet_name"]
#                 file_path = downloaded_files.get(meet_name)
#                 if not file_path:
#                     continue
#                 # Find the meet row we just upserted
#                 cur.execute(
#                     "SELECT id FROM meets WHERE region=? AND meet_name=?",
#                     (region_name, meet_name),
#                 )
#                 row = cur.fetchone()
#                 if row:
#                     enqueue_for_parse(conn, row[0], file_path)

#     if export_csv:
#         export_to_csv(regions, log_data, csv_file)

#     # Optional: console view
#     for reg, meets in regions.items():
#         print(f"\nRegion: {reg}")
#         for m in meets:
#             print(
#                 f"  Meet: {m['meet_name']}, Date: {m.get('meet_date')}, Year: {m.get('meet_year')}, Location: {m.get('location')}, Course: {m.get('course')}"
#             )

#     conn.close()


# @cli.command("parse-files")
# @click.option(
#     "--all-downloaded", is_flag=True, help="Parse all downloaded but unparsed meets"
# )
# @click.option(
#     "--region",
#     type=str,
#     help="Parse downloaded meets for a specific meet region label (from download stage)",
# )
# @click.option(
#     "--meet", "meet_name", type=str, help="Parse a specific meet by exact name"
# )
# def parse_files(all_downloaded, region, meet_name):
#     """
#     Parse HY3 result files from downloaded zips using the parse queue.
#     Processes sequentially to avoid temp directory collisions.
#     """
#     conn = init_db()
#     cur = conn.cursor()

#     if meet_name:
#         cur.execute(
#             "SELECT id, file_path FROM meets WHERE meet_name=? AND downloaded=1",
#             (meet_name,),
#         )
#     elif region:
#         cur.execute(
#             "SELECT id, file_path FROM meets WHERE region=? AND downloaded=1", (region,)
#         )
#     elif all_downloaded:
#         cur.execute(
#             "SELECT id, file_path FROM meets WHERE downloaded=1 AND parsed is NULL"
#         )
#     else:
#         print("Nothing to do. Provide --all-downloaded, --region, or --meet.")
#         conn.close()
#         return

#     rows = cur.fetchall()
#     to_enqueue = [(r[0], r[1]) for r in rows if r[1]]
#     for meet_id, file_path in to_enqueue:
#         # print(f"Enqueuing meet ID {meet_id} for parsing from file {file_path}")
#         enqueue_for_parse(conn, meet_id, file_path)

#     # Process sequentially
#     ingest_queue(conn)
#     warnings = load_log(conn)  # Refresh log after parsing
#     print("\nParsing completed. Warnings/Errors:")
#     if not warnings.get("errors"):
#         print("  None")
#     for w in warnings.get("errors", []):
#         print(
#             f"- [{w.get('type')}] Meet ID {w.get('meet_id')}, File: {w.get('file_path')}: {w.get('message')}"
#         )
#     conn.close()


if __name__ == "__main__":
    cli()
