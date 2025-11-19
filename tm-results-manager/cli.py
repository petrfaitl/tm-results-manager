# Python
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

        # Optionally enqueue newly downloaded files for parsing
        if enqueue_parse:
            # downloaded_files is a dict: meet_name -> file_path
            # We need to map meet_name to DB row id; update_log will upsert rows
            # so after update_log we can enqueue by joining on region+meet_name
            pass  # see note below

    # Update DB with parsed web data and download results
    update_log(conn, regions, downloaded_files)

    # Now that meets are upserted, we can enqueue the downloaded files if requested
    if download and enqueue_parse:
        cur = conn.cursor()
        for region_name, meets in regions.items():
            for m in meets:
                meet_name = m["meet_name"]
                file_path = downloaded_files.get(meet_name)
                if not file_path:
                    continue
                # Find the meet row we just upserted
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
def parse_files(all_downloaded, region, meet_name):
    """
    Parse HY3 result files from downloaded zips using the parse queue.
    Processes sequentially to avoid temp directory collisions.
    """
    conn = init_db()
    cur = conn.cursor()

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
        cur.execute("SELECT id, file_path FROM meets WHERE downloaded=1 AND parsed=0")
    else:
        print("Nothing to do. Provide --all-downloaded, --region, or --meet.")
        conn.close()
        return

    rows = cur.fetchall()
    to_enqueue = [(r[0], r[1]) for r in rows if r[1]]
    for meet_id, file_path in to_enqueue:
        # print(f"Enqueuing meet ID {meet_id} for parsing from file {file_path}")
        enqueue_for_parse(conn, meet_id, file_path)

    # Process sequentially
    ingest_queue(conn)
    conn.close()


if __name__ == "__main__":
    cli()
