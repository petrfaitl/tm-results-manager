# Python
import click
from .config import URL, DEFAULT_OUTPUT_DIR, CSV_FILE
from .http import fetch_page
from .parsing.community_page import parse_meets
from .storage.db import init_db, load_log, update_log
from .pipeline.downloader import download_files
from .pipeline.exporter import export_to_csv


@click.command()
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
def main(region, all_regions, download, export_csv, output_dir, csv_file):
    """Process Swimming NZ community files: parse, log, download, export."""
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

    update_log(conn, regions, downloaded_files)

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


if __name__ == "__main__":
    main()
