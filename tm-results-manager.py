import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
from pathlib import Path
import click
from datetime import datetime
import re
from urllib.parse import urlparse, unquote, parse_qs

# Constants
URL = "https://www.swimmingnz.org/community-files"
DB_FILE = "meets.db"
DEFAULT_OUTPUT_DIR = "results"
CSV_FILE = f"meet_results_{datetime.now().strftime('%Y%m%d')}.csv"


# Python
def init_db():
    """Initialize SQLite database and table."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
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

    # One-time migration for existing DBs missing the new columns
    def add_col(name, type_):
        try:
            cursor.execute(f"ALTER TABLE meets ADD COLUMN {name} {type_}")
        except sqlite3.OperationalError:
            pass  # column already exists

    add_col("meet_date", "TEXT")
    add_col("meet_year", "INTEGER")
    add_col("location", "TEXT")
    add_col("course", "TEXT")

    conn.commit()
    return conn


def fetch_page(url):
    """Fetch webpage content."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return None


# Python
DATE_TOKEN_RE = re.compile(r"(0[1-9]|[12][0-9]|3[01])[A-Za-z]{3}\d{4}")


# Python
def parse_meets(html):
    """Parse meets and regions from HTML by processing section-by-section."""
    soup = BeautifulSoup(html, "html.parser")
    regions = {}

    containers = soup.find_all(["section", "article", "div"])
    PREFIX = "TM Results Files"

    for container in containers:
        h3 = container.find("h3")
        if not h3:
            continue

        heading = h3.get_text(strip=True)
        if "TM Results Files" not in heading:
            continue

        region_name = heading
        for candidate in (f"{PREFIX} ", PREFIX):
            if region_name.startswith(candidate):
                region_name = region_name[len(candidate) :].strip()
                break

        if not region_name:
            continue

        regions.setdefault(region_name, [])

        for a in container.find_all("a"):
            text = a.get_text(strip=True)
            if text != "TM File":
                continue

            link = a.get("href")
            if not link:
                continue

            # Find meet name (same logic as before)
            meet_name = None
            parent = a
            for _ in range(4):
                if not parent or parent == container:
                    break
                parent = parent.parent
                if parent:
                    h1 = parent.find("h1")
                    if h1 and h1.get_text(strip=True):
                        meet_name = h1.get_text(strip=True)
                        break
            if not meet_name:
                card = a.find_parent(["article", "div", "section"])
                if card and card != container:
                    h1 = card.find("h1")
                    if h1 and h1.get_text(strip=True):
                        meet_name = h1.get_text(strip=True)
            if not meet_name:
                continue

            # Heuristic: try to extract a date from the URL path (may or may not be present)
            orig_filename = infer_filename_from_url(link)
            base_no_ext = base_name_without_ext_and_code(orig_filename)
            date_token, year_int = extract_date_token(base_no_ext)

            regions[region_name].append(
                {
                    "meet_name": meet_name,
                    "link": link,
                    "meet_date": date_token,  # likely None unless the URL contains it
                    "meet_year": year_int,
                    "location": None,
                    "course": None,
                }
            )

    return regions


# Python
def load_log(conn):
    """Load existing meets from SQLite."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT region, meet_name, url, processed_timestamp, downloaded, file_path,
               uploaded, processed_by_target, meet_date, meet_year, location, course
        FROM meets
        """
    )
    rows = cursor.fetchall()
    log_data = {}
    for row in rows:
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
        }
    return log_data


# Python
def update_log(conn, regions, downloaded_files=None):
    """Update SQLite log with new meets and download status."""
    if downloaded_files is None:
        downloaded_files = {}

    cursor = conn.cursor()
    current_time = datetime.now().isoformat()

    for region, meets in regions.items():
        for meet in meets:
            meet_name = meet["meet_name"]
            file_path = downloaded_files.get(meet_name)
            downloaded = file_path is not None

            meet_date = meet.get("meet_date")
            meet_year = meet.get("meet_year")
            location = meet.get("location")
            course = meet.get("course")

            # Python
            cursor.execute(
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
                    current_time,
                    downloaded,
                    file_path,
                    False,  # only used on initial insert
                    False,  # only used on initial insert
                    meet_date,
                    meet_year,
                    location,
                    course,
                ),
            )

    conn.commit()


def export_to_csv(regions, log_data, csv_file):
    """Export meet data to CSV, merging with log data."""
    data = []
    for region, meets in regions.items():
        for meet in meets:
            meet_name = meet["meet_name"]
            log_entry = log_data.get(region, {}).get(meet_name, {})
            data.append(
                {
                    "region": region,
                    "meet_name": meet_name,
                    "link": meet["link"],
                    "meet_date": meet.get("meet_date", log_entry.get("meet_date")),
                    "meet_year": meet.get("meet_year", log_entry.get("meet_year")),
                    "location": meet.get("location", log_entry.get("location")),
                    "course": meet.get("course", log_entry.get("course")),
                    "processed_timestamp": log_entry.get(
                        "processed_timestamp", datetime.now().isoformat()
                    ),
                    "downloaded": log_entry.get("downloaded", False),
                    "file_path": log_entry.get("file_path", None),
                    "uploaded": log_entry.get("uploaded", False),
                    "processed_by_target": log_entry.get("processed_by_target", False),
                }
            )

    df = pd.DataFrame(data)

    # Append to existing CSV if it exists, avoiding duplicates
    csv_path = Path(csv_file)
    if csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        combined_df = pd.concat([existing_df, df]).drop_duplicates(
            subset=["region", "meet_name"], keep="last"
        )
    else:
        combined_df = df

    combined_df.to_csv(csv_path, index=False)
    print(f"Exported data to {csv_path}")


def download_files(regions, output_dir, log_data):
    """Download result files for specified regions, appending date from source filename if present."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    downloaded_files = {}

    for region, meets in regions.items():
        region_path = output_path / region.replace("/", "_")
        region_path.mkdir(exist_ok=True)

        for meet in meets:
            meet_name = meet["meet_name"]

            if (
                region in log_data
                and meet_name in log_data[region]
                and log_data[region][meet_name]["downloaded"]
            ):
                continue

            url = meet["link"]
            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()

                orig_filename = extract_filename_from_response_or_url(response, url)
                # Prefer already-parsed date; otherwise extract from normalized base
                date_token = meet.get("meet_date")
                if not date_token:
                    base_no_ext = base_name_without_ext_and_code(orig_filename)
                    date_token, _ = extract_date_token(base_no_ext)

                safe_meet = meet_name.replace("/", "_")
                target_name = (
                    f"{safe_meet} - {date_token}{orig_suffix}"
                    if date_token
                    else f"{safe_meet}{orig_suffix}"
                )
                file_path = region_path / target_name

                with file_path.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                downloaded_files[meet_name] = str(file_path)
                print(f"Downloaded: {meet_name} to {file_path}")
            except requests.RequestException as e:
                print(f"Error downloading {meet_name}: {e}")

    return downloaded_files


def extract_date_token(base_no_ext: str):
    """
    Find date tokens like 08Aug2025 in a base filename (no extension).
    Returns (date_token, year_int) or (None, None).
    """
    m = DATE_TOKEN_RE.search(base_no_ext)
    if not m:
        return None, None
    token = m.group(0)
    # Last 4 characters are the year
    try:
        year = int(token[-4:])
    except ValueError:
        year = None
    return token, year


def infer_filename_from_url(url: str) -> str:
    """
    Best-effort filename inference using dn= query param when present;
    otherwise from URL path basename. Falls back to 'download.zip'.
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    # dn often contains the true filename, e.g. Meet Results-Name-02Nov2024-001.zip
    if "dn" in qs and qs["dn"]:
        # parse_qs returns a list per key
        name = unquote(qs["dn"][0])
        if name:
            return name
    name = unquote(Path(parsed.path).name)
    return name or "download.zip"


def extract_filename_from_response_or_url(response, url: str) -> str:
    """
    Try Content-Disposition filename, otherwise fall back to URL path basename.
    """
    cd = response.headers.get("Content-Disposition") or response.headers.get(
        "content-disposition"
    )
    if cd:
        m = re.search(
            r'filename\*?=(?:UTF-8\'\')?"?([^\";]+)"?', cd, flags=re.IGNORECASE
        )
        if m:
            return unquote(m.group(1))
    return infer_filename_from_url(url)


TRAILING_CODE_RE = re.compile(r"(.*?)-\d{3}$")  # captures everything before -NNN


def base_name_without_ext_and_code(filename: str) -> str:
    """
    Remove extension and a trailing '-NNN' code if present.
    Returns the cleaned base name.
    """
    suffix = Path(filename).suffix
    base = filename[: -len(suffix)] if suffix else filename
    m = TRAILING_CODE_RE.match(base)
    return m.group(1) if m else base


@click.command()
@click.option(
    "--region",
    multiple=True,
    help="Specify region(s) to process (e.g., Waikato, National & International)",
)
@click.option("--all-regions", is_flag=True, help="Process all regions")
@click.option("--download", is_flag=True, help="Download result files")
@click.option("--export-csv", is_flag=True, help="Export results to CSV")
@click.option(
    "--output-dir",
    default=DEFAULT_OUTPUT_DIR,
    help="Directory to save downloaded files",
)
@click.option("--csv-file", default=CSV_FILE, help="Output CSV file name")
def main(region, all_regions, download, export_csv, output_dir, csv_file):
    """Process Swimming NZ meet results, log to SQLite, export to CSV, and optionally download files."""
    html = fetch_page(URL)
    if not html:
        return

    conn = init_db()
    regions = parse_meets(html)
    log_data = load_log(conn)

    # Filter regions if specified
    if not all_regions and region:
        regions = {r: regions[r] for r in region if r in regions}

    # Download files if requested
    downloaded_files = {}
    if download:
        downloaded_files = download_files(regions, output_dir, log_data)

    # Update log
    update_log(conn, regions, downloaded_files)

    # Export to CSV if requested
    if export_csv:
        export_to_csv(regions, log_data, csv_file)

    # Print results
    for region, meets in regions.items():
        print(f"\nRegion: {region}")
        for meet in meets:
            md = meet.get("meet_date")
            yr = meet.get("meet_year")
            loc = meet.get("location")
            crs = meet.get("course")
            print(
                f"  Meet: {meet['meet_name']}, Link: {meet['link']}, Date: {md}, Year: {yr}, Location: {loc}, Course: {crs}"
            )

    conn.close()


if __name__ == "__main__":
    main()
