# TM Results Manager

TM Results Manager is a Python CLI tool that automates collecting, organizing, and parsing Swimming New Zealand meet result files from the Community Files page. It downloads HY3/ZIP result files, parses meet/team/swimmer metadata, and stores it in a local SQLite database for later querying and export.

Highlights:

-   Scrape Swimming NZ “Community Files” by region and discover meets
-   Download result archives to a region-based folder structure
-   Parse HY3 files (from ZIPs) to extract meet (name, dates, course), teams, and swimmers
-   De-duplicate across lifecycle stages (URL, file path, canonical meet name + start date)
-   Manual import and bulk-import of local files
-   CSV export of discovered/processed meets
-   Robust error logging and sequential parsing queue

---

## Installation

Requirements:

-   Python 3.9+
-   macOS, Linux, or Windows

Install dependencies:

```bash
pip install -r requirements.txt
```

Recommended layout:

-   Run commands from the project root.
-   The app uses a local SQLite database file (default: meets.db) in the current directory.
-   Downloads go to results/&lt;Region&gt;/ by default.

---

## Quick Start

Fetch, download, parse, and export:

Process all regions: discover meets, download files, enqueue parsing, parse queue, and export CSV

```bash
python -m tm_results_manager.cli run --all-regions --download --enqueue-parse
python -m tm_results_manager.cli parse-files --all-downloaded

```

Process only new (not-yet-downloaded) meets:

```bash
python -m tm_results_manager.cli run --process-new

```

Parse by region or by a specific meet:

```bash
python -m tm_results_manager.cli parse-files --region "Otago"
python -m tm_results_manager.cli parse-files --meet "PSC October 2025 Club Night"
```

Manual import (local ZIPs) from import_files:

Single file (pick from a list). Use --region to avoid a prompt.

```bash
python -m tm_results_manager.cli parse-files --manual --region "Waikato"
```

Bulk import all zips in import_files

```bash
python -m tm_results_manager.cli parse-files --manual-bulk --region "Waikato"
```

After each parse-files run, you’ll see a success/failure summary. Errors are also recorded in the error_log table.

---

## Commands

### run

Main workflow used to discover meets, optionally download and/or export, or process only new meets.

Options:

-   --region &lt;name&gt; Process only the specified region(s) (can be repeated)
-   --all-regions Process all regions
-   --download Download result files for the selected regions/meets
-   --enqueue-parse Enqueue newly downloaded files for parsing
-   --parse-now Parse all enqueued files
-   --export-csv Export a CSV snapshot of currently known meets
-   --output-dir &lt;dir&gt; Directory for downloads (default: results)
-   --csv-file &lt;file&gt; CSV output file (default: meet*results*&lt;YYYYMMDD&gt;.csv)
-   --process-new Download and parse meets where downloaded=0 (skips discovery)

Examples:

Discover and download selected region, enqueue for parsing, then parse

```bash
python -m tm_results_manager.cli run --region "Bay of Plenty" --download --enqueue-parse
python -m tm_results_manager.cli parse-files --all-downloaded
```

Process only new (not yet downloaded) meets across all regions

```bash
python -m tm_results_manager.cli run --process-new

```

### parse-files

Parse HY3 ZIPs into the database (meet/team/swimmer tables). Files are parsed sequentially via a queue.

Modes:

-   Downloaded files:

    -   --all-downloaded Parse all downloaded but unparsed meets
    -   --region &lt;name&gt; Parse downloaded meets for a specific region
    -   --meet "&lt;exact name&gt;" Parse a specific meet by exact name (must be downloaded)

-   Manual imports:
    -   --manual Import a single ZIP from import_files (you can add --region to avoid prompt)
    -   --manual-bulk Import all ZIPs from import_files (use --region to apply to all files)
    -   --keep-import Keep original ZIPs in import_files (otherwise moved to import_files/processed)

Examples:

Parse one region’s downloaded files

```bash
python -m tm_results_manager.cli parse-files --region "Otago"
```

Parse a specific downloaded meet

```bash
python -m tm_results_manager.cli parse-files --meet "PSC October 2025 Club Night"
```

Manual import from local ZIPs

```bash
python -m tm_results_manager.cli parse-files --manual --region "Waikato"
python -m tm_results_manager.cli parse-files --manual-bulk --region "Waikato"
```

---

## Data Flow

1. Discovery (web)

-   Scrape https://www.swimmingnz.org/community-files
-   Parse sections per region (“TM Results Files &lt;Region&gt;”)
-   Extract meet entries and initial date token from link filenames (via dn= parameter or URL path)
-   Store meets in DB keyed by URL; meet_date is stored in a readable “DD Mon YYYY” format when available

2. Download

-   Save ZIPs to results/&lt;Region&gt;/&lt;Meet Name - Date&gt;.zip
-   Deduplicate by URL and file path; skip already downloaded

3. Parse (HY3)

-   Unzip and read the .hy3 (warn if missing)
-   Extract:
    -   Meet metadata: meet_name (canonical), meet_date_start/end (saved as “DD Mon YYYY”), meet_year, course
    -   Teams: team_code, team_name, team_type, region_code, derived region (via models/region_codes.json)
    -   Swimmers: first_name, last_name, gender, birth_date (DDMMYYYY), mm_number, and team context
-   Upsert meet (overwrite meet_name and human-readable meet_date from file)
-   Insert/Upsert teams globally by (team_code, team_name)
-   Insert/Upsert swimmers globally by identity and link:
    -   meet_team (meet -&gt; teams competing)
    -   meet_swimmer (meet -&gt; swimmers who competed)
    -   meet_team_swimmer (meet -&gt; team -&gt; swimmer representation)

4. Export (optional)

-   CSV export of known meets (including web-derived fields and parsed fields)

---

## De-duplication Strategy

-   Pre-download: unique by URL (ux_meets_url)
-   Download/manual: unique by file_path (ux_meets_file_path)
-   Post-parse: canonical identity by meet_name + meet_date_start is used to detect potential duplicates (logged), while the DB row is updated in place

This layered approach avoids re-downloading, withstands meet_name changes after parsing, and keeps a consistent canonical identity once parsed.

---

## Database

SQLite schema (key tables):

-   meets: meet metadata (region label, url, file_path, meet_name, meet_date, meet_date_start, meet_date_end, meet_year, course, parsed)
-   teams: global team registry (team_code, team_name, team_type, region_code, region, address info)
-   swimmers: global swimmer registry (linked to teams via team_id; deduped by identity)
-   meet_team: link (meet_id, team_id)
-   meet_swimmer: link (meet_id, swimmer_id)
-   meet_team_swimmer: link (meet_id, team_id, swimmer_id)
-   parse_queue: sequential parse work items
-   error_log: structured error/warn entries from ingest (MissingHY3, LinkWarning, DuplicateMeet, etc.)

Regions are mapped via models/region_codes.json (code → canonical region name). Unknowns resolve to empty string.

---

## Manual Imports

Place ZIPs in import_files. Then run:

Single file, choose from list

```bash
python -m tm_results_manager.cli parse-files --manual --region "Waikato"
```

Bulk import everything in import_files for a single region

```bash
python -m tm_results_manager.cli parse-files --manual-bulk --region "Otago"
```

Behavior:

-   Files are copied into results/&lt;Region&gt;/ (same structure as downloads)
-   A placeholder meet row is inserted using the filename stem as a temporary meet_name
-   The file is enqueued and parsed immediately
-   Original ZIP is moved to import_files/processed (unless --keep-import is set)

---

## Error Handling and Logging

-   All parsing issues and link anomalies are recorded in error_log (with message and optional JSON context), and a short summary is printed after parse-files completes
-   The parser is sequential (one ZIP at a time) and uses a per-file temporary directory to avoid collisions
-   The database uses a busy_timeout and retry-on-lock for write operations

---

## Configuration

See config.py for basic settings:

-   URL: community page URL
-   DEFAULT_OUTPUT_DIR: default download/import target (results)
-   CSV_FILE: default export filename

Region mapping:

-   models/region_codes.json: reverse-mapped to resolve team region by region_code

---

## Development

Key modules:

-   cli.py: CLI entry point (Click group with run and parse-files subcommands)
-   parsing/community_page.py: HTML parser for community page meets
-   parsing/hy3_parser.py: HY3 ZIP and line parser
-   pipeline/downloader.py: file downloads
-   pipeline/ingest_results.py: end-to-end ingestion from ZIP → DB
-   storage/db.py: schema, migrations, upserts, linking, queue, error log
-   models/: JSON configs (hy3-results.json, region_codes.json)

---

## FAQ

-   Why does meet_name change after parsing?

-   We standardize meet_name from the HY3 file to avoid website naming inconsistencies. De-duplication uses URL pre-download and canonical name + date after parsing.

-   Why are meet_date_start/end stored as “DD Mon YYYY”?

-   For readability in the DB and CSV. meet_year is stored numerically for queries.

-   Can I import a ZIP manually without downloading from the website?
-   Yes. Use parse-files --manual or --manual-bulk. Files go through the same parsing pipeline.

---

## License

This project processes publicly available meet result files for organizational purposes. Ensure you comply with any relevant terms of use on Swimming NZ and related sites before redistributing data.

## Common SQL queries

### 1. Meet Discovery & Listing

Queries to find, filter, and list meets in various ways.

**List all meets (most recent first)**

```sql
SELECT id, region, meet_name, meet_date, meet_year, parsed, downloaded
FROM meets
ORDER BY COALESCE(meet_year, 0) DESC, meet_name;
```

**Meets in a specific region and year**

```sql
SELECT id, meet_name, meet_date_start AS start_date, meet_date_end AS end_date, meet_year
FROM meets
WHERE region = 'Bay of Plenty' AND meet_year = 2025
ORDER BY meet_date_start;
```

**Meets not yet downloaded (all regions)**

```sql
SELECT id, region, meet_name, url
FROM meets
WHERE downloaded = 0
ORDER BY region, meet_name;
```

**New (not downloaded) meets in a specific region**

```sql
SELECT id, meet_name, url
FROM meets
WHERE downloaded = 0 AND region = 'Wellington'
ORDER BY meet_name;
```

**Meets for a specific team**

```sql
SELECT m.meet_name, m.meet_date_start, m.meet_year
FROM meet_team mt
JOIN meets m ON m.id = mt.meet_id
JOIN teams t ON t.id = mt.team_id
WHERE t.team_code = 'ABC' AND t.team_name = 'Example Swim Club'
ORDER BY m.meet_year DESC, m.meet_name;
```

**Potential duplicate meets (same name + start date)**

```sql
SELECT meet_name, meet_date_start, COUNT(*) AS cnt
FROM meets
WHERE meet_date_start IS NOT NULL AND meet_name <> ''
GROUP BY meet_name, meet_date_start
HAVING COUNT(*) > 1
ORDER BY cnt DESC, meet_name;
```

### 2. Meet Participants

Who competed at a meet (teams or swimmers).

**Teams that competed at a given meet**

```sql
SELECT t.id, t.team_code, t.team_name, t.team_type, t.region
FROM meet_team mt
JOIN teams t ON t.id = mt.team_id
WHERE mt.meet_id = :meet_id
ORDER BY t.team_name;
```

**Swimmers in a specific meet (with their team)**

```sql
SELECT m.meet_name,
       m.meet_date_start,
       s.first_name, s.last_name, s.gender, s.birth_date, s.mm_number,
       t.team_name, t.team_type
FROM meet_team_swimmer mts
JOIN meets m   ON m.id = mts.meet_id
JOIN teams t   ON t.id = mts.team_id
JOIN swimmers s ON s.id = mts.swimmer_id
WHERE m.id = :meet_id
ORDER BY s.last_name, s.first_name;
```

### 3. Regional / Yearly Overviews

Big-picture views across a region or year.

**All teams that competed in a given year (distinct)**

```sql
SELECT DISTINCT t.team_code, t.team_name, t.team_type, t.region, m.meet_year
FROM meet_team mt
JOIN teams t ON t.id = mt.team_id
JOIN meets m ON m.id = mt.meet_id
WHERE m.meet_year = 2025
ORDER BY t.team_name;
```

**Swimmers across all meets in a region & year**

```sql
SELECT m.region, m.meet_name, m.meet_year,
       s.first_name, s.last_name, s.gender, s.birth_date, s.mm_number,
       t.team_name, t.team_type
FROM meet_team_swimmer mts
JOIN meets m   ON m.id = mts.meet_id
JOIN teams t   ON t.id = mts.team_id
JOIN swimmers s ON s.id = mts.swimmer_id
WHERE m.region = 'Otago' AND m.meet_year = 2025
ORDER BY m.meet_name, s.last_name, s.first_name;
```

**All swimmers representing a specific team in a given year**

```sql
SELECT s.first_name, s.last_name, s.gender, s.birth_date, s.mm_number,
       m.meet_name, m.meet_year, m.meet_date_start
FROM meet_team_swimmer mts
JOIN swimmers s ON s.id = mts.swimmer_id
JOIN teams t    ON t.id = mts.team_id
JOIN meets m    ON m.id = mts.meet_id
WHERE t.team_code = 'ABC' AND t.team_name = 'Example Swim Club' AND m.meet_year = 2025
ORDER BY m.meet_date_start, s.last_name, s.first_name;
```

### 4. Statistics & Aggregates

**Swimmer count per meet**

```sql
SELECT m.meet_name, COUNT(*) AS swimmer_count
FROM meet_swimmer ms
JOIN meets m ON m.id = ms.meet_id
GROUP BY ms.meet_id
ORDER BY swimmer_count DESC, m.meet_name;
```

**Team count per meet**

```sql
SELECT m.meet_name, COUNT(*) AS team_count
FROM meet_team mt
JOIN meets m ON m.id = mt.meet_id
GROUP BY mt.meet_id
ORDER BY team_count DESC, m.meet_name;
```

### 5. Team Data Quality & Management

**Teams with no region resolved**

```sql
SELECT id, team_code, team_name, region_code, region
FROM teams
WHERE region = '' OR region IS NULL
ORDER BY team_name;
```

**Region names and codes actually used by teams**

```sql
SELECT region, region_code, COUNT(*) AS teams
FROM teams
GROUP BY region, region_code
ORDER BY region, teams DESC;
```

### 6. Swimmer Data Quality

**Swimmers missing critical data (empty name or gender)**

```sql
SELECT id, first_name, last_name, gender, birth_date, mm_number
FROM swimmers
WHERE gender = '' OR first_name = '' OR last_name = '';
```

### 7. Sanity Checks

**Meets with no date after parsing**

```sql
SELECT id, region, meet_name, meet_date_start, meet_date_end, parsed
FROM meets
WHERE (meet_date_start IS NULL OR meet_date_start = '')
ORDER BY id DESC;
```

### 8. System / Admin Queries

**Recently parsed meets with errors (latest 50)**

```sql
SELECT e.timestamp, e.error_type, e.message, m.meet_name, m.file_path
FROM error_log e
LEFT JOIN meets m ON m.id = e.meet_id
ORDER BY e.id DESC
LIMIT 50;
```

**Parse queue status summary**

```sql
SELECT status, COUNT(*) AS cnt
FROM parse_queue
GROUP BY status;
```
