# Python
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List

COLUMNS = [
    "region",
    "meet_name",
    "link",
    "meet_date",
    "meet_year",
    "location",
    "course",
    "processed_timestamp",
    "downloaded",
    "file_path",
    "uploaded",
    "processed_by_target",
]


def export_to_csv(regions: Dict[str, List[dict]], log_data, csv_file: str):
    """Export meet data to CSV, merging with DB log where relevant."""
    rows = []
    for region, meets in regions.items():
        for meet in meets:
            log_entry = log_data.get(region, {}).get(meet["meet_name"], {})
            meet_date = (
                _pretty_from_iso(meet["meet_date"])
                if meet.get("meet_date")
                else _pretty_from_iso(log_entry.get("meet_date"))
            )
            rows.append(
                {
                    "region": region,
                    "meet_name": meet["meet_name"],
                    "link": meet["link"],
                    "meet_date": meet_date,
                    "meet_year": meet.get("meet_year", log_entry.get("meet_year")),
                    "location": meet.get("location", log_entry.get("location")),
                    "course": meet.get("course", log_entry.get("course")),
                    "processed_timestamp": log_entry.get(
                        "processed_timestamp", datetime.now().isoformat()
                    ),
                    "downloaded": log_entry.get("downloaded", False),
                    "file_path": log_entry.get("file_path"),
                    "uploaded": log_entry.get("uploaded", False),
                    "processed_by_target": log_entry.get("processed_by_target", False),
                }
            )

    df = pd.DataFrame(rows)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[COLUMNS]

    csv_path = Path(csv_file)
    if csv_path.exists():
        existing = pd.read_csv(csv_path)
        for col in COLUMNS:
            if col not in existing.columns:
                existing[col] = None
        combined = pd.concat(
            [existing[COLUMNS], df], ignore_index=True
        ).drop_duplicates(subset=["region", "meet_name"], keep="last")
    else:
        combined = df

    combined.to_csv(csv_path, index=False)
    print(f"Exported data to {csv_path}")
