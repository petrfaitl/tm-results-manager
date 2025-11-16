# Python
from pathlib import Path
import requests
from typing import Dict, List
from ..http import extract_filename_from_response_or_url
from ..utils.dates import extract_date_token, base_name_without_ext_and_code


def download_files(regions: Dict[str, List[dict]], output_dir: str, log_data) -> dict:
    """Download result files, appending date token to filenames when available."""
    out = Path(output_dir)
    out.mkdir(exist_ok=True)
    downloaded = {}

    for region, meets in regions.items():
        region_path = out / region.replace("/", "_")
        region_path.mkdir(exist_ok=True)

        for meet in meets:
            meet_name = meet["meet_name"]

            if log_data.get(region, {}).get(meet_name, {}).get("downloaded"):
                continue

            url = meet["link"]
            try:
                resp = requests.get(url, stream=True)
                resp.raise_for_status()

                orig_filename = extract_filename_from_response_or_url(resp, url)
                orig_suffix = Path(orig_filename).suffix or ".zip"

                # Prefer parsed date; otherwise try from the response filename
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
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                downloaded[meet_name] = str(file_path)
                print(f"Downloaded: {meet_name} to {file_path}")
            except requests.RequestException as e:
                print(f"Error downloading {meet_name}: {e}")

    return downloaded
