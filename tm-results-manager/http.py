# Python
import re
import requests
from pathlib import Path
from urllib.parse import urlparse, unquote, parse_qs


def fetch_page(url: str, timeout: float = 20.0) -> str | None:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"Error fetching page: {e}")
        return None


def infer_filename_from_url(url: str) -> str:
    """
    Best-effort filename inference using dn= query param when present;
    otherwise from URL path basename. Falls back to 'download.zip'.
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "dn" in qs and qs["dn"]:
        name = unquote(qs["dn"][0])
        if name:
            return name
    name = unquote(Path(parsed.path).name)
    return name or "download.zip"


def extract_filename_from_response_or_url(response, url: str) -> str:
    """
    Try Content-Disposition filename, otherwise fall back to URL path/dn.
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
