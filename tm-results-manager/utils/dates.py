# Python
import re
from pathlib import Path

# 02Nov2024, 08Aug2025 etc.
DATE_TOKEN_RE = re.compile(r"(0[1-9]|[12][0-9]|3[01])[A-Za-z]{3}\d{4}")
# Trailing -NNN code at the end of base filename
TRAILING_CODE_RE = re.compile(r"(.*?)-\d{3}$")


def extract_date_token(base_no_ext: str):
    """
    Find date tokens like 08Aug2025 in a base filename (no extension).
    Returns (date_token, year_int) or (None, None).
    """
    m = DATE_TOKEN_RE.search(base_no_ext)
    if not m:
        return None, None
    token = m.group(0)
    try:
        year = int(token[-4:])
    except ValueError:
        year = None
    return token, year


def base_name_without_ext_and_code(filename: str) -> str:
    """
    Remove extension and a trailing '-NNN' code if present.
    Returns the cleaned base name.
    """
    suffix = Path(filename).suffix
    base = filename[: -len(suffix)] if suffix else filename
    m = TRAILING_CODE_RE.match(base)
    return m.group(1) if m else base
