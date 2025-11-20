# Python
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import io
import json
import zipfile
import tempfile
from datetime import datetime

MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "hy3-results.json"
REGION_CODES_PATH = Path(__file__).resolve().parents[1] / "models" / "region_codes.json"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


MODEL = _load_json(MODEL_PATH)
REGION_CODES = _load_json(REGION_CODES_PATH) if REGION_CODES_PATH.exists() else {}


# Build reverse mapping: code -> region name
def _build_region_reverse_map(region_codes: dict) -> Dict[str, str]:
    reverse = {}
    for region_name, codes in region_codes.items():
        for code in codes:
            if code:
                reverse[str(code).strip().upper()] = region_name
    return reverse


REGION_BY_CODE = _build_region_reverse_map(REGION_CODES)


def _slice(line: str, start: Optional[int], length: Optional[int]) -> str:
    if start is None or length is None:
        return ""
    i = max(start - 1, 0)
    j = i + length
    return line[i:j].rstrip()


def _parse_date_token(s: str, fmt: str) -> Optional[str]:
    s = s.strip()
    if not s:
        return None
    fmts = {
        "MMDDYYYY": "%m%d%Y",
        "DDMMYYYY": "%d%m%Y",
        "YYYYMMDD": "%Y%m%d",
    }
    py_fmt = fmts.get(fmt)
    if not py_fmt:
        return None
    try:
        dt = datetime.strptime(s, py_fmt).date()
        return dt.isoformat()
    except ValueError:
        return None


def _reformat_date(date_iso: Optional[str], to_fmt: str) -> Optional[str]:
    if not date_iso:
        return None
    try:
        d = datetime.fromisoformat(date_iso).date()
        if to_fmt == "DDMMYYYY":
            return d.strftime("%d%m%Y")  # e.g., 12 Oct 2025
        if to_fmt == "MMDDYYYY":
            return d.strftime("%m%d%Y")
        if to_fmt == "YYYYMMDD":
            return d.strftime("%Y%m%d")
    except Exception:
        return None
    return None


def _detect_team_type_from_name(
    name: str, detection_map: Dict[str, str]
) -> Optional[str]:
    n = (name or "").lower()
    for needle, team_type in detection_map.items():
        if needle and needle.lower() in n:
            return team_type
    return detection_map.get("default")


def _apply_team_overrides(
    line: str, meet_type_code: str, team_spec: dict, detection_map: Dict[str, str]
) -> Tuple[str, str, str]:
    # Returns (team_code, region_code, team_type)
    overrides = (team_spec.get("overrides") or {}).get("meet_type", {})
    mt = meet_type_code or ""
    cfg = overrides.get(mt, overrides.get("fallback", {}))

    team_code = _slice(
        line,
        cfg.get("team_code", {}).get("start"),
        cfg.get("team_code", {}).get("length"),
    )
    region_code = _slice(
        line,
        cfg.get("region_code", {}).get("start"),
        cfg.get("region_code", {}).get("length"),
    )
    team_type = cfg.get("team_type", "Other")

    return team_code, region_code, team_type


def parse_hy3_lines(
    lines: List[str],
) -> Tuple[dict, List[dict], List[dict], List[dict]]:
    """
    Returns: (meet, teams, swimmers, warnings)
    - meet: dict with meet_name, meet_date_start (DDMMYYYY), meet_date_end (DDMMYYYY), meet_year, meet_type, course
    - teams: list of dicts
    - swimmers: list of dicts (includes team context: team_id, team_name)
    - warnings: list of warn dicts
    """
    meet = {}
    teams: List[dict] = []
    swimmers: List[dict] = []
    warnings: List[dict] = []

    meet_info_spec = MODEL["meet_info"]
    meet_ext_spec = MODEL.get("meet_info_extended", {})
    team_info_spec = MODEL["team_info"]
    team_ext_spec = MODEL.get("team_info_extended", {})
    swimmer_spec = MODEL["swimmer_info"]
    detection_map = MODEL.get("team_name_detection", {})

    meet_type_code = ""
    meet_type_map = meet_ext_spec.get("meet_type") or {}
    meet_type_fallback = meet_ext_spec.get("meet_type_fallback", "Other")
    course_map = meet_ext_spec.get("course") or {}

    current_team: Optional[dict] = None

    for raw in lines:
        line = raw.rstrip("\r\n")
        if not line:
            continue

        rec = line[:2]  # HY3 row_identifier are 2 chars in your JSON (B1,B2,C1,C2,D1)

        if rec == meet_info_spec["row_identifier"]:
            # Meet info core
            name = _slice(
                line, meet_info_spec["name"]["start"], meet_info_spec["name"]["length"]
            )
            location = _slice(
                line,
                meet_info_spec["location"]["start"],
                meet_info_spec["location"]["length"],
            )

            # Parse source dates as per model, then reformat to DDMMYYYY
            src_fmt = meet_info_spec.get("date_format", "MMDDYYYY")
            date_start_iso = _parse_date_token(
                _slice(
                    line,
                    meet_info_spec["meet_date_start"]["start"],
                    meet_info_spec["meet_date_start"]["length"],
                ),
                src_fmt,
            )
            date_end_iso = _parse_date_token(
                _slice(
                    line,
                    meet_info_spec["meet_date_end"]["start"],
                    meet_info_spec["meet_date_end"]["length"],
                ),
                src_fmt,
            )

            meet_date_start = _reformat_date(date_start_iso, "DDMMYYYY")
            meet_date_end = _reformat_date(date_end_iso, "DDMMYYYY")

            meet.update(
                {
                    "meet_name": name.strip(),
                    "location_text": location.strip(),
                    "meet_date_start": meet_date_start or "",
                    "meet_date_end": meet_date_end or "",
                    "meet_year": int(date_start_iso[:4]) if date_start_iso else None,
                }
            )

        elif rec == meet_ext_spec.get("row_identifier"):
            # Meet info extended
            mt_code = _slice(
                line,
                meet_ext_spec["meet_type_code"]["start"],
                meet_ext_spec["meet_type_code"]["length"],
            )
            meet_type_code = mt_code
            meet_type = meet_type_map.get(mt_code, meet_type_fallback)
            course_code = _slice(
                line,
                meet_ext_spec["course_code"]["start"],
                meet_ext_spec["course_code"]["length"],
            )
            course = course_map.get(course_code, "")

            meet.update(
                {
                    "meet_type_code": mt_code,
                    "meet_type": meet_type,
                    "course_code": course_code,
                    "course": course,
                }
            )

        elif rec == team_info_spec["row_identifier"]:
            team_name = _slice(
                line,
                team_info_spec["team_name"]["start"],
                team_info_spec["team_name"]["length"],
            ).strip()

            team_code, region_code, team_type = _apply_team_overrides(
                line, meet_type_code, team_info_spec, detection_map
            )

            # Safety override: if meet_type not 03/04 but name looks like school-ish, blank region_code
            name_type = _detect_team_type_from_name(team_name, detection_map)

            if name_type in ("High School", "College") and meet.get(
                "meet_type_code"
            ) not in ("03", "04"):
                region_code = ""

            # If meet type explicitly school types, blank region_code
            if meet.get("meet_type_code") in ("03", "04"):
                region_code = ""

            # Resolve region name via region_codes.json
            region_name = REGION_BY_CODE.get((region_code or "").strip().upper(), "")

            current_team = {
                "team_code": team_code,
                "team_name": team_name,
                "team_type": name_type or team_type,
                "region_code": region_code or "",
                "region": region_name,
            }
            teams.append(current_team)

        elif rec == team_ext_spec.get("row_identifier") and teams:
            # Optional; extend last team with extra info
            t = teams[-1]
            t["address_1"] = _slice(
                line,
                team_ext_spec["address_1"]["start"],
                team_ext_spec["address_1"]["length"],
            ).strip()
            t["address_2"] = _slice(
                line,
                team_ext_spec["address_2"]["start"],
                team_ext_spec["address_2"]["length"],
            ).strip()
            t["city"] = _slice(
                line, team_ext_spec["city"]["start"], team_ext_spec["city"]["length"]
            ).strip()
            t["postal_code"] = _slice(
                line,
                team_ext_spec["postal_code"]["start"],
                team_ext_spec["postal_code"]["length"],
            ).strip()
        elif rec == swimmer_spec["row_identifier"]:
            if not teams:
                warnings.append(
                    {
                        "type": "SwimmerWithoutTeam",
                        "message": "Encountered swimmer before any team",
                        "line": line[:50],
                    }
                )
                continue

            gender = _slice(
                line, swimmer_spec["gender"]["start"], swimmer_spec["gender"]["length"]
            ).strip()
            last_name = _slice(
                line,
                swimmer_spec["last_name"]["start"],
                swimmer_spec["last_name"]["length"],
            ).strip()
            first_name = _slice(
                line,
                swimmer_spec["first_name"]["start"],
                swimmer_spec["first_name"]["length"],
            ).strip()
            mm_number = _slice(
                line,
                swimmer_spec["mm_number"]["start"],
                swimmer_spec["mm_number"]["length"],
            ).strip()
            birth_date_src = _slice(
                line,
                swimmer_spec["birth_date"]["start"],
                swimmer_spec["birth_date"]["length"],
            ).strip()
            birth_fmt = swimmer_spec.get("birth_date_format", "MMDDYYYY")
            birth_iso = _parse_date_token(birth_date_src, birth_fmt)
            birth_ddmmyyyy = _reformat_date(birth_iso, "DDMMYYYY") if birth_iso else ""

            swimmers.append(
                {
                    "first_name": first_name,
                    "last_name": last_name,
                    "gender": gender,
                    "birth_date": birth_ddmmyyyy,  # stored as DDMMYYYY text
                    "mm_number": mm_number,
                    # team context
                    "team_code": teams[-1]["team_code"],
                    "team_name": teams[-1]["team_name"],
                    "team_type": teams[-1]["team_type"],
                    "region_code": teams[-1]["region_code"],
                    "region": teams[-1]["region"],
                }
            )

        else:
            # Ignore other rows for now
            continue

    return meet, teams, swimmers, warnings


def parse_hy3_zip(
    zip_path: Path,
) -> Tuple[Optional[dict], List[dict], List[dict], List[dict]]:
    """
    Extracts and parses a HY3 zip.
    Returns (meet, teams, swimmers, warnings); meet=None if .hy3 missing.
    """
    warnings: List[dict] = []
    p = Path(zip_path)
    if not p.exists():
        return None, [], [], [{"type": "FileNotFound", "message": str(zip_path)}]

    with zipfile.ZipFile(p, "r") as zf:
        names = zf.namelist()
        hy3_files = [n for n in names if n.lower().endswith(".hy3")]
        cl2_files = [n for n in names if n.lower().endswith(".cl2")]

        if not hy3_files:
            warnings.append(
                {
                    "type": "MissingHY3",
                    "message": "Zip does not contain .hy3",
                    "files": names,
                }
            )
            return None, [], [], warnings

        if len(hy3_files) > 1:
            warnings.append(
                {
                    "type": "MultipleHY3",
                    "message": "Multiple .hy3 files found; parsing first",
                    "files": hy3_files,
                }
            )

        if not cl2_files:
            warnings.append(
                {"type": "MissingCL2", "message": "Zip does not contain .cl2"}
            )

        hy3_name = hy3_files[0]

        with tempfile.TemporaryDirectory(prefix="hy3_") as tmpdir:
            tmp = Path(tmpdir)
            # Extract only the chosen hy3 and optional cl2 for debugging
            zf.extract(hy3_name, tmp)
            if cl2_files:
                zf.extract(cl2_files[0], tmp)

            hy3_path = tmp / hy3_name
            content = hy3_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            meet, teams, swimmers, warn2 = parse_hy3_lines(content)
            warnings.extend(warn2)

            return meet, teams, swimmers, warnings
