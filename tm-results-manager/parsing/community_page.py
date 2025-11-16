# Python
from bs4 import BeautifulSoup
from typing import Dict, List
from ..http import infer_filename_from_url
from ..utils.dates import extract_date_token, base_name_without_ext_and_code

PREFIX = "TM Results Files"


def parse_meets(html: str) -> Dict[str, List[dict]]:
    """
    Parse meets and regions from the Swimming NZ community page.
    Scoped by h3 sections to avoid cross-region leakage.
    """
    soup = BeautifulSoup(html, "html.parser")
    regions: Dict[str, List[dict]] = {}
    containers = soup.find_all(["section"])
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

            # Early date extraction from dn/path, stripping trailing -NNN
            orig_filename = infer_filename_from_url(link)
            base_no_ext = base_name_without_ext_and_code(orig_filename)
            date_token, year_int = extract_date_token(base_no_ext)

            regions[region_name].append(
                {
                    "meet_name": meet_name,
                    "link": link,
                    "meet_date": date_token,
                    "meet_year": year_int,
                    "location": None,
                    "course": None,
                }
            )

    return regions
