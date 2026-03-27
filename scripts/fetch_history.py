#!/usr/bin/env python3
"""
Fetch full inspection history (all past inspections + violations) for every
location in data/locations.json and save to data/history.json.

Safe to interrupt and resume — already-fetched locations are skipped.

Usage:
    nat-health/bin/python3 scripts/fetch_history.py

Options:
    --fetch-codes   Also fetch HTML inspection reports to get FDA violation codes
                    (much slower — roughly 3x more HTTP requests).
    --limit N       Only process N locations (useful for testing).

Output format (data/history.json):
    {
      "<location_id>": [
        {
          "date": "03-18-2026",
          "type": "Routine",
          "violations": [
            {
              "code": "4",
              "description": "Improper Hand Washing",
              "severity": "critical",
              "corrected": false
            }
          ]
        },
        ...
      ]
    }
"""

import base64
import json
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DATA_DIR    = Path(__file__).parent.parent / "data"
HISTORY_FILE = DATA_DIR / "history.json"
LOCATIONS_FILE = DATA_DIR / "locations.json"

INSPECTIONS_URL = "https://ri.healthinspections.us/ri/API/index.cfm/inspectionsData/{}"
HTML_BASE       = "https://ri.healthinspections.us/"

API_HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://ri.healthinspections.us/",
}

# ── Severity lookup (from FDA Food Code item numbers) ─────────────────────────
PRIORITY_ITEMS            = {4, 6, 8, 9, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 27, 28}
PRIORITY_FOUNDATION_ITEMS = {1, 3, 5, 10, 14, 25, 29}


def encode_id(numeric_id: str) -> str:
    return base64.b64encode(str(numeric_id).encode()).decode()


def fetch_json(url: str) -> list | dict | None:
    req = urllib.request.Request(url, headers=API_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception:
        raise


def fetch_html_codes(printable_path: str) -> list[str]:
    """Fetch the HTML inspection report and extract all FDA violation codes cited."""
    clean = re.sub(r'^\.\.?/', '', printable_path)
    # Percent-encode unsafe chars while preserving existing encoding and path structure
    safe_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&\'()*+,;=%')
    import urllib.parse
    safe = ''.join(c if c in safe_chars else urllib.parse.quote(c) for c in clean)
    url = HTML_BASE + safe
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": HTML_BASE})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        return re.findall(r"Violation of Code:.*?([\d]+-[\d]+\.[\d]+(?:\([A-Z]\))?)", html)
    except Exception:
        return []


def item_severity(item_num: int) -> str:
    if item_num in PRIORITY_ITEMS:
        return "critical"
    if item_num in PRIORITY_FOUNDATION_ITEMS:
        return "major"
    return "minor"


def parse_violations(violations_dict: dict, html_codes: list[str]) -> list[dict]:
    """
    Convert the API violations dict into our normalized violation list.

    violations_dict values are lists; element [0] is the violation text,
    e.g. "4 - Improper Hand Washing" or just a description string.

    If html_codes are provided, assign them positionally to violations.
    """
    result = []
    raw_items = [v[0] for v in violations_dict.values() if v and v[0]]

    for idx, text in enumerate(raw_items):
        parts = text.split(" - ", 1)
        if len(parts) == 2:
            try:
                item_num    = int(parts[0].strip())
                description = parts[1].strip().capitalize()
                severity    = item_severity(item_num)
                code        = html_codes[idx] if idx < len(html_codes) else str(item_num)
            except ValueError:
                description = text.strip().capitalize()
                severity    = "minor"
                code        = html_codes[idx] if idx < len(html_codes) else ""
        else:
            description = text.strip().capitalize()
            severity    = "minor"
            code        = html_codes[idx] if idx < len(html_codes) else ""

        result.append({
            "code":        code,
            "description": description,
            "severity":    severity,
            "corrected":   False,
        })

    return result


def fetch_location_history(loc_id: str, fetch_codes: bool) -> list[dict]:
    """Fetch all inspections for a single location."""
    url  = INSPECTIONS_URL.format(encode_id(loc_id))
    data = fetch_json(url)

    if not data:
        return []

    inspections = []
    for insp in data:
        raw_date = insp.get("columns", {}).get("0", "")
        date_str = raw_date.replace("Inspection Date:", "").strip() or None

        violations_dict = insp.get("violations", {})
        html_codes = []
        if fetch_codes:
            pp = insp.get("printablePath", "")
            if pp:
                html_codes = fetch_html_codes(pp)
                time.sleep(0.15)

        violations = parse_violations(violations_dict, html_codes)

        inspections.append({
            "date":       date_str,
            "type":       "Routine",
            "violations": violations,
        })

    return inspections


def main():
    fetch_codes = "--fetch-codes" in sys.argv
    limit       = None
    for i, arg in enumerate(sys.argv):
        if arg == "--limit" and i + 1 < len(sys.argv):
            limit = int(sys.argv[i + 1])

    if not LOCATIONS_FILE.exists():
        print(f"ERROR: {LOCATIONS_FILE} not found.")
        sys.exit(1)

    locations = json.loads(LOCATIONS_FILE.read_text())
    if limit:
        locations = locations[:limit]
    print(f"Loaded {len(locations)} locations")

    # Load existing history (checkpoint)
    history: dict = {}
    if HISTORY_FILE.exists():
        history = json.loads(HISTORY_FILE.read_text())
        print(f"Resuming — {len(history)} locations already fetched")

    todo = [loc for loc in locations if str(loc["id"]) not in history]
    print(f"{len(todo)} locations to fetch"
          f"{' (with HTML codes)' if fetch_codes else ''}\n")

    if not todo:
        print("Nothing to do.")
        return

    errors   = 0
    fetched  = 0
    SAVE_EVERY = 100

    for i, loc in enumerate(todo, 1):
        loc_id = str(loc["id"])
        name   = loc.get("name", loc_id)

        try:
            inspections = fetch_location_history(loc_id, fetch_codes)
            history[loc_id] = inspections
            fetched += 1
        except Exception as e:
            print(f"  ERROR {name}: {e}")
            history[loc_id] = []  # mark as attempted so we don't retry endlessly
            errors += 1

        # Rate limiting — be polite to the RI API
        time.sleep(0.4)

        if i % SAVE_EVERY == 0 or i == len(todo):
            HISTORY_FILE.write_text(json.dumps(history))
            total_inspections = sum(len(v) for v in history.values())
            total_violations  = sum(
                len(insp["violations"])
                for inspections in history.values()
                for insp in inspections
            )
            print(f"  [{i}/{len(todo)}] saved — "
                  f"{total_inspections} inspections, "
                  f"{total_violations} violations, "
                  f"{errors} errors")

    HISTORY_FILE.write_text(json.dumps(history))
    print(f"\nDone. {fetched} fetched, {errors} errors.")
    print(f"Saved to {HISTORY_FILE}")
    print("Now run: nat-health/bin/python3 scripts/import_ri.py")


if __name__ == "__main__":
    main()
