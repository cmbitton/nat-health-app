#!/usr/bin/env python3
"""
Refresh data/locations.json with latest inspection data from the RI health inspections API.

Phases:
  1. Incremental update — fetch new/re-inspected facilities from the top of the API
     (most recent first) and stop once we hit a full page of already-current records.
     New locations are fully enriched: geocoded (Google), scored, and classified.
  2. Backfill scores — populate violation_count/risk_score for any location still missing it
     (only needed on the very first run after these fields were added).
  3. Backfill classification — populate google_category/cuisine for any location missing it
     (catches anything added before fetch_cuisines.py was run, or interrupted runs).

Usage:
  GOOGLE_MAPS_KEY=your-key python3 scripts/refresh.py
"""

import base64
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from place_types import CATEGORY_TYPE_MAP, CUISINE_TYPE_MAP  # noqa: E402

DATA_FILE   = Path(__file__).parent.parent / "data" / "locations.json"
FACILITIES  = "https://ri.healthinspections.us/ri/API/index.cfm/facilities/{}/0"
INSPECTIONS = "https://ri.healthinspections.us/ri/API/index.cfm/inspectionsData/{}"
SEARCH_URL  = "https://ri.healthinspections.us/ri/API/index.cfm/search/{}/{}"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}"
PLACES_URL  = "https://places.googleapis.com/v1/places:searchText"
HTML_BASE   = "https://ri.healthinspections.us/"

LOOKBACK_DAYS = 4  # how many days back to search for updated inspections

GOOGLE_KEY = os.environ.get("GOOGLE_MAPS_KEY")
if not GOOGLE_KEY:
    raise SystemExit("GOOGLE_MAPS_KEY environment variable not set.")

API_HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "Mozilla/5.0",
    "Referer":    "https://ri.healthinspections.us/",
}


# ── Violation severity weights (FDA Food Code 2022) ───────────────────────────
# Extracted from Annex 7 of the FDA Food Code 2022 PDF.
# Subsection-specific entries (e.g. "4-601.11(A)") take precedence over base entries.
# code_weight() falls back to the base code if no subsection entry exists.
# P = Priority (weight 3), Pf = Priority Foundation (weight 2), C = Core (weight 1)
CODE_SEVERITY = {
    "2-101.11": "Pf", "2-102.11": "Pf", "2-102.12": "C",  "2-103.11": "Pf",
    "2-201.11": "P",  "2-201.12": "P",  "2-201.13": "P",
    "2-301.11": "P",  "2-301.12": "P",  "2-301.14": "P",  "2-301.15": "Pf",
    "2-301.16": "Pf", "2-302.11": "Pf", "2-303.11": "C",  "2-304.11": "C",
    "2-401.11": "C",  "2-401.12": "C",  "2-401.13": "C",  "2-402.11": "C",
    "2-403.11": "Pf", "2-501.11": "Pf",
    "3-101.11": "P",
    "3-201.11": "P",  "3-201.12": "P",  "3-201.13": "P",  "3-201.14": "P",
    "3-201.15": "P",  "3-201.16": "P",  "3-201.17": "P",
    "3-202.11": "P",  "3-202.110": "P", "3-202.12": "P",  "3-202.13": "P",
    "3-202.14": "P",  "3-202.15": "Pf", "3-202.16": "P",  "3-202.17": "C",
    "3-202.18": "Pf", "3-203.11": "C",  "3-203.12": "Pf",
    "3-301.11": "P",  "3-301.12": "P",
    "3-302.11": "P",  "3-302.12": "C",  "3-302.13": "P",  "3-302.14": "P",
    "3-302.15": "Pf", "3-303.11": "P",  "3-303.12": "C",
    "3-304.11": "P",  "3-304.12": "C",  "3-304.13": "C",  "3-304.14": "C",
    "3-304.15": "P",  "3-304.16": "C",  "3-304.17": "P",
    "3-305.11": "C",  "3-305.12": "Pf", "3-305.13": "C",  "3-305.14": "C",
    "3-306.11": "P",  "3-306.12": "C",  "3-306.13": "P",  "3-306.14": "P",
    "3-307.11": "C",
    "3-401.11": "P",  "3-401.12": "P",  "3-401.13": "Pf", "3-401.14": "P",
    "3-401.15": "P",  "3-402.11": "P",  "3-402.12": "Pf", "3-403.11": "P",
    "3-404.11": "P",
    "3-501.11": "C",  "3-501.12": "C",  "3-501.13": "Pf", "3-501.14": "P",
    "3-501.15": "Pf", "3-501.16": "P",  "3-501.17": "Pf", "3-501.18": "P",
    "3-501.19": "P",  "3-502.11": "Pf", "3-502.12": "P",
    "3-601.11": "C",  "3-601.12": "C",  "3-602.11": "Pf", "3-602.12": "C",  "3-603.11": "Pf",
    "3-701.11": "P",  "3-801.11": "P",
    "4-101.11": "P",  "4-101.12": "C",  "4-101.13": "P",  "4-101.14": "P",
    "4-101.15": "P",  "4-101.16": "C",  "4-101.17": "C",  "4-101.18": "C",
    "4-101.19": "C",  "4-102.11": "P",  "4-201.11": "C",  "4-201.12": "P",
    "4-202.11": "Pf", "4-202.12": "Pf", "4-202.13": "C",  "4-202.14": "C",
    "4-202.15": "C",  "4-202.16": "C",  "4-202.17": "C",  "4-202.18": "C",
    "4-203.11": "Pf", "4-203.12": "Pf", "4-203.13": "C",
    "4-204.11": "C",  "4-204.110": "P", "4-204.111": "P", "4-204.112": "Pf", "4-204.113": "C",
    "4-204.114": "C", "4-204.115": "Pf","4-204.116": "Pf","4-204.117": "Pf",
    "4-204.118": "C", "4-204.119": "C", "4-204.12": "C",  "4-204.120": "C",
    "4-204.121": "C", "4-204.122": "C", "4-204.123": "C", "4-204.13": "P",
    "4-204.14": "C",  "4-204.15": "C",  "4-204.16": "C",  "4-204.17": "C",
    "4-204.18": "C",  "4-204.19": "C",
    "4-301.11": "Pf", "4-301.12": "Pf", "4-301.13": "C",  "4-301.14": "C",
    "4-301.15": "C",  "4-302.11": "Pf", "4-302.12": "Pf", "4-302.13": "Pf",
    "4-302.14": "Pf", "4-303.11": "Pf", "4-401.11": "Pf", "4-402.11": "C",  "4-402.12": "C",
    "4-501.11": "C",  "4-501.110": "Pf","4-501.111": "P", "4-501.112": "Pf",
    "4-501.113": "C", "4-501.114": "P",  "4-501.115": "C", "4-501.116": "Pf","4-501.12": "C",
    "4-501.13": "C",  "4-501.14": "C",  "4-501.15": "C",  "4-501.16": "C",
    "4-501.17": "Pf", "4-501.18": "C",  "4-501.19": "Pf",
    "4-502.11": "Pf", "4-502.12": "P",  "4-502.13": "C",  "4-502.14": "C",
    "4-601.11": "Pf", "4-602.11": "P",  "4-602.12": "C",  "4-602.13": "C",
    "4-603.11": "C",  "4-603.12": "C",  "4-603.13": "C",  "4-603.14": "C",
    "4-603.15": "C",  "4-603.16": "C",  "4-702.11": "P",  "4-703.11": "P",
    "4-801.11": "C",  "4-802.11": "C",  "4-803.11": "C",  "4-803.12": "C",
    "4-803.13": "C",  "4-901.11": "C",  "4-901.12": "C",  "4-902.11": "C",
    "4-902.12": "C",  "4-903.11": "C",  "4-903.12": "Pf", "4-904.11": "C",  "4-904.12": "C",
    "4-904.13": "C",  "4-904.14": "C",
    "5-101.11": "P",  "5-101.12": "P",  "5-101.13": "P",
    "5-102.11": "P",  "5-102.12": "P",  "5-102.13": "Pf", "5-102.14": "C",
    "5-103.11": "Pf", "5-103.12": "Pf", "5-104.11": "Pf", "5-104.12": "Pf",
    "5-201.11": "P",
    "5-202.11": "P",  "5-202.12": "Pf", "5-202.13": "P",  "5-202.14": "P",
    "5-202.15": "C",  "5-203.11": "Pf", "5-203.12": "C",  "5-203.13": "C",
    "5-203.14": "P",  "5-203.15": "P",  "5-204.11": "Pf", "5-204.12": "C",
    "5-204.13": "C",  "5-205.11": "Pf", "5-205.12": "P",  "5-205.13": "Pf",
    "5-205.14": "P",  "5-205.15": "P",
    "5-301.11": "P",  "5-302.11": "C",  "5-302.12": "C",  "5-302.13": "C",
    "5-302.14": "C",  "5-302.15": "C",  "5-302.16": "P",  "5-303.11": "P",
    "5-303.12": "C",  "5-303.13": "C",  "5-304.11": "P",  "5-304.12": "C",
    "5-304.13": "C",  "5-304.14": "P",
    "5-401.11": "C",  "5-402.11": "P",  "5-402.12": "C",  "5-402.13": "P",
    "5-402.14": "Pf", "5-402.15": "C",  "5-403.11": "P",  "5-403.12": "C",
    "5-501.11": "C",  "5-501.110": "C", "5-501.111": "C", "5-501.112": "C",
    "5-501.113": "C", "5-501.114": "C", "5-501.115": "C", "5-501.116": "C",
    "5-501.12": "C",  "5-501.13": "C",  "5-501.14": "C",  "5-501.15": "C",
    "5-501.16": "C",  "5-501.17": "C",  "5-501.18": "C",  "5-501.19": "C",  "5-502.11": "C",
    "5-502.12": "C",  "5-503.11": "C",
    "6-101.11": "C",  "6-102.11": "C",
    "6-201.11": "C",  "6-201.12": "C",  "6-201.13": "C",  "6-201.14": "C",
    "6-201.15": "C",  "6-201.16": "C",  "6-201.17": "C",  "6-201.18": "C",
    "6-202.11": "C",  "6-202.110": "C", "6-202.111": "P", "6-202.112": "C",
    "6-202.12": "C",  "6-202.13": "C",  "6-202.14": "C",  "6-202.15": "C",
    "6-202.16": "C",  "6-202.17": "C",  "6-202.18": "C",  "6-202.19": "C",
    "6-301.11": "Pf", "6-301.12": "Pf", "6-301.13": "C",  "6-301.14": "C",
    "6-302.11": "Pf", "6-303.11": "C",  "6-304.11": "C",  "6-305.11": "C",
    "6-402.11": "C",  "6-403.11": "C",  "6-404.11": "Pf",
    "6-501.11": "C",  "6-501.110": "C", "6-501.111": "Pf","6-501.112": "C",
    "6-501.113": "C", "6-501.114": "C", "6-501.115": "Pf","6-501.12": "C",
    "6-501.13": "C",  "6-501.14": "C",  "6-501.15": "Pf", "6-501.16": "C",
    "6-501.17": "C",  "6-501.18": "C",  "6-501.19": "C",
    "7-101.11": "Pf", "7-102.11": "Pf", "7-201.11": "P",  "7-202.11": "Pf",
    "7-202.12": "P",  "7-203.11": "P",  "7-204.11": "P",  "7-204.12": "P",  "7-204.13": "P",
    "7-204.14": "P",  "7-205.11": "P",  "7-206.11": "P",  "7-206.12": "P",
    "7-206.13": "P",  "7-207.11": "P",  "7-207.12": "P",  "7-208.11": "P",
    "7-209.11": "C",  "7-301.11": "P",
    "8-103.11": "Pf", "8-103.12": "P",  "8-201.13": "C",  "8-201.14": "Pf",
    # Subsection overrides — sections where (A)/(B)/(C)/... have different severity levels.
    # Base entries above are kept at the highest severity as a fallback for unlettered citations.
    "2-201.11(B)": "Pf",  "2-201.11(E)": "Pf",
    "3-202.11(E)": "Pf",  "3-202.11(F)": "Pf",
    "3-202.110(A)": "Pf",
    "3-203.11(B)": "Pf",
    "3-301.11(C)": "Pf",
    "3-304.15(A)": "P",   "3-304.15(B)": "C",   "3-304.15(C)": "C",   "3-304.15(D)": "C",
    "3-306.13(A)": "P",   "3-306.13(B)": "Pf",  "3-306.13(C)": "Pf",
    "3-401.12(A)": "C",   "3-401.12(B)": "C",   "3-401.12(C)": "P",   "3-401.12(D)": "C",
    "3-401.14(F)": "Pf",
    "3-404.11(A)": "P",   "3-404.11(B)": "Pf",
    "3-501.19(A)": "Pf",
    "3-502.12(B)": "Pf",
    "3-801.11(G)": "C",
    "6-501.111(A)": "C",  "6-501.111(B)": "C",  "6-501.111(D)": "C",
    "5-205.12(B)": "Pf",
    "7-202.12(C)": "Pf",
    "7-207.11(A)": "Pf",
    "7-208.11(A)": "Pf",
    "8-103.12(A)": "Pf",
    "4-204.110(A)": "P",  "4-204.110(B)": "Pf",
    "4-401.11(C)": "C",
    "4-502.11(A)": "C",   "4-502.11(B)": "Pf",  "4-502.11(C)": "C",
    "4-601.11(A)": "Pf",  "4-601.11(B)": "C",   "4-601.11(C)": "C",
}

# Fallback: item-number severity (used when HTML report has no parseable code sections)
# Items not listed default to Core (weight 1)
PRIORITY_ITEMS            = {4, 6, 8, 9, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 27, 28}
PRIORITY_FOUNDATION_ITEMS = {1, 3, 5, 10, 14, 25, 29}

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_json(url, headers=API_HEADERS):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())

def decode_id(b64_str):
    padded = b64_str + "=" * (-len(b64_str) % 4)
    return base64.b64decode(padded).decode()

def encode_id(numeric_id):
    return base64.b64encode(str(numeric_id).encode()).decode()

def parse_facility(item):
    return {
        "id":              decode_id(item["id"]),
        "name":            item.get("name", ""),
        "address":         item.get("mapAddress", ""),
        "last_inspection": item.get("columns", {}).get("1", "").replace("Last Inspection Date:", "").strip(),
        "license_type":    item.get("columns", {}).get("2", "").replace("License Type: ", "").strip(),
    }

def violation_weight(violation_str):
    """Fallback: weight by item number when code sections aren't available."""
    try:
        item = int(violation_str.split(" - ")[0].strip())
        if item in PRIORITY_ITEMS:            return 3
        if item in PRIORITY_FOUNDATION_ITEMS: return 2
        return 1
    except (ValueError, IndexError):
        return 1

def code_weight(code):
    severity = CODE_SEVERITY.get(code)
    if severity is None:
        # Fall back to base code (strip subsection letter e.g. "(A)")
        base = re.sub(r'\([A-Z]\)$', '', code)
        severity = CODE_SEVERITY.get(base)
    if severity == "P":  return 3
    if severity == "Pf": return 2
    return 1

def fetch_report_codes(printable_path):
    """Fetch the HTML inspection report and return all FDA code sections cited."""
    clean = re.sub(r'^\.\.?/', '', printable_path)
    # Percent-encode characters that are invalid in URLs (spaces, quotes, backslashes, etc.)
    # but leave the path structure and existing percent-encoding intact
    safe = ''.join(c if c in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&\'()*+,;=%' else urllib.parse.quote(c)
                   for c in clean)
    url = HTML_BASE + safe
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": HTML_BASE})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        return re.findall(r"Violation of Code:.*?([\d]+-[\d]+\.[\d]+(?:\([A-Z]\))?)", html)
    except Exception as e:
        print(f"    Warning: Could not fetch HTML report: {e}")
        return []

def score_inspection(violations_dict, code_sections=None):
    if code_sections:
        return len(code_sections), sum(code_weight(c) for c in code_sections)
    # Fallback: item-number classification
    items = [v[0] for v in violations_dict.values() if v and v[0]]
    return len(items), sum(violation_weight(v) for v in items)

def get_inspection_scores(facility_id):
    """Return (count, score, insp_date) for the most recent inspection.

    insp_date is a string like "03-18-2026" from the inspections API,
    which is more reliable than the date returned by the facilities list API.
    """
    url = INSPECTIONS.format(encode_id(facility_id))
    try:
        data = fetch_json(url)
        if not data:
            return 0, 0, None
        insp = data[0]
        raw_date = insp.get("columns", {}).get("0", "")
        insp_date = raw_date.replace("Inspection Date:", "").strip() or None
        pp = insp.get("printablePath", "")
        codes = fetch_report_codes(pp) if pp else []
        count, score = score_inspection(insp.get("violations", {}), codes or None)
        return count, score, insp_date
    except urllib.error.HTTPError as e:
        print(f"    Warning: HTTP {e.code} fetching violations for {facility_id}")
        return None, None, None
    except Exception as e:
        print(f"    Warning: {e}")
        return None, None, None

def geocode(address):
    query = urllib.parse.quote(address)
    url   = GEOCODE_URL.format(query, GOOGLE_KEY)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        if data["status"] == "OK":
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]
    except Exception as e:
        print(f"    Warning: geocoding failed for '{address}': {e}")
    return None, None

def fetch_place_types(name, address):
    body = json.dumps({
        "textQuery": f"{name} {address}",
        "maxResultCount": 1,
        "locationBias": {
            "circle": {
                "center": {"latitude": 41.7798, "longitude": -71.4373},
                "radius": 50000.0,
            }
        },
    }).encode()
    req = urllib.request.Request(
        PLACES_URL,
        data=body,
        headers={
            "Content-Type":     "application/json",
            "X-Goog-Api-Key":   GOOGLE_KEY,
            "X-Goog-FieldMask": "places.types",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        places = data.get("places", [])
        return places[0].get("types", []) if places else []
    except Exception as e:
        print(f"    Warning: Places API failed for '{name}': {e}")
        return None

def classify(loc, types):
    """Write google_category (and cuisine if a restaurant) into loc in-place."""
    category = "other"
    for t in types:
        if t in CATEGORY_TYPE_MAP:
            category = CATEGORY_TYPE_MAP[t]
            break
    loc["google_category"] = category

    is_restaurant = (loc.get("license_type") or "").startswith("Seats") or category == "restaurant"
    if is_restaurant and "cuisine" not in loc:
        cuisine = "other"
        for t in types:
            if t in CUISINE_TYPE_MAP:
                cuisine = CUISINE_TYPE_MAP[t]
                break
        loc["cuisine"] = cuisine


# ── Phase 1: Incremental update ───────────────────────────────────────────────

def search_by_date(from_str, to_str, page):
    """One page of facilities whose last inspection falls in [from_str, to_str].

    Dates are MM/DD/YYYY.  The portal JS replaces literal '/' with '%2F' inside
    the JSON string before embedding it in the URL path, so we do the same.
    """
    json_obj = {
        "keyword": base64.b64encode(b"").decode(),
        "from":    base64.b64encode(from_str.encode()).decode(),
        "to":      base64.b64encode(to_str.encode()).decode(),
    }
    json_str = json.dumps(json_obj).replace("/", "%2F")
    url = SEARCH_URL.format(urllib.parse.quote(json_str), page)
    return fetch_json(url)


def incremental_update(locations, by_id):
    from datetime import date, timedelta

    today     = date.today()
    from_date = today - timedelta(days=LOOKBACK_DAYS)
    from_str  = from_date.strftime("%m/%d/%Y")
    to_str    = today.strftime("%m/%d/%Y")

    updated_count = 0
    added_count   = 0

    print(f"─── Phase 1: Incremental update ({from_str} – {to_str}) ───")

    page = 0
    while True:
        try:
            batch = search_by_date(from_str, to_str, page)
        except Exception as e:
            print(f"Error at page {page}: {e}")
            break

        if not batch:
            break

        for item in batch:
            fac      = parse_facility(item)
            existing = by_id.get(fac["id"])

            if existing and existing.get("last_inspection") == fac["last_inspection"]:
                continue  # already up to date

            if existing:
                print(f"  Updated: {fac['name']} ({fac['last_inspection']})")
                count, score, insp_date = get_inspection_scores(fac["id"])
                existing["last_inspection"] = insp_date or fac["last_inspection"]
                if count is not None:
                    existing["violation_count"] = count
                    existing["risk_score"]      = score
                updated_count += 1
            else:
                print(f"  New:     {fac['name']}")
                lat, lng = geocode(fac["address"])
                time.sleep(0.05)
                count, score, insp_date = get_inspection_scores(fac["id"])
                fac["lat"]              = lat
                fac["lng"]              = lng
                fac["violation_count"]  = count if count is not None else 0
                fac["risk_score"]       = score if score is not None else 0
                if insp_date:
                    fac["last_inspection"] = insp_date

                types = fetch_place_types(fac["name"], fac["address"])
                if types is not None:
                    classify(fac, types)
                time.sleep(0.1)

                locations.append(fac)
                by_id[fac["id"]] = fac
                added_count += 1

            time.sleep(0.4)

        page += 1
        time.sleep(0.4)

    print(f"Phase 1 done: {updated_count} updated, {added_count} added.\n")


# ── Phase 2: Backfill scores ──────────────────────────────────────────────────

def backfill_scores(locations):
    missing = [loc for loc in locations if "risk_score" not in loc]
    if not missing:
        print("─── Phase 2: No score backfill needed ───\n")
        return

    print(f"─── Phase 2: Backfilling scores for {len(missing)} locations ───")

    for i, loc in enumerate(missing, 1):
        count, score, insp_date = get_inspection_scores(loc["id"])
        if count is not None:
            loc["violation_count"] = count
            loc["risk_score"]      = score
        if insp_date:
            loc["last_inspection"] = insp_date
        time.sleep(0.7)

        if i % 100 == 0 or i == len(missing):
            print(f"  {i}/{len(missing)} — saving checkpoint…")
            DATA_FILE.write_text(json.dumps(locations, indent=2))

    print("Phase 2 done.\n")


# ── Phase 3: Backfill classification ─────────────────────────────────────────

def backfill_classification(locations):
    def needs_processing(loc):
        if "google_category" not in loc:
            return True
        is_restaurant = (loc.get("license_type") or "").startswith("Seats") \
            or loc.get("google_category") == "restaurant"
        return is_restaurant and "cuisine" not in loc

    missing = [loc for loc in locations if needs_processing(loc)]
    if not missing:
        print("─── Phase 3: No classification backfill needed ───\n")
        return

    print(f"─── Phase 3: Backfilling classification for {len(missing)} locations ───")

    for i, loc in enumerate(missing, 1):
        types = fetch_place_types(loc["name"], loc["address"])
        if types is not None:
            classify(loc, types)
        else:
            print(f"  Skipped (will retry next run): {loc['name']}")
        time.sleep(0.1)

        if i % 100 == 0 or i == len(missing):
            print(f"  {i}/{len(missing)} — saving checkpoint…")
            DATA_FILE.write_text(json.dumps(locations, indent=2))

    print("Phase 3 done.\n")


# ── Phase 4: Reconcile (remove delisted locations) ────────────────────────────

def reconcile(locations, by_id):
    """Remove locations no longer present in the RI API (closed/delisted)."""
    print("─── Phase 4: Reconciling against full API ───")

    api_ids = set()
    offset  = 0
    while True:
        try:
            batch = fetch_json(FACILITIES.format(offset))
        except Exception as e:
            print(f"  Error at offset {offset}: {e} — aborting reconcile")
            return
        if not batch:
            break
        for item in batch:
            api_ids.add(decode_id(item["id"]))
        if offset % 100 == 0:
            print(f"  Crawled {offset} pages ({len(api_ids)} IDs)…")
        offset += 1
        time.sleep(0.2)

    removed = [loc for loc in locations if loc["id"] not in api_ids]
    if not removed:
        print(f"Phase 4 done: no delisted locations found ({len(api_ids)} active).\n")
        return

    for loc in removed:
        print(f"  Removing: {loc['name']} (id={loc['id']}, last={loc.get('last_inspection')})")
        locations.remove(loc)
        by_id.pop(loc["id"], None)

    print(f"Phase 4 done: removed {len(removed)} delisted location(s).\n")


# ── Phase 5: Full rescan ──────────────────────────────────────────────────────

def full_rescan(locations):
    """Check inspectionsData for every facility and update if the API has a newer date.

    Run once with --rescan to fix historically stale entries that the incremental
    update missed because the facilities search API never surfaced them.
    """
    from datetime import datetime

    def parse_date(s):
        try:    return datetime.strptime(s, "%m-%d-%Y")
        except: return None

    print(f"─── Phase 5: Full rescan ({len(locations)} facilities) ───")
    updated = 0

    for i, loc in enumerate(locations, 1):
        count, score, insp_date = get_inspection_scores(loc["id"])
        if insp_date:
            api_d    = parse_date(insp_date)
            stored_d = parse_date(loc.get("last_inspection", ""))
            if api_d and stored_d and api_d > stored_d:
                print(f"  Updated: {loc['name']} ({loc['last_inspection']} → {insp_date})")
                loc["last_inspection"] = insp_date
                if count is not None:
                    loc["violation_count"] = count
                    loc["risk_score"]      = score
                updated += 1
        time.sleep(0.5)

        if i % 100 == 0 or i == len(locations):
            print(f"  {i}/{len(locations)} checked ({updated} updated) — saving checkpoint…")
            DATA_FILE.write_text(json.dumps(locations, indent=2))

    print(f"Phase 5 done: {updated} updated.\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    reconcile_mode = "--reconcile" in sys.argv
    rescan_mode    = "--rescan"    in sys.argv

    locations = json.loads(DATA_FILE.read_text())
    by_id     = {loc["id"]: loc for loc in locations}
    print(f"Loaded {len(locations)} locations from {DATA_FILE}\n")

    incremental_update(locations, by_id)
    backfill_scores(locations)
    backfill_classification(locations)
    if rescan_mode:
        full_rescan(locations)
    if reconcile_mode:
        reconcile(locations, by_id)

    DATA_FILE.write_text(json.dumps(locations, indent=2))
    print(f"Saved {len(locations)} locations to {DATA_FILE}")


if __name__ == "__main__":
    main()
