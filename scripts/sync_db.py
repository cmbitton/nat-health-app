#!/usr/bin/env python3
"""
Incremental DB sync — run after refresh.py has updated data/locations.json.

For each location in locations.json:
  - New location (not in DB): create restaurant + import full inspection history
  - Updated location (new inspection date): fetch latest inspections from API, add to DB
  - Unchanged: skip

Usage:
    nat-health/bin/python3 scripts/sync_db.py

No Google Maps key needed — only hits the RI inspections API.
"""

import base64
import json
import math
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.models.violation import Violation

DATA_DIR       = Path(__file__).parent.parent / "data"
LOCATIONS_FILE = DATA_DIR / "locations.json"

INSPECTIONS_URL = "https://ri.healthinspections.us/ri/API/index.cfm/inspectionsData/{}"
HTML_BASE       = "https://ri.healthinspections.us/"
API_HEADERS     = {"Accept": "application/json", "User-Agent": "Mozilla/5.0",
                   "Referer": "https://ri.healthinspections.us/"}

PRIORITY_ITEMS            = {4, 6, 8, 9, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 27, 28}
PRIORITY_FOUNDATION_ITEMS = {1, 3, 5, 10, 14, 25, 29}

# ── Re-used helpers (mirrors import_ri.py / fetch_history.py) ─────────────────

RI_MUNICIPALITIES = [
    "Barrington", "Bristol", "Burrillville", "Central Falls", "Charlestown",
    "Coventry", "Cranston", "Cumberland", "East Greenwich", "East Providence",
    "Exeter", "Foster", "Glocester", "Hopkinton", "Jamestown", "Johnston",
    "Lincoln", "Little Compton", "Middletown", "Narragansett", "Newport",
    "New Shoreham", "North Kingstown", "North Providence", "North Smithfield",
    "Pawtucket", "Portsmouth", "Providence", "Richmond", "Scituate",
    "Smithfield", "South Kingstown", "Tiverton", "Warren", "Warwick",
    "West Greenwich", "West Warwick", "Westerly", "Woonsocket",
]
_CITIES_SORTED = sorted(RI_MUNICIPALITIES, key=len, reverse=True)
_CITIES_UPPER  = [c.upper() for c in _CITIES_SORTED]

CUISINE_MAP = {
    "pizza": "Pizza", "italian": "Italian", "japanese": "Japanese / Sushi",
    "chinese": "Chinese", "mexican": "Mexican / Latin", "thai": "Thai",
    "indian": "Indian", "greek": "Greek / Mediterranean", "seafood": "Seafood",
    "breakfast": "Café / Breakfast", "american": "American", "bar": "Bar / Pub",
}
CATEGORY_LABEL_MAP = {
    "grocery": "Grocery / Market", "caterer": "Catering",
    "school": "School / Childcare", "healthcare": "Healthcare Facility",
}


def encode_id(s): return base64.b64encode(str(s).encode()).decode()

def fetch_json(url):
    req = urllib.request.Request(url, headers=API_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404: return []
        raise

def fetch_html_codes(path):
    clean = re.sub(r'^\.\.?/', '', path)
    safe_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&\'()*+,;=%')
    safe = ''.join(c if c in safe_chars else urllib.parse.quote(c) for c in clean)
    req = urllib.request.Request(HTML_BASE + safe,
                                 headers={"User-Agent": "Mozilla/5.0", "Referer": HTML_BASE})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        return re.findall(r"Violation of Code:.*?([\d]+-[\d]+\.[\d]+(?:\([A-Z]\))?)", html)
    except Exception:
        return []

def item_severity(n):
    if n in PRIORITY_ITEMS: return "critical"
    if n in PRIORITY_FOUNDATION_ITEMS: return "major"
    return "minor"

def parse_violations(vdict, codes):
    out = []
    for idx, text in enumerate(v[0] for v in vdict.values() if v and v[0]):
        parts = text.split(" - ", 1)
        if len(parts) == 2:
            try:
                n    = int(parts[0].strip())
                desc = parts[1].strip().capitalize()
                sev  = item_severity(n)
                code = codes[idx] if idx < len(codes) else str(n)
            except ValueError:
                desc = text.strip().capitalize()
                sev  = "minor"
                code = codes[idx] if idx < len(codes) else ""
        else:
            desc = text.strip().capitalize()
            sev  = "minor"
            code = codes[idx] if idx < len(codes) else ""
        out.append({"code": code, "description": desc, "severity": sev})
    return out

def risk_to_score(risk):
    return round(100 * math.exp(-risk * 0.07))

def score_to_result(score):
    if score >= 80: return "Pass"
    if score >= 60: return "Pass with Conditions"
    return "Fail"

def parse_date(s):
    if not s: return None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s.strip(), fmt).date()
        except ValueError: pass
    return None

def parse_address(raw):
    raw = raw.strip()
    m = re.search(r'\s+([A-Za-z]{2})\s+(\d{5})\d*\s*$', raw)
    if not m: return raw.title(), "Unknown", "RI", None
    state, zip5 = m.group(1).upper(), m.group(2)
    before = raw[:m.start()].strip()
    if state != "RI":
        parts = before.rsplit(' ', 1)
        return (parts[0].title() if len(parts)==2 else before.title(),
                parts[1].title() if len(parts)==2 else "Unknown", state, zip5)
    upper = before.upper()
    for i, cu in enumerate(_CITIES_UPPER):
        if upper.endswith(' ' + cu) or upper == cu:
            return before[:-(len(cu))].strip().title(), _CITIES_SORTED[i], state, zip5
    parts = before.rsplit(' ', 1)
    if len(parts) == 2: return parts[0].title(), parts[1].title(), state, zip5
    return before.title(), "Unknown", state, zip5

def make_slug(name, city):
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', city.lower().replace(' ', '-').replace("'", ''))
    return s + '-' + c

def facility_type_label(loc):
    cat = loc.get("google_category", "other")
    if cat == "restaurant":
        return CUISINE_MAP.get(loc.get("cuisine", ""))
    return CATEGORY_LABEL_MAP.get(cat)

def unique_slug(base, seen):
    slug, n = base, 2
    while slug in seen:
        slug = f"{base}-{n}"; n += 1
    seen.add(slug)
    return slug


# ── Core sync logic ───────────────────────────────────────────────────────────

def import_inspections(restaurant_id, source_id, since_date=None, fetch_codes=False):
    """
    Fetch all inspections for source_id from the RI API and insert any
    that are newer than since_date (or all of them if since_date is None).
    Returns number of inspections added.
    """
    data = fetch_json(INSPECTIONS_URL.format(encode_id(source_id)))
    if not data:
        return 0

    added = 0
    for insp_data in data:
        raw_date = insp_data.get("columns", {}).get("0", "")
        insp_date = parse_date(raw_date.replace("Inspection Date:", "").strip())
        if not insp_date:
            continue
        if since_date and insp_date <= since_date:
            continue  # already have this or older

        # Check not already in DB (duplicate guard)
        exists = Inspection.query.filter_by(
            restaurant_id=restaurant_id, inspection_date=insp_date
        ).first()
        if exists:
            continue

        vdict = insp_data.get("violations", {})
        codes = []
        if fetch_codes:
            pp = insp_data.get("printablePath", "")
            if pp:
                codes = fetch_html_codes(pp)
                time.sleep(0.15)

        violations = parse_violations(vdict, codes)
        risk = sum(3 if v["severity"]=="critical" else 2 if v["severity"]=="major" else 1
                   for v in violations)
        score = risk_to_score(risk)

        insp = Inspection(
            restaurant_id   = restaurant_id,
            inspection_date = insp_date,
            inspection_type = "Routine",
            score           = score,
            risk_score      = risk,
            grade           = None,
            result          = score_to_result(score),
        )
        db.session.add(insp)
        db.session.flush()

        for v in violations:
            db.session.add(Violation(
                inspection_id     = insp.id,
                violation_code    = v["code"],
                description       = v["description"],
                severity          = v["severity"],
                corrected_on_site = False,
            ))
        added += 1

    return added


def main():
    if not LOCATIONS_FILE.exists():
        print(f"ERROR: {LOCATIONS_FILE} not found."); sys.exit(1)

    locations = json.loads(LOCATIONS_FILE.read_text())
    print(f"Loaded {len(locations)} locations\n")

    app = create_app()
    with app.app_context():
        db.create_all()

        # Load existing source_id → (restaurant_id, latest_date) map in one query
        existing = {
            str(r.source_id): r
            for r in Restaurant.query.filter_by(region="rhode-island")
                                     .filter(Restaurant.source_id.isnot(None))
                                     .all()
        }
        # Pre-load latest inspection date per restaurant
        from sqlalchemy import func
        latest_dates = {
            str(restaurant_id): max_date
            for restaurant_id, max_date in
            db.session.query(Inspection.restaurant_id, func.max(Inspection.inspection_date))
                      .group_by(Inspection.restaurant_id).all()
        }

        # Track slugs to ensure uniqueness for new locations
        seen_slugs = {r.slug for r in existing.values()}

        new_locations   = 0
        updated         = 0
        skipped         = 0
        insp_added      = 0

        for i, loc in enumerate(locations, 1):
            source_id = str(loc.get("id", ""))
            name      = (loc.get("name") or "").strip()
            if not name or not source_id:
                skipped += 1
                continue

            loc_last_date = parse_date(loc.get("last_inspection"))

            if source_id in existing:
                restaurant    = existing[source_id]
                db_latest     = latest_dates.get(str(restaurant.id))

                if loc_last_date and (not db_latest or loc_last_date > db_latest):
                    # New inspection available
                    n = import_inspections(restaurant.id, source_id, since_date=db_latest)
                    if n:
                        db.session.commit()
                        insp_added += n
                        updated    += 1
                        print(f"  Updated: {name} (+{n} inspection{'s' if n>1 else ''})")
                    else:
                        skipped += 1
                else:
                    skipped += 1
            else:
                # Brand-new location
                street, city, state, zip5 = parse_address(loc.get("address", ""))
                slug = unique_slug(make_slug(name, city), seen_slugs)

                restaurant = Restaurant(
                    source_id    = source_id,
                    name         = name.title(),
                    slug         = slug,
                    address      = street,
                    city         = city,
                    state        = state,
                    zip          = zip5,
                    latitude     = loc.get("lat"),
                    longitude    = loc.get("lng"),
                    cuisine_type = facility_type_label(loc),
                    license_type = loc.get("license_type"),
                    region       = "rhode-island",
                )
                db.session.add(restaurant)
                db.session.flush()

                n = import_inspections(restaurant.id, source_id)
                db.session.commit()
                insp_added   += n
                new_locations += 1
                print(f"  New: {name} ({city}) — {n} inspections")

            time.sleep(0.3)

            if i % 200 == 0:
                print(f"  [{i}/{len(locations)}] {new_locations} new, {updated} updated, {skipped} skipped")

        db.session.commit()
        print(f"\nSync complete:")
        print(f"  {new_locations} new locations")
        print(f"  {updated} updated locations")
        print(f"  {insp_added} new inspections added")
        print(f"  {skipped} unchanged (skipped)")


if __name__ == "__main__":
    main()
