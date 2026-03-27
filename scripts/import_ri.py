#!/usr/bin/env python3
"""
Import RI location data from data/locations.json + data/history.json into the app database.

Usage (from project root):
    nat-health/bin/python3 scripts/import_ri.py

Run scripts/fetch_history.py first to build data/history.json with full inspection
history and violations. Without history.json, only one inspection per location is
imported (no violations).
"""

import json
import math
import re
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.models.violation import Violation

# ── RI municipalities ─────────────────────────────────────────────────────────
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


def parse_address(raw: str) -> tuple[str, str, str, str | None]:
    """
    Parse 'STREET CITY STATE ZIP[+4]' → (street, city, state, zip5).

    Handles:
    - Standard 5-digit ZIP
    - ZIP+4 without hyphen (e.g. 029043701 → 02904)
    - Mixed-case state codes (e.g. 'Ri')
    - Out-of-state addresses
    """
    raw = raw.strip()

    # Match state + zip at end; zip may be 5 or 9 digits (ZIP+4 no hyphen)
    m = re.search(r'\s+([A-Za-z]{2})\s+(\d{5})\d*\s*$', raw)
    if not m:
        return raw.title(), "Unknown", "RI", None

    state    = m.group(1).upper()
    zip_code = m.group(2)           # always just the 5-digit base
    before   = raw[:m.start()].strip()

    if state != "RI":
        # Out-of-state: best-effort city from last word(s) before state
        parts = before.rsplit(' ', 1)
        street = parts[0].title() if len(parts) == 2 else before.title()
        city   = parts[1].title() if len(parts) == 2 else "Unknown"
        return street, city, state, zip_code

    # RI address — match against known municipalities (longest first)
    upper = before.upper()
    for i, city_upper in enumerate(_CITIES_UPPER):
        if upper.endswith(' ' + city_upper) or upper == city_upper:
            city   = _CITIES_SORTED[i]
            street = before[:-(len(city_upper))].strip().title()
            return street, city, state, zip_code

    # Fallback: assume last word is city
    parts = before.rsplit(' ', 1)
    if len(parts) == 2:
        return parts[0].title(), parts[1].title(), state, zip_code
    return before.title(), "Unknown", state, zip_code


def make_slug(name: str, city: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"['\u2019]", '', slug)
    slug = re.sub(r'[\s_/&]+', '-', slug)
    slug = re.sub(r'[^a-z0-9-]', '', slug)
    slug = re.sub(r'-+', '-', slug).strip('-')
    city_part = city.lower().replace(' ', '-').replace("'", '')
    city_part = re.sub(r'[^a-z0-9-]', '', city_part)
    return slug + '-' + city_part


def risk_to_score(risk_score: int) -> int:
    """Convert InspectRI weighted risk score → 0–100 numeric score.

    Matches InspectRI's cleanlinessScore formula exactly:
        round(100 * exp(-risk_score * 0.07))

    Examples: 0→100, 1 minor→93, 1 priority→81, risk=10→50, risk=50→3
    """
    return round(100 * math.exp(-risk_score * 0.07))


def score_to_result(score: int) -> str:
    if score >= 80:
        return "Pass"
    if score >= 60:
        return "Pass with Conditions"
    return "Fail"


def parse_date(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None


CUISINE_MAP = {
    "pizza":     "Pizza",
    "italian":   "Italian",
    "japanese":  "Japanese / Sushi",
    "chinese":   "Chinese",
    "mexican":   "Mexican / Latin",
    "thai":      "Thai",
    "indian":    "Indian",
    "greek":     "Greek / Mediterranean",
    "seafood":   "Seafood",
    "breakfast": "Café / Breakfast",
    "american":  "American",
    "bar":       "Bar / Pub",
}

CATEGORY_LABEL_MAP = {
    "grocery":    "Grocery / Market",
    "caterer":    "Catering",
    "school":     "School / Childcare",
    "healthcare": "Healthcare Facility",
}


def facility_type_label(loc: dict) -> str | None:
    cat = loc.get("google_category", "other")
    if cat == "restaurant":
        return CUISINE_MAP.get(loc.get("cuisine", ""))
    return CATEGORY_LABEL_MAP.get(cat)


def main():
    data_dir     = Path(__file__).parent.parent / "data"
    loc_file     = data_dir / "locations.json"
    history_file = data_dir / "history.json"

    if not loc_file.exists():
        print(f"ERROR: {loc_file} not found.")
        sys.exit(1)

    locations = json.loads(loc_file.read_text())
    print(f"Loaded {len(locations)} locations from locations.json")

    history: dict = {}
    if history_file.exists():
        history = json.loads(history_file.read_text())
        print(f"Loaded history for {len(history)} locations from history.json")
    else:
        print("No history.json found — importing one inspection per location (no violations).")
        print("Run scripts/fetch_history.py to get full inspection history.\n")

    app = create_app()
    with app.app_context():
        db.create_all()

        existing = Restaurant.query.filter_by(region="rhode-island").count()
        if existing > 0:
            ans = input(
                f"Found {existing} existing rhode-island records. "
                "Delete and reimport? [y/N] "
            ).strip().lower()
            if ans != 'y':
                print("Aborted.")
                return
            db.session.execute(db.text(
                "DELETE FROM violations WHERE inspection_id IN "
                "(SELECT i.id FROM inspections i "
                " JOIN restaurants r ON r.id = i.restaurant_id "
                " WHERE r.region = 'rhode-island')"
            ))
            db.session.execute(db.text(
                "DELETE FROM inspections WHERE restaurant_id IN "
                "(SELECT id FROM restaurants WHERE region = 'rhode-island')"
            ))
            db.session.execute(db.text(
                "DELETE FROM restaurants WHERE region = 'rhode-island'"
            ))
            db.session.commit()
            print("Cleared existing data.\n")

        seen_slugs: set[str] = set()
        imported = skipped = 0

        for i, loc in enumerate(locations, 1):
            name = (loc.get("name") or "").strip()
            if not name:
                skipped += 1
                continue

            street, city, state, zip_code = parse_address(loc.get("address", ""))

            base_slug = make_slug(name, city)
            slug = base_slug
            n = 2
            while slug in seen_slugs:
                slug = f"{base_slug}-{n}"
                n += 1
            seen_slugs.add(slug)

            restaurant = Restaurant(
                source_id   = str(loc.get("id", "")),
                name        = name.title(),
                slug        = slug,
                address     = street,
                city        = city,
                state       = state,
                zip         = zip_code,
                latitude    = loc.get("lat"),
                longitude   = loc.get("lng"),
                cuisine_type= facility_type_label(loc),
                license_type= loc.get("license_type"),
                region      = "rhode-island",
                ai_summary  = None,
            )
            db.session.add(restaurant)
            db.session.flush()

            loc_id = str(loc.get("id", ""))

            if loc_id in history and history[loc_id]:
                # Full history from fetch_history.py
                for insp_data in history[loc_id]:
                    insp_date = parse_date(insp_data.get("date"))
                    if not insp_date:
                        continue
                    violations_data = insp_data.get("violations", [])
                    risk = sum(
                        3 if v.get("severity") == "critical"
                        else 2 if v.get("severity") == "major"
                        else 1
                        for v in violations_data
                    )
                    score = risk_to_score(risk)
                    insp = Inspection(
                        restaurant_id   = restaurant.id,
                        inspection_date = insp_date,
                        inspection_type = insp_data.get("type", "Routine"),
                        score           = score,
                        risk_score      = risk,
                        grade           = None,
                        result          = score_to_result(score),
                    )
                    db.session.add(insp)
                    db.session.flush()

                    for v in violations_data:
                        db.session.add(Violation(
                            inspection_id    = insp.id,
                            violation_code   = v.get("code", ""),
                            description      = v.get("description", ""),
                            severity         = v.get("severity", "minor"),
                            corrected_on_site= v.get("corrected", False),
                        ))
            else:
                # Fallback: single inspection from locations.json summary
                insp_date  = parse_date(loc.get("last_inspection"))
                risk_score = loc.get("risk_score") or 0
                score      = risk_to_score(risk_score)
                if insp_date:
                    db.session.add(Inspection(
                        restaurant_id   = restaurant.id,
                        inspection_date = insp_date,
                        inspection_type = "Routine",
                        score           = score,
                        risk_score      = risk_score,
                        grade           = None,
                        result          = score_to_result(score),
                    ))

            imported += 1

            if i % 500 == 0 or i == len(locations):
                db.session.commit()
                print(f"  {i}/{len(locations)} — {imported} imported, {skipped} skipped")

        db.session.commit()
        print(f"\nDone. {imported} locations imported.")


if __name__ == "__main__":
    main()
