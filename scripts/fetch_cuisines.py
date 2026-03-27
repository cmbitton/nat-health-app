#!/usr/bin/env python3
"""
Enrich all locations with a Google-validated category, and cuisine type for restaurants.

Uses the Google Places API (New) to look up each location and store:
  - google_category: authoritative category used by locationCategory() in map.js
                     ('restaurant', 'school', 'healthcare', 'grocery', 'other')
  - cuisine:         cuisine bucket for restaurants ('pizza', 'italian', etc.)

Only processes locations missing 'google_category' or restaurants missing 'cuisine'.
Safe to interrupt and resume.

Usage:
  GOOGLE_MAPS_KEY=your-key python3 scripts/fetch_cuisines.py
"""

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from place_types import CATEGORY_TYPE_MAP, CUISINE_TYPE_MAP  # noqa: E402

DATA_FILE  = Path(__file__).parent.parent / "data" / "locations.json"
PLACES_URL = "https://places.googleapis.com/v1/places:searchText"

API_KEY = os.environ.get("GOOGLE_MAPS_KEY")
if not API_KEY:
    raise SystemExit("GOOGLE_MAPS_KEY environment variable not set.")


def fetch_place_types(name: str, address: str) -> list[str] | None:
    """Return Google Places types for the best-matching location, or None on error."""
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
            "X-Goog-Api-Key":   API_KEY,
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
        print(f"  Error fetching '{name}': {e}")
        return None


def classify(loc: dict, types: list[str]) -> None:
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


def needs_processing(loc: dict) -> bool:
    if "google_category" not in loc:
        return True
    is_restaurant = (loc.get("license_type") or "").startswith("Seats") \
        or loc.get("google_category") == "restaurant"
    return is_restaurant and "cuisine" not in loc


def main() -> None:
    locations = json.loads(DATA_FILE.read_text())
    todo = [loc for loc in locations if needs_processing(loc)]
    print(f"{len(todo)} locations need classification.\n")

    for i, loc in enumerate(todo, 1):
        types = fetch_place_types(loc["name"], loc["address"])
        if types is not None:
            classify(loc, types)
        else:
            print(f"  Skipped (will retry next run): {loc['name']}")

        time.sleep(0.1)

        if i % 100 == 0 or i == len(todo):
            print(f"  {i}/{len(todo)} — saving checkpoint…")
            DATA_FILE.write_text(json.dumps(locations, indent=2))

    classified = sum(1 for l in locations if "google_category" in l)
    print(f"\nDone. {classified} locations now have Google-validated categories.")


if __name__ == "__main__":
    main()
