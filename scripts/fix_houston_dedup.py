#!/usr/bin/env python3
"""
Targeted fix for Houston same-date duplicate inspection records.

The Houston portal sometimes records two inspections for the same facility on the
same date (e.g. a routine inspection with violations + a same-day reinspection).
When the lower-risk one was stored first (before the dedup fix), this script finds
and corrects those records.

Strategy:
  1. Load all Houston restaurants from DB: facility_id → {date → Inspection}.
  2. For each unique inspection date in the DB, search the Houston portal for that
     single day (1-day window guarantees any facility appearing twice IS a same-date
     duplicate — no detail pages needed to confirm the date).
  3. For any facility with 2+ inspection IDs on that date, fetch all detail pages.
  4. Keep whichever has the highest risk; update DB if it differs from what we have.

Usage:
  python3 scripts/fix_houston_dedup.py           # check + fix
  python3 scripts/fix_houston_dedup.py --dry-run # report only, no DB writes
"""

import os
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

from import_houston import (
    fetch_pairs_for_range, fetch_detail,
    compute_score, score_to_result,
    REGION, DELAY,
)


def main():
    dry_run = '--dry-run' in sys.argv

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation

    app = create_app()

    with app.app_context():
        # ── Load all Houston inspections from DB ───────────────────────────────
        # facility_id → {inspection_date → Inspection object}
        all_restaurants = (
            Restaurant.query
            .filter_by(region=REGION)
            .filter(Restaurant.source_id.isnot(None))
            .all()
        )
        rid_to_fid = {r.id: r.source_id for r in all_restaurants}
        fid_to_rest = {r.source_id: r for r in all_restaurants}

        all_inspections = (
            Inspection.query
            .filter(Inspection.restaurant_id.in_(rid_to_fid.keys()))
            .all()
        )

        # fid → {date → Inspection}
        db_map: dict[str, dict[date, Inspection]] = defaultdict(dict)
        for insp in all_inspections:
            fid = rid_to_fid.get(insp.restaurant_id)
            if fid:
                db_map[fid][insp.inspection_date] = insp

        unique_dates = sorted({
            d for date_map in db_map.values() for d in date_map
        })

        print(f'Houston restaurants in DB: {len(fid_to_rest)}')
        print(f'Houston inspections in DB: {len(all_inspections)}')
        print(f'Unique inspection dates to check: {len(unique_dates)}\n')

        fixed = dates_with_dupes = 0

        for i, insp_date in enumerate(unique_dates):
            if (i + 1) % 50 == 0:
                print(f'  [{i+1}/{len(unique_dates)}] dates checked, '
                      f'{dates_with_dupes} with duplicates, {fixed} fixed so far...')

            # ── Search Houston portal for this single day ──────────────────────
            try:
                opener, jar, pairs, sd, ed = fetch_pairs_for_range(
                    insp_date, insp_date, maxrows=500,
                )
            except Exception as exc:
                print(f'  {insp_date}: search failed — {exc}')
                time.sleep(DELAY * 2)
                continue

            time.sleep(DELAY)

            if not pairs:
                continue

            # Group by facility_id — any facility appearing 2+ times is a same-date dup
            by_fid: dict[str, list[str]] = defaultdict(list)
            for pair in pairs:
                by_fid[pair['facility_id']].append(pair['inspection_id'])

            for fid, insp_ids in by_fid.items():
                if len(insp_ids) < 2:
                    continue  # only one inspection that day — nothing to fix
                if fid not in db_map:
                    continue  # facility not in our DB at all
                if insp_date not in db_map[fid]:
                    continue  # we don't have an inspection for this date

                dates_with_dupes += 1
                insp_obj  = db_map[fid][insp_date]
                restaurant = fid_to_rest[fid]
                current_risk = insp_obj.risk_score or 0

                # ── Fetch all detail pages for this facility/date ──────────────
                best_detail = None
                best_risk   = current_risk  # only replace if we find something better

                for iid in insp_ids:
                    try:
                        detail = fetch_detail(opener, jar, fid, iid, sd, ed)
                    except Exception:
                        detail = None
                    time.sleep(DELAY)
                    if not detail:
                        continue
                    risk, score = compute_score(detail.get('violations', []))
                    if risk > best_risk:
                        best_risk         = risk
                        best_detail       = detail
                        best_detail['_r'] = risk
                        best_detail['_s'] = score

                if not best_detail:
                    print(f'  SKIP (already best): {restaurant.name} | {insp_date} | '
                          f'risk={current_risk}')
                    continue

                risk  = best_detail['_r']
                score = best_detail['_s']
                vios  = best_detail.get('violations', [])
                print(f'  FIX: {restaurant.name} | {insp_date} | '
                      f'risk {current_risk} → {risk} | {len(vios)} violations'
                      + (' [DRY RUN]' if dry_run else ''))

                if not dry_run:
                    Violation.query.filter_by(inspection_id=insp_obj.id).delete()
                    insp_obj.score           = score
                    insp_obj.risk_score      = risk
                    insp_obj.result          = score_to_result(score)
                    insp_obj.inspection_type = best_detail.get('type') or 'Routine'
                    for v in vios:
                        db.session.add(Violation(
                            inspection_id     = insp_obj.id,
                            violation_code    = v['code'],
                            description       = v['desc'],
                            severity          = v['severity'],
                            corrected_on_site = v['corrected'],
                        ))
                    restaurant.ai_summary = None
                    db.session.commit()

                fixed += 1

        print(f'\nDone.')
        print(f'  Dates checked: {len(unique_dates)}')
        print(f'  Dates with same-day duplicates: {dates_with_dupes}')
        print(f'  Inspections fixed: {fixed}' + (' (dry run)' if dry_run else ''))


if __name__ == '__main__':
    main()
