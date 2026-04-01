#!/usr/bin/env python3
"""
Clear ai_summary for restaurants where the summary contains a score
that differs from the current score by more than THRESHOLD points.

Uses regex to find "score of N" mentions in the summary text. If any
differ from the restaurant's current score, the summary is stale and cleared.

Usage:
    python3 scripts/clear_stale_summaries.py
    python3 scripts/clear_stale_summaries.py --dry-run
    python3 scripts/clear_stale_summaries.py --region nyc
    python3 scripts/clear_stale_summaries.py --region rhode-island --dry-run
"""

import argparse
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection

SCORE_RE = re.compile(r'score of (\d+)', re.IGNORECASE)
THRESHOLD = 1


def _extract_scores(text):
    return [int(m) for m in SCORE_RE.findall(text)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true', help='Print without clearing')
    parser.add_argument('--region', default=None, help='Limit to a specific region slug')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        q = (
            db.session.query(Restaurant, Inspection.score)
            .join(Inspection, db.and_(
                Inspection.restaurant_id == Restaurant.id,
                Inspection.inspection_date == Restaurant.latest_inspection_date,
            ))
            .filter(
                Restaurant.ai_summary.isnot(None),
                Inspection.score.isnot(None),
            )
        )
        if args.region:
            q = q.filter(Restaurant.region == args.region)

        rows = q.all()
        print(f"Checking {len(rows)} restaurants with summaries{' in ' + args.region if args.region else ''}...")

        stale = []
        for r, current_score in rows:
            current = int(current_score)
            mentions = _extract_scores(r.ai_summary)
            if any(abs(m - current) > THRESHOLD for m in mentions):
                stale.append(r)
                if args.dry_run:
                    bad = [m for m in mentions if abs(m - current) > THRESHOLD]
                    print(f"  [{r.id}] {r.name} — current={current}, mentioned={bad}")

        print(f"\n{'Would clear' if args.dry_run else 'Clearing'} {len(stale)} stale summaries.")

        if not args.dry_run and stale:
            for r in stale:
                r.ai_summary = None
            db.session.commit()
            print("Done.")


if __name__ == '__main__':
    main()
