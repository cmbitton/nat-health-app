"""Dump top 15 violation codes + stored descriptions for each region."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.db import db
from app.models.inspection import Inspection
from app.models.violation import Violation
from sqlalchemy import func

app = create_app()
with app.app_context():
    regions = [r[0] for r in db.session.query(Inspection.region).distinct().all() if r[0]]
    for region in sorted(regions):
        print(f'\n=== {region.upper()} ===')
        rows = (
            db.session.query(
                Violation.violation_code,
                Violation.description,
                func.count(Violation.id).label('cnt'),
            )
            .join(Inspection, Violation.inspection_id == Inspection.id)
            .filter(Inspection.region == region,
                    Violation.violation_code.isnot(None),
                    Violation.violation_code != '')
            .group_by(Violation.violation_code, Violation.description)
            .order_by(func.count(Violation.id).desc())
            .limit(15)
            .all()
        )
        for i, r in enumerate(rows, 1):
            desc = (r.description or '(NULL)')[:120]
            print(f'  {i:2d}. code={r.violation_code:22s} cnt={r.cnt:>6,}  desc="{desc}"')
