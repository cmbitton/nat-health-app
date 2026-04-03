#!/usr/bin/env python3
"""
Philadelphia food inspection importer.

Data source: https://philadelphia-pa.healthinspections.us/philadelphia/

Flow:
  Full import (--full):
    1. Search date-range chunks (2-day, most-recent-first) to collect
       (facilityID, inspectionID) pairs.  Portal caps results at 100 per
       search; 2-day windows stay safely under that.
    2. Fetch _report_full.cfm for each new inspection in parallel.
    3. Parse compliance table (item# + IN/OUT) and observations section
       (PA code + inspector text + COS flag), write to DB.

  Incremental (default):
    Same but only the last --days days (default 7).

Violation severity:
  FDA inspection form item numbers → item_severity() from fda_codes.
  Items 1-27 are Risk Factor/Intervention (critical/major).
  Items 28+ are Good Retail Practices / Phila. ordinances (minor).

Scoring (same formula as all other regions):
  risk_score = sum of violation weights (3/2/1)
  score      = round(100 × exp(−risk × 0.05))

Usage:
  python3 scripts/import_philadelphia.py
  python3 scripts/import_philadelphia.py --days=14
  python3 scripts/import_philadelphia.py --full
  python3 scripts/import_philadelphia.py --full --since=2023-01-01
  python3 scripts/import_philadelphia.py --dry-run
"""

import html as _html
import math
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))            # for fda_codes
sys.path.insert(0, str(_HERE.parent))     # for app

from fda_codes import item_severity

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL   = 'https://philadelphia-pa.healthinspections.us'
SEARCH_URL = BASE_URL + '/philadelphia/search.cfm'
REPORT_URL = BASE_URL + '/_templates/551/RetailFood/_report_full.cfm'
DOMAIN_ID  = 551
REGION     = 'philadelphia'
WORKERS    = 20
CHUNK_DAYS = 1     # days per search window; 2-day windows stay under the 100-result cap

# ── HTTP ──────────────────────────────────────────────────────────────────────

def _ssl_ctx():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

def _get(url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    'User-Agent': 'Mozilla/5.0',
                    'Accept':     'text/html,application/xhtml+xml,*/*;q=0.8',
                }
            )
            with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=20) as r:
                return r.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            if e.code in (503, 429) and attempt < retries - 1:
                print(f'  {e.code} on attempt {attempt + 1}, retrying in 2s...')
                time.sleep(2)
            else:
                return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(1)
    return None

# ── Helpers ───────────────────────────────────────────────────────────────────

_TAGS_RE   = re.compile(r'<[^>]+>')
_SPACES_RE = re.compile(r'\s+')

def _strip(text: str) -> str:
    """Strip HTML tags, decode entities, collapse whitespace."""
    return _SPACES_RE.sub(' ', _TAGS_RE.sub(' ', _html.unescape(text))).strip()

def make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', (city or 'philadelphia').lower().replace(' ', '-'))
    return f'{s}-{c}'

def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f'{base}-{n}'
        n += 1
    seen.add(slug)
    return slug

# ── Search page parsing ───────────────────────────────────────────────────────

# Named facility links:  facilityID=UUID"><b>Name</b>
# UUIDs here use non-standard 8-4-4-16 grouping (35 chars), so match liberally
_FAC_NAME_RE = re.compile(
    r'facilityID=([A-F0-9-]{30,40})"[^>]*>\s*<b>([^<]+)</b>',
    re.IGNORECASE
)
# Inspection links in green date-link:  facilityID=X&inspectionID=Y&inspType=Food">MM/DD/YYYY
_INSP_LINK_RE = re.compile(
    r'facilityID=([A-F0-9-]{30,40})&(?:amp;)?inspectionID=([A-F0-9-]{30,40})'
    r'&(?:amp;)?inspType=Food[^>]*>\s*(\d{2}/\d{2}/\d{4})',
    re.IGNORECASE
)
# Address block right after a facilityID link
_ADDR_BLOCK_RE  = re.compile(r'margin-bottom:10px;">(.*?)</div>', re.DOTALL | re.IGNORECASE)
_CITY_STATE_ZIP = re.compile(r'([^,<]+),\s*[A-Z]{2}\s+(\d{5})', re.IGNORECASE)


def parse_search_page(html: str) -> list[dict]:
    """Extract (facilityID, inspectionID, name, address, date) from one results page."""
    # 1. Map facilityID → display name (from <b> links)
    fac_names: dict[str, str] = {}
    for m in _FAC_NAME_RE.finditer(html):
        fac_names.setdefault(m.group(1).upper(), _html.unescape(m.group(2).strip()))

    # 2. Extract inspection pairs (facilityID + inspectionID + date)
    results = []
    seen_insp = set()
    for m in _INSP_LINK_RE.finditer(html):
        fac_id  = m.group(1).upper()
        insp_id = m.group(2).upper()
        if insp_id in seen_insp:
            continue
        seen_insp.add(insp_id)

        try:
            insp_date = datetime.strptime(m.group(3), '%m/%d/%Y').date()
        except ValueError:
            insp_date = None

        results.append({
            'facility_id': fac_id,
            'insp_id':     insp_id,
            'name':        fac_names.get(fac_id, ''),
            'insp_date':   insp_date,
            'street':      '',
            'city':        'Philadelphia',
            'zipcode':     '',
        })

    # 3. Extract address for each result from the HTML block near its facilityID
    for rec in results:
        pos = html.upper().find(f'FACILITYID={rec["facility_id"]}')
        if pos == -1:
            continue
        snippet = html[pos:pos + 600]
        addr_m  = _ADDR_BLOCK_RE.search(snippet)
        if not addr_m:
            continue
        addr_text = _strip(addr_m.group(1))
        csz_m = _CITY_STATE_ZIP.search(addr_text)
        if csz_m:
            rec['city']    = csz_m.group(1).strip().title()
            rec['zipcode'] = csz_m.group(2).strip()
            street_raw     = addr_text[:csz_m.start()].strip()
            rec['street']  = _SPACES_RE.sub(' ', street_raw).title()
        else:
            rec['street'] = addr_text

    return results


def fetch_search_chunk(from_date: date, to_date: date) -> list[dict]:
    """Fetch all pages for a date range; warn if the 100-result cap is hit."""
    sd = from_date.strftime('%m/%d/%Y')
    ed = to_date.strftime('%m/%d/%Y')

    all_results: list[dict] = []
    seen_insp:   set[str]   = set()
    start = 1

    while True:
        url = (
            f'{SEARCH_URL}?1=1'
            f'&sd={urllib.parse.quote(sd)}&ed={urllib.parse.quote(ed)}'
            f'&kw1=&kw2=&rel1=F.organization_facility&rel2=F.organization_facility'
            f'&zc=&facType=Any&dtRng=YES&pre=Contains&subType=Any&start={start}'
        )
        html = _get(url)
        if not html:
            break

        page_results = parse_search_page(html)
        if not page_results:
            break

        for r in page_results:
            if r['insp_id'] not in seen_insp:
                seen_insp.add(r['insp_id'])
                all_results.append(r)

        # Stop when we get a partial page (genuinely last page)
        if len(page_results) < 20:
            break
        start += 20

    return all_results

# ── Inspection report parsing ─────────────────────────────────────────────────

# Compliance table rows: item#, IN/OUT/N-O/N-A, description, COS cell, Repeat cell
_COMP_ROW_RE = re.compile(
    r'<td class="center b l"[^>]*>\s*(\d+)\s*</td>'
    r'\s*<td class="center b"[^>]*>\s*(IN|OUT|N/O|N/A)\s*</td>'
    r'\s*<td class="b r"[^>]*>(.*?)</td>'
    r'\s*<td class="center b"[^>]*>(.*?)</td>'   # COS column
    r'\s*<td class="center b"[^>]*>(.*?)</td>',  # Repeat column
    re.DOTALL | re.IGNORECASE
)

# Observation rows: item#, full cell content (class="ten b r")
_OBS_ROW_RE = re.compile(
    r'<td class="ten b r"[^>]*>\s*(\d+)\s*</td>'
    r'\s*<td class="ten b r"[^>]*>(.*?)</td>',
    re.DOTALL | re.IGNORECASE
)

_PA_CODE_RE = re.compile(r'\[([^\]]+)\]')
_COS_RE     = re.compile(r'corrected\s+on.site', re.IGNORECASE)
_REPEAT_RE  = re.compile(r'repeat\s+violation', re.IGNORECASE)

# Strip "Violation of Code: [XX]" prefix from observation text
_OBS_PREFIX_RE = re.compile(r'.*?Violation of Code:\s*\[[^\]]+\]\s*', re.DOTALL | re.IGNORECASE)
# Strip trailing status phrases
_OBS_SUFFIX_RE = re.compile(
    r'\s*(?:\*\*\s*)?(?:Person in charge[^.]*?Corrected On.Site\.'
    r'|Corrected On.Site\.'
    r'|Repeat Violation\.?'
    r'|Correct By:\s*[\d/]+\.?).*$',
    re.IGNORECASE | re.DOTALL
)


def parse_inspection_page(html: str, insp_id: str) -> dict | None:
    if not html:
        return None

    # ── Compliance table ──────────────────────────────────────────────────────
    # Collect only OUT items; map item# → {desc, cos}
    compliance: dict[int, dict] = {}
    for m in _COMP_ROW_RE.finditer(html):
        status = m.group(2).strip().upper()
        if status != 'OUT':
            continue
        item_n  = int(m.group(1))
        desc    = _strip(m.group(3))
        cos_raw = m.group(4)
        # COS column contains "X" when corrected, "&nbsp;" when not
        cos = bool(re.search(r'\bX\b', cos_raw, re.IGNORECASE)
                   and '&nbsp;' not in cos_raw)
        compliance[item_n] = {'desc': desc, 'cos': cos}

    # ── Observations section ──────────────────────────────────────────────────
    # Map item# → {pa_code, notes, corrected, repeat}
    observations: dict[int, dict] = {}
    for m in _OBS_ROW_RE.finditer(html):
        try:
            item_n = int(m.group(1).strip())
        except ValueError:
            continue
        cell = m.group(2)

        code_m  = _PA_CODE_RE.search(cell)
        pa_code = code_m.group(1).strip() if code_m else ''

        # Extract inspector note: strip code prefix and trailing status lines
        text = _strip(cell)
        text = _OBS_PREFIX_RE.sub('', text, count=1)
        text = _OBS_SUFFIX_RE.sub('', text).strip()
        text = _SPACES_RE.sub(' ', text).strip()

        observations[item_n] = {
            'pa_code':   pa_code,
            'notes':     text,
            'corrected': bool(_COS_RE.search(cell)),
            'repeat':    bool(_REPEAT_RE.search(cell)),
        }

    # ── Merge: compliance table drives the violation list ─────────────────────
    violations = []
    for item_n, comp in sorted(compliance.items()):
        obs      = observations.get(item_n, {})
        sev      = item_severity(item_n)
        # Prefer observation COS text over compliance table X (more reliable)
        corrected = obs.get('corrected', comp['cos'])

        violations.append({
            'code':      obs.get('pa_code', ''),
            'desc':      comp['desc'],
            'notes':     obs.get('notes', ''),
            'severity':  sev,
            'corrected': corrected,
        })

    return {'insp_id': insp_id, 'violations': violations}


def _fetch_insp_page(insp_id: str) -> tuple[str, dict | None]:
    """Thread-safe: fetch and parse one inspection report."""
    url  = f'{REPORT_URL}?inspectionID={insp_id}&domainID={DOMAIN_ID}&userID=0'
    html = _get(url)
    return insp_id, (parse_inspection_page(html, insp_id) if html else None)

# ── Scoring ───────────────────────────────────────────────────────────────────

_SEV_WEIGHTS = {'critical': 3, 'major': 2, 'minor': 1}

def compute_score(violations: list) -> tuple[int, int]:
    risk  = sum(_SEV_WEIGHTS.get(v['severity'], 1) for v in violations)
    score = round(100 * math.exp(-risk * 0.05))
    return risk, score

def score_to_result(score: int) -> str:
    if score >= 75: return 'Pass'
    if score >= 55: return 'Pass with Conditions'
    return 'Fail'

# ── DB write ──────────────────────────────────────────────────────────────────

def write_chunk(records: list[dict], dry_run: bool,
                existing: dict, seen_slugs: set,
                db, Restaurant, Inspection, Violation) -> tuple[int, int, int]:
    """
    Write one chunk of enriched records to DB.
    existing: facilityID → Restaurant (pre-loaded, updated in place)
    seen_slugs: set of slugs in use (updated in place)
    Returns (new_restaurants, new_inspections, skipped).
    """
    new_r = new_i = skipped = 0

    for rec in records:
        detail    = rec.get('detail')
        insp_date = rec.get('insp_date')
        name      = (rec.get('name') or '').strip()

        if not detail or not insp_date or not name:
            skipped += 1
            continue

        violations      = detail.get('violations', [])
        risk, score     = compute_score(violations)

        if dry_run:
            print(f"  [dry] {name} {insp_date} score={score} "
                  f"violations={len(violations)}")
            new_i += 1
            continue

        fid = rec['facility_id']

        # ── Get or create restaurant ──────────────────────────────────────────
        if fid in existing:
            restaurant = existing[fid]
        else:
            slug = unique_slug(
                make_slug(name, rec.get('city') or 'philadelphia'),
                seen_slugs
            )
            restaurant = Restaurant(
                region    = REGION,
                source_id = fid,
                name      = name,
                slug      = slug,
                address   = rec.get('street', ''),
                city      = rec.get('city', 'Philadelphia'),
            )
            db.session.add(restaurant)
            db.session.flush()
            existing[fid] = restaurant
            new_r += 1

        # ── Skip inspection if already in DB ──────────────────────────────────
        if Inspection.query.filter_by(
            restaurant_id=restaurant.id, source_id=rec['insp_id']
        ).first():
            skipped += 1
            continue

        # ── Write inspection ──────────────────────────────────────────────────
        insp = Inspection(
            restaurant_id   = restaurant.id,
            inspection_date = insp_date,
            source_id       = rec['insp_id'],
            inspection_type = 'Routine',
            score           = score,
            risk_score      = risk,
            result          = score_to_result(score),
        )
        db.session.add(insp)
        db.session.flush()

        for v in violations:
            db.session.add(Violation(
                inspection_id     = insp.id,
                violation_code    = v['code'] or None,
                description       = v['desc'],
                inspector_notes   = v['notes'] or None,
                severity          = v['severity'],
                corrected_on_site = v['corrected'],
            ))

        old_latest = restaurant.latest_inspection_date
        if old_latest is None or insp_date > old_latest:
            if old_latest != insp_date:
                restaurant.ai_summary = None
            restaurant.latest_inspection_date = insp_date

        new_i += 1

    return new_r, new_i, skipped


# ── Import runners ────────────────────────────────────────────────────────────

def run_import(from_date: date, to_date: date, dry_run: bool,
               app, db, Restaurant, Inspection, Violation,
               chunk_days: int = CHUNK_DAYS):

    # Build chunk list, most-recent-first (recent data matters more)
    chunks: list[tuple[date, date]] = []
    cur = from_date
    while cur <= to_date:
        nxt = min(cur + timedelta(days=chunk_days), to_date)
        chunks.append((cur, nxt))
        cur = nxt + timedelta(days=1)
    chunks.reverse()

    with app.app_context():
        # Pre-load all known Philly restaurants to avoid per-record queries
        existing = {
            r.source_id: r
            for r in Restaurant.query.filter_by(region=REGION)
                                     .filter(Restaurant.source_id.isnot(None)).all()
        }
        seen_slugs = {r.slug for r in existing.values()}
        seen_slugs |= {
            r.slug for r in
            Restaurant.query.filter(Restaurant.region != REGION)
                            .with_entities(Restaurant.slug).all()
        }

        total_r = total_i = total_skip = 0
        committed = 0

        for idx, (chunk_from, chunk_to) in enumerate(chunks):
            label = f'{chunk_from.strftime("%m/%d/%Y")}–{chunk_to.strftime("%m/%d/%Y")}'
            print(f'  Searching {label}...', end=' ', flush=True)

            search_results = fetch_search_chunk(chunk_from, chunk_to)
            print(f'{len(search_results)} pairs', flush=True)

            if not search_results:
                continue

            # Filter out inspections already in DB by source_id
            if not dry_run and search_results:
                known = {
                    row[0] for row in db.session.execute(
                        db.text('SELECT source_id FROM inspections '
                                'WHERE source_id = ANY(:ids)'),
                        {'ids': [r['insp_id'] for r in search_results]}
                    ).fetchall()
                }
                search_results = [r for r in search_results
                                  if r['insp_id'] not in known]
                if not search_results:
                    continue

            # Fetch inspection detail pages in parallel
            insp_map: dict[str, dict | None] = {}
            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                futures = {
                    pool.submit(_fetch_insp_page, r['insp_id']): r['insp_id']
                    for r in search_results
                }
                done = 0
                for future in as_completed(futures):
                    insp_id, detail = future.result()
                    insp_map[insp_id] = detail
                    done += 1
                    if done % 50 == 0 or done == len(futures):
                        print(f'  {done}/{len(futures)} fetched', end='\r', flush=True)
            if search_results:
                print()

            for r in search_results:
                r['detail'] = insp_map.get(r['insp_id'])

            new_r, new_i, skip = write_chunk(
                search_results, dry_run,
                existing, seen_slugs,
                db, Restaurant, Inspection, Violation
            )
            if not dry_run:
                db.session.commit()
                committed += new_i

            total_r    += new_r
            total_i    += new_i
            total_skip += skip

            print(f'  Chunk {idx + 1}/{len(chunks)}: '
                  f'+{new_r} restaurants, +{new_i} inspections, {skip} skipped')

            if committed >= 250:
                print(f'  Committed {committed} inspections so far...')
                committed = 0

    print(f'\nDone: +{total_r} restaurants, +{total_i} inspections, '
          f'{total_skip} skipped.')


def run_full_import(dry_run: bool, app, db, Restaurant, Inspection, Violation,
                    since: date | None = None, chunk_days: int = CHUNK_DAYS):
    today     = date.today()
    from_date = since or date(2018, 1, 1)
    print(f'=== Philadelphia: Full import {from_date} → {today} ===')
    run_import(from_date, today, dry_run, app, db, Restaurant, Inspection, Violation,
               chunk_days=chunk_days)


def run_incremental(days: int, dry_run: bool, app, db, Restaurant, Inspection, Violation):
    today     = date.today()
    from_date = today - timedelta(days=days)
    print(f'=== Philadelphia: Incremental (last {days} days) ===')
    run_import(from_date, today, dry_run, app, db, Restaurant, Inspection, Violation)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    full_mode  = '--full'    in sys.argv
    dry_run    = '--dry-run' in sys.argv
    days       = 7
    since      = None
    chunk_days = CHUNK_DAYS
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])
        elif arg.startswith('--since='):
            since = date.fromisoformat(arg.split('=', 1)[1])
        elif arg.startswith('--chunk-days='):
            chunk_days = int(arg.split('=', 1)[1])

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation

    app = create_app()

    if full_mode:
        run_full_import(dry_run, app, db, Restaurant, Inspection, Violation,
                        since=since, chunk_days=chunk_days)
    else:
        run_incremental(days, dry_run, app, db, Restaurant, Inspection, Violation)


if __name__ == '__main__':
    main()
