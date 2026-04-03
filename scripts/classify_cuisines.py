#!/usr/bin/env python3
"""
Classify cuisine_type for restaurants missing it (Houston, Maricopa).

Phase 1: Rule-based matching (chain names + keywords) — free, instant.
Phase 2: Gemini batch classification for remaining unknowns.

Usage:
    # Dry run — show coverage stats without writing
    python3 scripts/classify_cuisines.py --dry-run

    # Write rule-based matches only
    python3 scripts/classify_cuisines.py --rules-only

    # Full run (rules + Gemini for remainder)
    GEMINI_API_KEY=... python3 scripts/classify_cuisines.py

    # Limit Gemini phase (for testing)
    GEMINI_API_KEY=... python3 scripts/classify_cuisines.py --gemini-limit=100

    # Specific region only
    python3 scripts/classify_cuisines.py --region=houston
"""

import json
import os
import re
import sys
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

# ── Cuisine taxonomy (must match existing values in DB) ───────────────────────

CUISINES = [
    'American',
    'Café / Breakfast',
    'Mexican / Latin',
    'Chinese',
    'Pizza',
    'Japanese / Sushi',
    'Italian',
    'Greek / Mediterranean',
    'Grocery / Market',
    'Asian / Fusion',
    'School / Childcare',
    'Indian',
    'Korean',
    'Bar / Pub',
    'Thai',
    'Frozen Desserts',
    'Seafood',
    'Healthcare Facility',
    'Catering',
    'Southeast Asian',
    'Steakhouse',
    'African',
    'French',
    'Vegetarian',
    'Other',
]

# ── Chain prefix lookup (lowercased, no punctuation) ─────────────────────────
# Matched against cleaned name prefix.

CHAIN_CUISINES = {
    # American / Fast Food
    'mcdonalds':             'American',
    'burger king':           'American',
    'wendys':                'American',
    'whataburger':           'American',
    'subway':                'American',
    'jersey mikes':          'American',
    'jimmy johns':           'American',
    'firehouse subs':        'American',
    'firehouse sub':         'American',
    'popeyes':               'American',
    'popeye':                'American',
    'chick-fil-a':           'American',
    'chick fil a':           'American',
    'jack in the box':       'American',
    'sonic':                 'American',
    'raising canes':         'American',
    'raising cane':          'American',
    'shake shack':           'American',
    'five guys':             'American',
    'wingstop':              'American',
    'wing stop':             'American',
    'buffalo wild wings':    'American',
    'buffalo wild wing':     'American',
    'applebees':             'American',
    'applebee':              'American',
    'chilis':                'American',
    'outback steakhouse':    'Steakhouse',
    'longhorn steakhouse':   'Steakhouse',
    'texas roadhouse':       'Steakhouse',
    'saltgrass':             'Steakhouse',
    'pappas bros':           'Steakhouse',
    'pappas steakhouse':     'Steakhouse',
    'pappadeaux':            'Seafood',
    'pappas seafood':        'Seafood',
    'pappas burger':         'American',
    'pappas bar':            'Bar / Pub',
    'luby':                  'American',
    'golden corral':         'American',
    'black bear diner':      'American',
    'dennys':                'Café / Breakfast',
    'denny':                 'Café / Breakfast',
    'ihop':                  'Café / Breakfast',
    'waffle house':          'Café / Breakfast',
    'first watch':           'Café / Breakfast',
    'kolache factory':       'Café / Breakfast',
    'kfc':                   'American',
    'church':                'American',  # Church's Chicken
    'churchs':               'American',
    'el pollo loco':         'Mexican / Latin',
    'pollo tropical':        'Mexican / Latin',
    # Pizza
    'dominos':               'Pizza',
    'domino':                'Pizza',
    'pizza hut':             'Pizza',
    'papa johns':            'Pizza',
    'papa john':             'Pizza',
    'little caesars':        'Pizza',
    'little caesar':         'Pizza',
    'pizza inn':             'Pizza',
    'cici':                  'Pizza',
    'pepperonis':            'Pizza',
    'bertucci':              'Pizza',
    # Café / Coffee
    'starbucks':             'Café / Breakfast',
    'dunkin':                'Café / Breakfast',
    'panera':                'Café / Breakfast',
    'coffee':                'Café / Breakfast',
    'einstein bros':         'Café / Breakfast',
    'einstein brother':      'Café / Breakfast',
    'la madeleine':          'Café / Breakfast',
    'jamba':                 'Café / Breakfast',
    'smoothie king':         'Café / Breakfast',
    # Mexican
    'taco bell':             'Mexican / Latin',
    'chipotle':              'Mexican / Latin',
    'taco cabana':           'Mexican / Latin',
    "torchy":                'Mexican / Latin',
    'torchys':               'Mexican / Latin',
    'fuzzy':                 'Mexican / Latin',
    'fuzzys':                'Mexican / Latin',
    'moe':                   'Mexican / Latin',
    # Chinese / Asian
    'panda express':         'Chinese',
    'panda exp':             'Chinese',
    'p.f. chang':            'Chinese',
    'pf chang':              'Chinese',
    # Japanese
    'kura':                  'Japanese / Sushi',
    'benihana':              'Japanese / Sushi',
    'nobu':                  'Japanese / Sushi',
    # Indian
    # Korean
    # Italian
    'olive garden':          'Italian',
    'carrabba':              'Italian',
    'macaroni grill':        'Italian',
    # Frozen Desserts
    'dairy queen':           'Frozen Desserts',
    'baskin robbins':        'Frozen Desserts',
    'baskin-robbins':        'Frozen Desserts',
    'marble slab':           'Frozen Desserts',
    'cold stone':            'Frozen Desserts',
    'rita':                  'Frozen Desserts',
    "dutch bros":            'Café / Breakfast',
    # Grocery
    'heb':                   'Grocery / Market',
    'h-e-b':                 'Grocery / Market',
    'kroger':                'Grocery / Market',
    'walmart':               'Grocery / Market',
    'wal-mart':              'Grocery / Market',
    'target':                'Grocery / Market',
    'costco':                'Grocery / Market',
    'sams club':             'Grocery / Market',
    "sam's club":            'Grocery / Market',
    'whole foods':           'Grocery / Market',
    'trader joe':            'Grocery / Market',
    'aldi':                  'Grocery / Market',
    'food lion':             'Grocery / Market',
    'fiesta mart':           'Grocery / Market',
    'randalls':              'Grocery / Market',
    'spec':                  'Grocery / Market',
    # Healthcare
    'memorial hermann':      'Healthcare Facility',
    'houston methodist':     'Healthcare Facility',
    'hca houston':           'Healthcare Facility',
    'harris health':         'Healthcare Facility',
    'md anderson':           'Healthcare Facility',
    'texas childrens':       'Healthcare Facility',
    "texas children":        'Healthcare Facility',
    'ut health':             'Healthcare Facility',
    'baylor':                'Healthcare Facility',
    'kindred':               'Healthcare Facility',
    'encompass health':      'Healthcare Facility',
}

# ── Keyword rules (matched anywhere in lowercased cleaned name) ───────────────

KEYWORD_RULES = [
    # School / Childcare — check first (before generic words)
    (['elementary', 'middle school', 'high school', 'junior high',
      'early college', 'magnet school', 'isd ', ' isd', 'head start',
      'kindercare', 'kinder care', 'learning center', 'child develop',
      'childcare', 'child care', 'montessori', 'preschool', 'pre-school',
      'daycare', 'day care', 'nursery school', 'after school'],
     'School / Childcare'),

    # Healthcare
    (['hospital', 'medical center', 'med center', 'health system',
      'nursing home', 'rehabilitation', 'rehab center', 'long term care',
      'ltc facility', 'assisted living', 'dialysis', 'surgery center',
      'urgent care', 'clinic ', ' clinic', 'hospice', 'pharmacy',
      'healthcare facility', 'health care'],
     'Healthcare Facility'),

    # Mexican / Latin
    (['taqueria', 'taquero', 'tacos', 'taco ', ' taco',
      'mexican', 'mexica', 'enchilada', 'tamale', 'tamales',
      'tortilleria', 'tortilla', 'carnitas', 'pupuseria', 'pupusa',
      'salvadoran', 'honduran', 'guatemalan', 'colombian', 'peruvian',
      'empanada', 'panaderia', 'pollos asados', 'pollo asado',
      'birria', 'barbacoa', 'mariscos', 'ceviche', 'burrito',
      'cantina mex', 'tex-mex', 'texmex'],
     'Mexican / Latin'),

    # Japanese / Sushi
    (['sushi', 'ramen', 'izakaya', 'yakitori', 'teriyaki',
      'hibachi', 'japanese', 'udon', 'tempura', 'tonkatsu',
      'omakase', 'sake house'],
     'Japanese / Sushi'),

    # Chinese
    (['chinese', 'china ', ' china', 'hong kong', 'dim sum',
      'szechuan', 'sichuan', 'cantonese', 'peking', 'beijing',
      'shanghai', 'wonton', 'dumpling', 'boba', 'bubble tea',
      'kung fu', 'panda '],
     'Chinese'),

    # Korean
    (['korean', 'korea ', ' korea', 'bulgogi', 'bibimbap',
      'galbi', 'bbq korean', 'korean bbq', 'tofu house'],
     'Korean'),

    # Vietnamese / Southeast Asian
    (['vietnamese', 'viet ', 'pho ', ' pho', 'banh mi',
      'bahn mi', 'thai ', ' thai', 'pad thai', 'lao ', ' lao',
      'cambodian', 'filipino', 'pilipino', 'singaporean',
      'noodle house', 'noodle bar'],
     'Southeast Asian'),

    # Indian
    (['indian', 'india ', ' india', 'curry ', ' curry',
      'tandoor', 'biryani', 'masala', 'tikka', 'naan',
      'pakistani', 'bengali', 'punjabi', 'halal cart',
      'karahi', 'kebab house'],
     'Indian'),

    # Greek / Mediterranean
    (['greek', 'mediterranean', 'gyro', 'gyros', 'falafel',
      'hummus', 'shawarma', 'kebab', 'kabob', 'lebanese',
      'turkish', 'moroccan', 'persian', 'middle eastern',
      'afghan', 'israel', 'jewish'],
     'Greek / Mediterranean'),

    # Italian
    (['italian', 'italia', 'ristorante', 'trattoria', 'osteria',
      'pizzeria', 'pasta ', ' pasta', 'lasagna', 'risotto',
      'gelato'],
     'Italian'),

    # Pizza (after Italian so "pizzeria" can go Italian or Pizza — Pizza wins for generic)
    (['pizza'],
     'Pizza'),

    # Seafood
    (['seafood', 'sea food', 'oyster', 'crab', 'lobster',
      'shrimp ', ' shrimp', 'fish house', 'fish market',
      'crawfish', 'cajun seafood', 'boiling'],
     'Seafood'),

    # Steakhouse
    (['steakhouse', 'steak house', 'chophouse', 'chop house'],
     'Steakhouse'),

    # Bar / Pub
    (['brewery', 'brewhouse', 'brew pub', 'brewpub',
      ' pub ', ' pub$', 'tavern', 'saloon', 'sports bar',
      'beer garden', 'wine bar', 'cocktail bar'],
     'Bar / Pub'),

    # Café / Breakfast
    (['cafe', 'café', 'bakery', 'panaderia', 'donut', 'doughnut',
      'pastry', 'espresso', 'brunch', 'breakfast', 'waffle',
      'pancake', 'crepe', 'beignet', 'tea house', 'teahouse',
      'bubble tea', 'smoothie', 'juice bar', 'kolache'],
     'Café / Breakfast'),

    # Frozen Desserts
    (['ice cream', 'frozen yogurt', 'froyo', 'gelato',
      'creamery', 'snow cone', 'shaved ice', 'paleta'],
     'Frozen Desserts'),

    # Grocery / Market
    (['grocery', 'supermarket', 'supermercado', 'food mart',
      'food store', 'food market', ' market', 'mercado',
      'carniceria', 'butcher', 'deli mart', 'convenience store',
      'gas station', 'fuel station', 'food truck park'],
     'Grocery / Market'),

    # Catering
    (['catering', 'caterer', 'food service', 'commissary'],
     'Catering'),

    # African
    (['african', 'nigerian', 'ethiopian', 'senegalese',
      'ghanaian', 'kenyan', 'somalian', 'cameroonian',
      'west african'],
     'African'),

    # American (broad fallback keywords)
    (['burger', 'barbecue', 'barbeque', ' bbq', 'smokehouse',
      'smoke house', 'southern kitchen', 'soul food',
      'fried chicken', 'hot dog', 'sandwich shop',
      'american grill', 'american kitchen', 'american bistro'],
     'American'),
]


def _clean(name: str) -> str:
    """Lowercase, strip punctuation for matching."""
    return re.sub(r"['\u2019\-\.\#]", '', name.lower()).strip()


def rule_classify(name: str) -> str | None:
    cleaned = _clean(name)

    # 1. Chain prefix lookup
    for prefix, cuisine in CHAIN_CUISINES.items():
        p = _clean(prefix)
        if cleaned.startswith(p) or f' {p}' in cleaned:
            return cuisine

    # 2. Keyword rules
    for keywords, cuisine in KEYWORD_RULES:
        for kw in keywords:
            if kw in cleaned:
                return cuisine

    return None


# ── Gemini batch classification ───────────────────────────────────────────────

GEMINI_SYSTEM = f"""\
You classify restaurant/establishment names into cuisine categories.
Return ONLY a JSON array of strings, one per input name, in the same order.
Use exactly one of these values, or null if truly uncertain:
{json.dumps(CUISINES, indent=2)}

Rules:
- Schools, daycares, learning centers → "School / Childcare"
- Hospitals, clinics, nursing homes → "Healthcare Facility"
- Gas stations with food → "Grocery / Market"
- Hotel restaurants → classify by their cuisine, not "Other"
- Bars without clear food → "Bar / Pub"
- null only if you genuinely cannot determine the cuisine\
"""

GEMINI_BATCH = 80
GEMINI_WORKERS = 20


def gemini_classify_batch(names: list[str], client, model: str) -> list[str | None]:
    from google.genai import types
    prompt = json.dumps(names)
    for attempt in range(4):
        try:
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=GEMINI_SYSTEM,
                    max_output_tokens=500,
                    temperature=0.1,
                ),
            )
            text = resp.text.strip()
            # Strip markdown code fences if present
            text = re.sub(r'^```(?:json)?\s*', '', text)
            text = re.sub(r'\s*```$', '', text)
            result = json.loads(text)
            if isinstance(result, list) and len(result) == len(names):
                # Validate each value
                return [r if r in CUISINES else None for r in result]
        except Exception as e:
            if attempt < 3:
                time.sleep(2 ** attempt)
    return [None] * len(names)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run',      action='store_true')
    parser.add_argument('--rules-only',   action='store_true')
    parser.add_argument('--region',       default=None)
    parser.add_argument('--gemini-limit', type=int, default=None)
    args = parser.parse_args()

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant

    app = create_app()
    with app.app_context():
        q = Restaurant.query.filter(Restaurant.cuisine_type.is_(None))
        if args.region:
            q = q.filter(Restaurant.region == args.region)
        else:
            q = q.filter(Restaurant.region.in_(['houston', 'maricopa']))
        restaurants = q.all()

        print(f'Restaurants missing cuisine: {len(restaurants)}')

        # ── Phase 1: Rule-based ──────────────────────────────────────────────
        rule_hits = 0
        unclassified = []
        updates: dict[int, str] = {}

        for r in restaurants:
            cuisine = rule_classify(r.name)
            if cuisine:
                updates[r.id] = cuisine
                rule_hits += 1
            else:
                unclassified.append(r)

        print(f'\nPhase 1 (rules): {rule_hits} classified, {len(unclassified)} remaining')

        if not args.dry_run:
            for r in restaurants:
                if r.id in updates:
                    r.cuisine_type = updates[r.id]
            db.session.commit()
            print(f'  Saved {rule_hits} rule-based classifications.')

        if args.rules_only or not unclassified:
            print('\nDone.')
            return

        # ── Phase 2: Gemini ──────────────────────────────────────────────────
        GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
        if not GEMINI_API_KEY:
            print('\nNo GEMINI_API_KEY — skipping Gemini phase.')
            return

        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        model = 'gemini-3.1-flash-lite-preview'

        targets = unclassified
        if args.gemini_limit:
            targets = targets[:args.gemini_limit]

        print(f'\nPhase 2 (Gemini): classifying {len(targets)} restaurants '
              f'in batches of {GEMINI_BATCH} with {GEMINI_WORKERS} workers...')

        batches = [targets[i:i+GEMINI_BATCH] for i in range(0, len(targets), GEMINI_BATCH)]
        gemini_hits = gemini_null = 0
        rid_to_cuisine: dict[int, str] = {}

        with ThreadPoolExecutor(max_workers=GEMINI_WORKERS) as pool:
            futures = {
                pool.submit(
                    gemini_classify_batch,
                    [r.name for r in batch],
                    client, model
                ): batch
                for batch in batches
            }
            for future in as_completed(futures):
                batch = futures[future]
                results = future.result()
                for r, cuisine in zip(batch, results):
                    if cuisine:
                        rid_to_cuisine[r.id] = cuisine
                        gemini_hits += 1
                    else:
                        gemini_null += 1

        print(f'  Gemini: {gemini_hits} classified, {gemini_null} left as null')

        if not args.dry_run:
            for r in targets:
                if r.id in rid_to_cuisine:
                    r.cuisine_type = rid_to_cuisine[r.id]
            db.session.commit()
            print(f'  Saved {gemini_hits} Gemini classifications.')

        total = rule_hits + gemini_hits
        print(f'\nDone. {total}/{len(restaurants)} classified '
              f'({100*total//len(restaurants)}% coverage).')


if __name__ == '__main__':
    main()
