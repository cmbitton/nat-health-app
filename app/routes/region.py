import re

from flask import Blueprint, render_template, request, abort, current_app
from sqlalchemy import func
from sqlalchemy.orm import selectinload

from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.routes.restaurant import render_restaurant

region_bp = Blueprint('region', __name__)


def _latest_inspection_subquery():
    """Subquery: most recent inspection_date per restaurant."""
    return (
        db.session.query(
            Inspection.restaurant_id,
            func.max(Inspection.inspection_date).label('max_date')
        )
        .group_by(Inspection.restaurant_id)
        .subquery()
    )


def _scored_restaurants(region, order='asc', limit=5):
    """(Restaurant, Inspection) tuples sorted by risk_score. One SQL query."""
    sq = _latest_inspection_subquery()
    col = Inspection.risk_score.asc() if order == 'asc' else Inspection.risk_score.desc()
    return (
        db.session.query(Restaurant, Inspection)
        .join(Inspection, Restaurant.id == Inspection.restaurant_id)
        .join(sq, db.and_(
            sq.c.restaurant_id == Inspection.restaurant_id,
            sq.c.max_date == Inspection.inspection_date,
        ))
        .filter(
            Restaurant.region == region,
            Inspection.risk_score.isnot(None),
        )
        .order_by(col)
        .limit(limit)
        .all()
    )


def _city_slug(city: str) -> str:
    c = city.lower().replace("'", '')
    c = re.sub(r'\s+', '-', c)
    return re.sub(r'[^a-z0-9-]', '', c)


def render_neighborhood(region, city_slug_str, city_name, restaurants_with_scores):
    """
    restaurants_with_scores: list of (Restaurant, Inspection|None) tuples,
    already sorted by risk_score asc (best first).
    """
    site_name     = current_app.config['SITE_NAME']
    base_url      = current_app.config['BASE_URL']
    region_display = region.replace('-', ' ').title()

    breadcrumbs = [
        {'name': 'Home',          'url': '/'},
        {'name': region_display,  'url': f'/{region}/'},
        {'name': city_name},
    ]

    return render_template(
        'neighborhood.html',
        title       = f'{city_name} Health Inspections | {site_name}',
        description = f'Browse health inspection scores for facilities in {city_name}.',
        canonical_url = f'{base_url}/{region}/{city_slug_str}/',
        region        = region,
        region_display= region_display,
        city_name     = city_name,
        city_slug     = city_slug_str,
        rows          = restaurants_with_scores,   # (Restaurant, Inspection|None)
        breadcrumbs   = breadcrumbs,
    )


@region_bp.route('/<region>/')
def region_index(region):
    q = request.args.get('q', '').strip()

    count = Restaurant.query.filter_by(region=region).count()
    if count == 0:
        abort(404)

    site_name      = current_app.config['SITE_NAME']
    base_url       = current_app.config['BASE_URL']
    region_display = region.replace('-', ' ').title()

    if q:
        search_results = (
            Restaurant.query
            .filter(Restaurant.region == region, Restaurant.name.ilike(f'%{q}%'))
            .order_by(Restaurant.name)
            .limit(20)
            .all()
        )
        return render_template(
            'region.html',
            title          = f'Health Inspections in {region_display} | {site_name}',
            description    = f'Search health inspection scores in {region_display}.',
            canonical_url  = f'{base_url}/{region}/',
            region         = region,
            region_display = region_display,
            search_query   = q,
            search_results = search_results,
            total_restaurants  = count,
            total_inspections  = 0,
            neighborhoods      = [],
            recent_inspections = [],
            top_restaurants    = [],
            bottom_restaurants = [],
        )

    total_inspections = (
        db.session.query(func.count(Inspection.id))
        .join(Restaurant)
        .filter(Restaurant.region == region)
        .scalar()
    )

    city_rows = (
        db.session.query(Restaurant.city, func.count(Restaurant.id))
        .filter(Restaurant.region == region)
        .group_by(Restaurant.city)
        .order_by(Restaurant.city)
        .all()
    )
    neighborhoods = [
        {'city': city, 'count': cnt, 'city_slug': _city_slug(city or '')}
        for city, cnt in city_rows
    ]

    recent_inspections = (
        db.session.query(Inspection, Restaurant)
        .join(Restaurant)
        .filter(Restaurant.region == region)
        .order_by(Inspection.inspection_date.desc())
        .limit(20)
        .all()
    )

    top_restaurants    = _scored_restaurants(region, order='asc',  limit=5)
    bottom_restaurants = _scored_restaurants(region, order='desc', limit=5)

    return render_template(
        'region.html',
        title          = f'Health Inspections in {region_display} | {site_name}',
        description    = f'Browse health inspection scores in {region_display}.',
        canonical_url  = f'{base_url}/{region}/',
        region         = region,
        region_display = region_display,
        search_query   = '',
        search_results = None,
        total_restaurants  = count,
        total_inspections  = total_inspections,
        neighborhoods      = neighborhoods,
        recent_inspections = recent_inspections,
        top_restaurants    = top_restaurants,    # list of (Restaurant, Inspection)
        bottom_restaurants = bottom_restaurants, # list of (Restaurant, Inspection)
    )


@region_bp.route('/<region>/<path_slug>/')
def region_sub(region, path_slug):
    # 1. Try restaurant slug
    restaurant = Restaurant.query.filter_by(region=region, slug=path_slug).first()
    if restaurant:
        return render_restaurant(restaurant)

    # 2. Try city slug — get distinct cities first (small query), then match
    cities = (
        db.session.query(Restaurant.city)
        .filter(Restaurant.region == region)
        .distinct()
        .all()
    )
    city_name = next(
        (c[0] for c in cities if c[0] and _city_slug(c[0]) == path_slug),
        None
    )
    if city_name:
        sq = _latest_inspection_subquery()
        rows = (
            db.session.query(Restaurant, Inspection)
            .outerjoin(
                Inspection,
                db.and_(
                    Inspection.restaurant_id == Restaurant.id,
                    Inspection.inspection_date == db.session.query(
                        func.max(Inspection.inspection_date)
                    ).filter(Inspection.restaurant_id == Restaurant.id)
                    .correlate(Restaurant)
                    .scalar_subquery()
                )
            )
            .filter(Restaurant.region == region, Restaurant.city == city_name)
            .order_by(
                db.case((Inspection.risk_score.is_(None), 1), else_=0),
                Inspection.risk_score.asc()
            )
            .all()
        )
        return render_neighborhood(region, path_slug, city_name, rows)

    abort(404)
