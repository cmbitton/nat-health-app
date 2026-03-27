from datetime import date
from flask import Blueprint, render_template, request, current_app
from sqlalchemy import func
from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection

home_bp = Blueprint('home', __name__)


@home_bp.route('/')
def index():
    q = request.args.get('q', '').strip()

    search_results = None
    if q:
        search_results = Restaurant.query.filter(
            Restaurant.name.ilike(f'%{q}%')
        ).order_by(Restaurant.name).limit(20).all()

        return render_template(
            'home.html',
            title=f'Search results for "{q}" | {current_app.config["SITE_NAME"]}',
            description='Search restaurant health inspection scores and violation history across the US.',
            canonical_url=current_app.config['BASE_URL'] + '/',
            search_query=q,
            search_results=search_results,
            regions=[],
            recent_inspections=[],
            low_scores_this_month=[]
        )

    cached = cache.get('home_page_data')
    if cached:
        regions, recent_inspections, low_scores_this_month = cached
    else:
        region_counts = (
            db.session.query(Restaurant.region, func.count(Restaurant.id))
            .group_by(Restaurant.region)
            .order_by(Restaurant.region)
            .all()
        )
        regions = [{'region': r, 'count': c} for r, c in region_counts]

        recent_inspections = (
            db.session.query(Inspection, Restaurant)
            .join(Restaurant)
            .order_by(Inspection.inspection_date.desc())
            .limit(20)
            .all()
        )

        today = date.today()
        month_start = today.replace(day=1)
        low_scores_this_month = (
            db.session.query(Inspection, Restaurant)
            .join(Restaurant)
            .filter(Inspection.inspection_date >= month_start)
            .filter(Inspection.score.isnot(None))
            .order_by(Inspection.score.asc())
            .limit(10)
            .all()
        )

        cache.set('home_page_data', (regions, recent_inspections, low_scores_this_month), timeout=300)

    return render_template(
        'home.html',
        title=f'Restaurant Health Inspection Scores | {current_app.config["SITE_NAME"]}',
        description='Search restaurant health inspection scores and violation history across the US.',
        canonical_url=current_app.config['BASE_URL'] + '/',
        search_query=q,
        search_results=search_results,
        regions=regions,
        recent_inspections=recent_inspections,
        low_scores_this_month=low_scores_this_month
    )
