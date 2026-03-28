from flask import Flask, render_template
from dotenv import load_dotenv

load_dotenv()


def create_app():
    app = Flask(__name__)
    app.config.from_object('app.config.Config')

    # Initialize extensions
    from app.db import db, cache
    db.init_app(app)
    cache.init_app(app)

    # Register custom Jinja2 filters
    from app.utils import get_region_display

    @app.template_filter('region_display')
    def region_display_filter(region):
        return get_region_display(region)

    @app.template_filter('format_date')
    def format_date_filter(value):
        if value is None:
            return 'N/A'
        try:
            return value.strftime('%b %-d, %Y')
        except Exception:
            return str(value)

    # Register blueprints
    from app.routes.home import home_bp
    from app.routes.region import region_bp
    from app.routes.sitemap import sitemap_bp

    app.register_blueprint(home_bp)
    app.register_blueprint(region_bp)
    app.register_blueprint(sitemap_bp)

    # Error handlers
    @app.errorhandler(404)
    def not_found(e):
        return render_template(
            '404.html',
            title='Page Not Found | ' + app.config['SITE_NAME'],
            description='The page you are looking for could not be found.',
            canonical_url=app.config['BASE_URL'] + '/'
        ), 404

    @app.errorhandler(500)
    def server_error(e):
        return render_template(
            '500.html',
            title='Server Error | ' + app.config['SITE_NAME'],
            description='An internal server error occurred.',
            canonical_url=app.config['BASE_URL'] + '/'
        ), 500

    return app
