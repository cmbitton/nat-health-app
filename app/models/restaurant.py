import re
from app.db import db


class Restaurant(db.Model):
    __tablename__ = 'restaurants'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), nullable=False)
    address = db.Column(db.String(255))
    city = db.Column(db.String(100))
    state = db.Column(db.String(50))
    zip = db.Column(db.String(20))
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    source_id = db.Column(db.String(50), index=True)   # RI facility ID from the API
    cuisine_type = db.Column(db.String(100))
    license_type = db.Column(db.String(200))
    region = db.Column(db.String(100), nullable=False)
    ai_summary = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    __table_args__ = (
        db.UniqueConstraint('region', 'slug', name='uq_restaurant_region_slug'),
    )

    inspections = db.relationship(
        'Inspection',
        backref='restaurant',
        lazy='select',
        cascade='all, delete-orphan',
        order_by='Inspection.inspection_date.desc()'
    )

    @property
    def latest_inspection(self):
        return self.inspections[0] if self.inspections else None

    @property
    def latest_score(self):
        insp = self.latest_inspection
        if insp:
            return insp.score
        return None

    @property
    def score_tier(self):
        insp = self.latest_inspection
        if insp is None:
            return None
        return insp.score_tier

    @property
    def city_slug(self):
        city = self.city or ''
        city = city.lower()
        city = city.replace("'", '')
        city = re.sub(r'\s+', '-', city)
        city = re.sub(r'[^a-z0-9-]', '', city)
        return city

    def __repr__(self):
        return f'<Restaurant {self.name} ({self.region})>'
