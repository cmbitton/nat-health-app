from app.db import db


class RegionStats(db.Model):
    __tablename__ = 'region_stats'

    region     = db.Column(db.String(100), primary_key=True)
    data       = db.Column(db.JSON, nullable=False)
    updated_at = db.Column(db.DateTime, server_default=db.func.now(),
                           onupdate=db.func.now())

    def __repr__(self):
        return f'<RegionStats {self.region}>'
