from datetime import datetime
import json
from app import db
from app.model import BaseModel


class Movie(BaseModel):
    __tablename__ = "movie"

    adult = db.Column(db.Boolean)
    backdrop_path = db.Column(db.String(255))
    budget = db.Column(db.BigInteger)
    homepage = db.Column(db.String(255))
    id = db.Column(db.Integer, unique=True, nullable=False)
    imdb_id = db.Column(db.String(50))
    original_language = db.Column(db.String(10))
    original_title = db.Column(db.String(255))
    overview = db.Column(db.Text)
    popularity = db.Column(db.Float)
    poster_path = db.Column(db.String(255))
    release_date = db.Column(db.String(20))
    revenue = db.Column(db.BigInteger)
    runtime = db.Column(db.Integer)
    status = db.Column(db.String(50))
    tagline = db.Column(db.Text)
    title = db.Column(db.String(255))
    video = db.Column(db.Boolean)
    vote_average = db.Column(db.Float)
    vote_count = db.Column(db.Integer)
