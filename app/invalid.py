from app import db
from app.model import BaseModel


class Invalid(BaseModel):
    __tablename__ = "invalid"

    # Store the movie ID (or endpoint identifier) that produced an invalid response.
    movie_id = db.Column(db.Integer, unique=True, nullable=False)
