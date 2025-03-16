import asyncio
from app import create_app, db
from app.movie import Movie
from app.scraper import Scraper

app = create_app()

with app.app_context():
    pass

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0")
