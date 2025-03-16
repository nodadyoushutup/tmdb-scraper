from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from app.config import Config
from app.logger import logger

db = SQLAlchemy()


def create_app():
    app = Flask(__name__)
    # Load configuration from the Config class
    app.config.from_object(Config)

    # Initialize SQLAlchemy with the app
    db.init_app(app)

    # Register blueprints
    from app.routes.movie import movie
    from app.routes.scraper import scraper
    app.register_blueprint(movie)
    app.register_blueprint(scraper)

    # Create database tables if they don't exist
    with app.app_context():
        db.create_all()

    return app
