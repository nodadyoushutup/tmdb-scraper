import os
from dotenv import load_dotenv, find_dotenv

env_path = find_dotenv()
load_dotenv(env_path)


class Config:
    DEBUG = os.environ.get("DEBUG", "True").lower() in ["true", "1"]
    SECRET_KEY = os.environ.get("SECRET_KEY", "mysecretkey")

    # Use an absolute path for the database file
    SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    API_TOKEN = os.environ.get("API_TOKEN")
    SQLALCHEMY_ECHO = True
