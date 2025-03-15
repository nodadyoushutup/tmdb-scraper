import os

from dotenv import load_dotenv, find_dotenv

env = find_dotenv()
load_dotenv(env)


class Config:
    token = os.environ.get("API_TOKEN")
