from flask import Blueprint, request, jsonify
from app.movie import Movie

movie = Blueprint('movie', __name__, url_prefix="/movie")


@movie.route("first", methods=["GET"])
def get_movie():
    key = request.args.get("key")
    value = request.args.get("value")
    if not key or not value:
        return jsonify({"error": "Both 'key' and 'value' query parameters are required."}), 400

    movie = Movie.get(key, value)
    if movie:
        movie_data = {
            "adult": movie.adult,
            "backdrop_path": movie.backdrop_path,
            "budget": movie.budget,
            "homepage": movie.homepage,
            "id": movie.id,
            "imdb_id": movie.imdb_id,
            "original_language": movie.original_language,
            "original_title": movie.original_title,
            "overview": movie.overview,
            "popularity": movie.popularity,
            "poster_path": movie.poster_path,
            "release_date": movie.release_date,
            "revenue": movie.revenue,
            "runtime": movie.runtime,
            "status": movie.status,
            "tagline": movie.tagline,
            "title": movie.title,
            "video": movie.video,
            "vote_average": movie.vote_average,
            "vote_count": movie.vote_count,
            "_created_at": movie._created_at.isoformat() if movie._created_at else None,
            "_updated_at": movie._updated_at.isoformat() if movie._updated_at else None,
            "_deleted_at": movie._deleted_at.isoformat() if movie._deleted_at else None,
        }
        return jsonify(movie_data)
    else:
        return jsonify({"error": "Movie not found."}), 404


@movie.route("all", methods=["GET"])
def get_movie_all():
    key = request.args.get("key")
    value = request.args.get("value")
    if not key or not value:
        return jsonify({"error": "Both 'key' and 'value' query parameters are required."}), 400

    movies = Movie.get_all(key, value)
    if movies:
        movies_data = []
        for movie in movies:
            movie_data = {
                "adult": movie.adult,
                "backdrop_path": movie.backdrop_path,
                "budget": movie.budget,
                "homepage": movie.homepage,
                "id": movie.id,
                "imdb_id": movie.imdb_id,
                "original_language": movie.original_language,
                "original_title": movie.original_title,
                "overview": movie.overview,
                "popularity": movie.popularity,
                "poster_path": movie.poster_path,
                "release_date": movie.release_date,
                "revenue": movie.revenue,
                "runtime": movie.runtime,
                "status": movie.status,
                "tagline": movie.tagline,
                "title": movie.title,
                "video": movie.video,
                "vote_average": movie.vote_average,
                "vote_count": movie.vote_count,
                "_created_at": movie._created_at.isoformat() if movie._created_at else None,
                "_updated_at": movie._updated_at.isoformat() if movie._updated_at else None,
                "_deleted_at": movie._deleted_at.isoformat() if movie._deleted_at else None,
            }
            movies_data.append(movie_data)
        return jsonify(movies_data)
    else:
        return jsonify({"error": "Movie not found."}), 404
