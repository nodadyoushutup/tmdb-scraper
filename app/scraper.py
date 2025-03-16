import time
import requests
from app.config import Config
from app.logger import logger
from app.movie import Movie
from app.invalid import Invalid  # Import the new Invalid model
from app import db, create_app


class Scraper:
    def __init__(self, start_id=1, end_id=1000000000000, max_requests_per_second=30, scrape_type="missing"):
        """
        scrape_type options:
          - "missing": Only scrape movie IDs that are missing in the Movie table (and not marked as invalid).
          - "fresh": Scrape all IDs regardless of existing records, except those marked as invalid.
        """
        self.start_id = start_id
        self.end_id = end_id
        self.max_requests_per_second = max_requests_per_second
        self.scrape_type = scrape_type.lower()

        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {Config.API_TOKEN}"
        }
        self.items_scraped = 0
        self.consecutive_invalid = 0
        self.cancelled = False  # Flag for cancellation

        # New variables to track request performance.
        self.total_requests = 0
        self.total_request_time = 0.0

    def cancel(self):
        self.cancelled = True

    def get_invalid_ids(self):
        # Return a set of all IDs that have been recorded as invalid.
        return {inv.movie_id for inv in Invalid.query.with_entities(Invalid.movie_id).all()}

    def get_existing_movie_ids(self):
        # Return a set of all valid movie IDs stored.
        return {m[0] for m in Movie.query.with_entities(Movie.id).all()}

    def run(self):
        app = create_app()
        with app.app_context():
            start_time = time.time()
            invalid_ids = self.get_invalid_ids()
            if self.scrape_type == "missing":
                existing_ids = self.get_existing_movie_ids().union(invalid_ids)
                total_to_scan = (self.end_id - self.start_id +
                                 1) - len(existing_ids)
                logger.info(
                    f"Scrape type 'missing': Found {len(existing_ids)} processed records (movies or invalid). "
                    f"Scanning {total_to_scan} movie IDs in range {self.start_id} to {self.end_id}."
                )
                id_iterator = (movie_id for movie_id in range(
                    self.start_id, self.end_id + 1) if movie_id not in existing_ids)
            elif self.scrape_type == "fresh":
                total_to_scan = (
                    self.end_id - self.start_id + 1) - len(invalid_ids)
                logger.info(
                    f"Scrape type 'fresh': Ignoring {len(invalid_ids)} invalid records. "
                    f"Scanning all {total_to_scan} movie IDs in range {self.start_id} to {self.end_id}."
                )
                id_iterator = (movie_id for movie_id in range(
                    self.start_id, self.end_id + 1) if movie_id not in invalid_ids)
            else:
                logger.error(
                    "Invalid scrape_type provided. Use 'missing' or 'fresh'.")
                return

            count = 0
            for movie_id in id_iterator:
                if self.cancelled:
                    logger.info("Scraping cancelled.")
                    break
                self.fetch_movie(movie_id)
                count += 1
                if count % self.max_requests_per_second == 0:
                    time.sleep(1)
                if self.consecutive_invalid >= 5000:
                    logger.info(
                        f"Encountered {self.consecutive_invalid} consecutive invalid errors. Stopping further processing."
                    )
                    break

            elapsed_time = time.time() - start_time
            rps = self.total_requests / elapsed_time if elapsed_time > 0 else 0
            avg_req_time = self.total_request_time / \
                self.total_requests if self.total_requests > 0 else 0

            logger.info(f"Movies scraped this session: {self.items_scraped}")
            logger.info(
                f"Total requests: {self.total_requests}, Elapsed time: {elapsed_time:.2f} seconds, "
                f"Requests per second: {rps:.2f}, Average request time: {avg_req_time:.2f} seconds."
            )

    def fetch_movie(self, movie_id):
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
        logger.debug(
            f"Starting fetch for movie ID {movie_id} using URL: {url}")
        while True:
            try:
                req_start = time.time()
                response = requests.get(url, headers=self.headers)
                req_duration = time.time() - req_start

                # Update request metrics
                self.total_requests += 1
                self.total_request_time += req_duration

                if response.status_code == 200:
                    data = response.json()
                    try:
                        # Upsert ensures that existing valid records are updated,
                        # and new records are created if they don't exist.
                        Movie.upsert("id", data)
                        self.items_scraped += 1
                        self.consecutive_invalid = 0
                        logger.info(
                            f"Movie ID {movie_id} stored/updated in database.")
                    except Exception as e:
                        logger.error(
                            f"Error storing movie ID {movie_id} in database: {e}")
                    break
                elif response.status_code == 429:
                    logger.error(
                        f"429 rate limit hit for movie ID {movie_id}. Pausing for 30 seconds and retrying...")
                    self.consecutive_invalid = 0
                    time.sleep(30)
                    continue
                elif response.status_code == 404:
                    logger.warning(
                        f"Movie ID {movie_id} returned 404. Storing as invalid.")
                    self.consecutive_invalid += 1
                    try:
                        Invalid.create({"movie_id": movie_id})
                        logger.info(
                            f"Movie ID {movie_id} recorded as invalid in the database.")
                    except Exception as e:
                        logger.error(
                            f"Error recording invalid for movie ID {movie_id}: {e}")
                    break
                else:
                    logger.error(
                        f"Movie ID {movie_id} returned status code: {response.status_code}")
                    self.consecutive_invalid = 0
                    break
            except Exception as e:
                logger.error(f"Error fetching movie ID {movie_id}: {e}")
                self.consecutive_invalid = 0
                break
