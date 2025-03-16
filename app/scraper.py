from app import db
from app.model import BaseModel
import time
import requests
from app.config import Config
from app.logger import logger
from app.movie import Movie
from app.invalid import Invalid  # Existing invalid model
from app import db, create_app


class ScraperRecord(BaseModel):
    __tablename__ = "scraper"

    start_id = db.Column(db.BigInteger, nullable=True)
    end_id = db.Column(db.BigInteger, nullable=True)
    max_requests_per_second = db.Column(db.Integer, nullable=True)
    scrape_type = db.Column(db.String(50), nullable=True)
    consecutive_invalid_threshold = db.Column(db.Integer, nullable=True)
    total_requests = db.Column(db.Integer, default=0)
    total_request_time = db.Column(db.Float, default=0.0)
    items_scraped = db.Column(db.Integer, default=0)
    consecutive_invalid = db.Column(db.Integer, default=0)
    cancelled = db.Column(db.Boolean, default=False)


class Scraper:
    def __init__(self, start_id=1, end_id=1000000000000, max_requests_per_second=30,
                 scrape_type="missing", consecutive_invalid_threshold=5000):
        """
        scrape_type options:
          - "missing": Only scrape movie IDs missing in the Movie table (and not marked as invalid).
          - "fresh": Scrape all IDs regardless of existing records, except those marked as invalid.
        consecutive_invalid_threshold: number of consecutive 404 responses to consider as end-of-scrape.
        """
        self.start_id = start_id
        self.end_id = end_id
        self.max_requests_per_second = max_requests_per_second
        self.scrape_type = scrape_type.lower()
        self.consecutive_invalid_threshold = consecutive_invalid_threshold

        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {Config.API_TOKEN}"
        }
        self.items_scraped = 0
        self.consecutive_invalid = 0
        self.cancelled = False  # Local flag

        # Track request performance.
        self.total_requests = 0
        self.total_request_time = 0.0

        # Track only the consecutive invalid IDs (current block).
        self.consecutive_invalid_ids = set()

        # Create a scraper record in the DB (we have only _id).
        self.record = ScraperRecord.create({
            "start_id": self.start_id,
            "end_id": self.end_id,
            "max_requests_per_second": self.max_requests_per_second,
            "scrape_type": self.scrape_type,
            "consecutive_invalid_threshold": self.consecutive_invalid_threshold,
            "total_requests": 0,
            "total_request_time": 0.0,
            "items_scraped": 0,
            "consecutive_invalid": 0,
            "cancelled": False
        })
        logger.info(f"Created scraper record with _id {self.record._id}")

    def cancel(self):
        self.cancelled = True
        # Also update the record in the DB
        self.record.update({"cancelled": True})
        logger.info("Scraper record marked as cancelled.")

    def check_cancelled(self):
        # Re-query the record by _id to see if it has been cancelled externally.
        updated_record = ScraperRecord.get("_id", self.record._id)
        return updated_record.cancelled

    def get_invalid_ids(self):
        return {inv.movie_id for inv in Invalid.query.with_entities(Invalid.movie_id).all()}

    def get_existing_movie_ids(self):
        return {m[0] for m in Movie.query.with_entities(Movie.id).all()}

    def remove_consecutive_invalids(self):
        """Remove the invalid records created in the current consecutive block."""
        try:
            removed_count = 0
            for movie_id in self.consecutive_invalid_ids:
                rec = Invalid.query.filter_by(movie_id=movie_id).first()
                if rec:
                    db.session.delete(rec)
                    removed_count += 1
            db.session.commit()
            logger.info(
                f"Removed {removed_count} consecutive invalid records from this run.")
            self.consecutive_invalid_ids.clear()
        except Exception as e:
            logger.error(f"Error removing invalid records: {e}")

    def run(self):
        app = create_app()
        with app.app_context():
            start_time = time.time()
            refresh_interval = 100  # Refresh known IDs every 100 iterations.
            iteration_count = 0  # total iterations (attempted IDs)
            processed_count = 0  # IDs for which fetch_movie was actually called
            self.items_scraped = 100
            current_id = self.start_id
            if self.scrape_type == "missing":
                known_ids = self.get_existing_movie_ids().union(self.get_invalid_ids())
                logger.info(
                    f"Initial known IDs in 'missing' mode: {len(known_ids)}")
            elif self.scrape_type == "fresh":
                known_ids = self.get_invalid_ids()
                logger.info(
                    f"Initial known IDs in 'fresh' mode: {len(known_ids)}")
            else:
                logger.error(
                    "Invalid scrape_type provided. Use 'missing' or 'fresh'.")
                return

            while current_id <= self.end_id:
                iteration_count += 1

                # Periodically refresh known IDs
                if iteration_count % refresh_interval == 0:
                    if self.scrape_type == "missing":
                        known_ids = self.get_existing_movie_ids().union(self.get_invalid_ids())
                    elif self.scrape_type == "fresh":
                        known_ids = self.get_invalid_ids()

                    # Also update the scraper record every refresh interval
                    self.record.update({
                        "total_requests": self.total_requests,
                        "total_request_time": self.total_request_time,
                        "items_scraped": self.items_scraped,
                        "consecutive_invalid": self.consecutive_invalid
                    })

                # Skip if known
                if current_id in known_ids:
                    current_id += 1
                    continue

                # Check if externally cancelled
                if self.check_cancelled():
                    logger.info("Scraping cancelled via scraper record.")
                    break

                # Fetch
                self.fetch_movie(current_id)
                processed_count += 1
                current_id += 1

                if processed_count % self.max_requests_per_second == 0:
                    time.sleep(1)

                if self.consecutive_invalid >= self.consecutive_invalid_threshold:
                    logger.info(
                        f"Encountered {self.consecutive_invalid} consecutive invalid errors. "
                        f"Removing the consecutive invalid records and stopping further processing."
                    )
                    self.remove_consecutive_invalids()
                    break

            elapsed_time = time.time() - start_time
            rps = self.total_requests / elapsed_time if elapsed_time > 0 else 0
            avg_req_time = self.total_request_time / \
                self.total_requests if self.total_requests else 0

            logger.info(f"Movies scraped this session: {self.items_scraped}")
            logger.info(
                f"Processed movie IDs: {processed_count}, Skipped movie IDs: {iteration_count - processed_count}"
            )
            logger.info(
                f"Total requests: {self.total_requests}, Elapsed time: {elapsed_time:.2f} seconds, "
                f"Requests per second: {rps:.2f}, Average request time: {avg_req_time:.2f} seconds."
            )

            # Final record update at the end
            self.record.update({
                "total_requests": self.total_requests,
                "total_request_time": self.total_request_time,
                "items_scraped": self.items_scraped,
                "consecutive_invalid": self.consecutive_invalid
            })

    def fetch_movie(self, movie_id):
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
        logger.debug(
            f"Starting fetch for movie ID {movie_id} using URL: {url}")
        while True:
            try:
                req_start = time.time()
                response = requests.get(url, headers=self.headers)
                req_duration = time.time() - req_start

                self.total_requests += 1
                self.total_request_time += req_duration

                if response.status_code == 200:
                    data = response.json()
                    try:
                        Movie.upsert("id", data)
                        self.items_scraped += 1
                        self.record.update(
                            {"items_scraped": self.items_scraped}
                        )
                        self.consecutive_invalid = 0
                        self.consecutive_invalid_ids.clear()
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
                    self.consecutive_invalid_ids.clear()
                    time.sleep(30)
                    continue
                elif response.status_code == 404:
                    logger.warning(
                        f"Movie ID {movie_id} returned 404. Storing as invalid.")
                    self.consecutive_invalid += 1
                    self.consecutive_invalid_ids.add(movie_id)
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
                    self.consecutive_invalid_ids.clear()
                    break
            except Exception as e:
                logger.error(f"Error fetching movie ID {movie_id}: {e}")
                self.consecutive_invalid = 0
                self.consecutive_invalid_ids.clear()
                break
