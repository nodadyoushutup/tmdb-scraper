import asyncio
import aiohttp
import os
import json
import logging
from app.config import token

# ---------------- Logging Setup ----------------

# Terminal log level can be adjusted here:
TERMINAL_LOG_LEVEL = logging.INFO  # Change this to logging.DEBUG, logging.WARNING, etc.

# Create the log/ directory if it doesn't exist
log_dir = "log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create a logger and set its base level to DEBUG (so all messages are processed)
logger = logging.getLogger("movie_scraper")
logger.setLevel(logging.DEBUG)

# Define a formatter for log messages
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Terminal (console) handler with configurable level
console_handler = logging.StreamHandler()
console_handler.setLevel(TERMINAL_LOG_LEVEL)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Create a filter class to allow only messages of a specific level to pass
class LevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level
    def filter(self, record):
        return record.levelno == self.level

# Create file handlers for each log level: CRITICAL, ERROR, WARNING, INFO, DEBUG
levels = [logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
level_names = {
    logging.CRITICAL: "critical",
    logging.ERROR: "error",
    logging.WARNING: "warning",
    logging.INFO: "info",
    logging.DEBUG: "debug"
}

for level in levels:
    file_handler = logging.FileHandler(os.path.join(log_dir, f"{level_names[level]}.log"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.addFilter(LevelFilter(level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Additional file handler for rate limit logs (records only genuine 429 errors)
rate_handler = logging.FileHandler(os.path.join(log_dir, "rate.log"))
rate_handler.setLevel(logging.DEBUG)
rate_handler.addFilter(lambda record: record.levelno == logging.WARNING and "rate limit hit" in record.getMessage())
rate_handler.setFormatter(formatter)
logger.addHandler(rate_handler)

# Additional file handler for skip logs (records any log message that mentions 'Skipping movie')
skip_handler = logging.FileHandler(os.path.join(log_dir, "skip.log"))
skip_handler.setLevel(logging.DEBUG)
skip_handler.addFilter(lambda record: "Skipping movie" in record.getMessage())
skip_handler.setFormatter(formatter)
logger.addHandler(skip_handler)

# ---------------- Application Setup ----------------

# Parameters
start_id = 1
end_id = 1000000000000
max_requests_per_second = 30

# Create the json/ directory if it doesn't exist
output_dir = "json"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Create the 404/ directory if it doesn't exist
error_dir = "404"
if not os.path.exists(error_dir):
    os.makedirs(error_dir)

# API headers
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {token}"
}

# Global counters
items_scraped = 0
consecutive_404 = 0  # Counts consecutive 404 errors

def get_existing_ids(directory):
    """Scans the directory and returns a set of movie IDs (as integers) for which a JSON file exists."""
    existing = set()
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            try:
                movie_id = int(filename.split('.')[0])
                existing.add(movie_id)
            except ValueError:
                continue
    return existing

def get_known_404_ids(directory):
    """Scans the directory and returns a set of movie IDs (as integers) for which a 404 file exists."""
    known = set()
    for filename in os.listdir(directory):
        if filename.endswith(".txt"):
            try:
                movie_id = int(filename.split('.')[0])
                known.add(movie_id)
            except ValueError:
                continue
    return known

def missing_ids_generator(start, end, existing_ids, known_404_ids):
    """Yields IDs in the range [start, end] that are not in existing_ids or known_404_ids."""
    for movie_id in range(start, end + 1):
        if movie_id not in existing_ids and movie_id not in known_404_ids:
            yield movie_id

async def fetch_movie(session, movie_id):
    global items_scraped, consecutive_404
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
    logger.debug(f"Starting fetch for movie ID {movie_id} using URL: {url}")
    while True:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    file_path = os.path.join(output_dir, f"{movie_id}.json")
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)
                    logger.info(f"Response for movie ID {movie_id} saved to: {file_path}")
                    items_scraped += 1
                    consecutive_404 = 0  # Reset counter on a successful fetch
                    break  # Exit loop on success
                elif response.status == 429:
                    logger.warning(f"429 rate limit hit for movie ID {movie_id}. Pausing for 30 seconds and retrying...")
                    consecutive_404 = 0  # Break any 404 chain
                    await asyncio.sleep(30)
                    continue  # Retry after pause
                elif response.status == 404:
                    logger.error(f"Movie ID {movie_id} returned 404. Marking as not found.")
                    # Write a file in the 404/ directory to record the known 404
                    error_file_path = os.path.join(error_dir, f"{movie_id}.txt")
                    with open(error_file_path, "w", encoding="utf-8") as f:
                        f.write("404 Not Found")
                    consecutive_404 += 1
                    break
                else:
                    logger.error(f"Movie ID {movie_id} returned status code: {response.status}")
                    consecutive_404 = 0
                    break
        except Exception as e:
            logger.error(f"Error fetching movie ID {movie_id}: {e}")
            consecutive_404 = 0
            break

async def main():
    global consecutive_404
    # Pre-compute which movie IDs already have a file in the json/ directory and known 404s in error_dir
    existing_ids = get_existing_ids(output_dir)
    known_404_ids = get_known_404_ids(error_dir)
    total_range = end_id - start_id + 1
    missing_count = total_range - len(existing_ids) - len(known_404_ids)
    logger.info(f"Found {len(existing_ids)} existing files and {len(known_404_ids)} known 404s. {missing_count} missing files in range {start_id} to {end_id}.")

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = []
        try:
            # Iterate only over missing movie IDs
            for movie_id in missing_ids_generator(start_id, end_id, existing_ids, known_404_ids):
                tasks.append(fetch_movie(session, movie_id))
                # Execute tasks in batches per max_requests_per_second
                if len(tasks) >= max_requests_per_second:
                    logger.debug(f"Executing batch of {len(tasks)} tasks.")
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
                    await asyncio.sleep(1)
                    # Check if we've encountered 5,000 consecutive 404 errors
                    if consecutive_404 >= 5000:
                        logger.info(f"Encountered {consecutive_404} consecutive 404 errors. Stopping further processing.")
                        break
        except KeyboardInterrupt:
            logger.info("Gracefully shutting down due to Ctrl+C...")
        finally:
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"Items scraped this session: {items_scraped}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Gracefully shutting down due to Ctrl+C...")
        logger.info(f"Items scraped this session: {items_scraped}")
