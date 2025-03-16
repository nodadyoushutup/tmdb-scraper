import os
import logging

# ---------------- Logging Setup ----------------

TERMINAL_LOG_LEVEL = logging.INFO

# Create the log/ directory if it doesn't exist
log_dir = "log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create a logger and set its base level to DEBUG (so all messages are processed)
logger = logging.getLogger("movie_scraper")
logger.setLevel(logging.DEBUG)

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
levels = [logging.CRITICAL, logging.ERROR,
          logging.WARNING, logging.INFO, logging.DEBUG]
level_names = {
    logging.CRITICAL: "critical",
    logging.ERROR: "error",
    logging.WARNING: "warning",
    logging.INFO: "info",
    logging.DEBUG: "debug"
}

for level in levels:
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"{level_names[level]}.log"))
    file_handler.setLevel(logging.DEBUG)
    file_handler.addFilter(LevelFilter(level))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# Additional file handler for rate limit logs (records only genuine 429 errors)
rate_handler = logging.FileHandler(os.path.join(log_dir, "rate.log"))
rate_handler.setLevel(logging.DEBUG)
rate_handler.addFilter(
    lambda record: record.levelno == logging.WARNING and "rate limit hit" in record.getMessage()
)
rate_handler.setFormatter(formatter)
logger.addHandler(rate_handler)

# Additional file handler for skip logs (records any log message that mentions 'Skipping movie')
skip_handler = logging.FileHandler(os.path.join(log_dir, "skip.log"))
skip_handler.setLevel(logging.DEBUG)
skip_handler.addFilter(lambda record: "Skipping movie" in record.getMessage())
skip_handler.setFormatter(formatter)
logger.addHandler(skip_handler)
