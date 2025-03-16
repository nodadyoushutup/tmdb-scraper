from flask import Blueprint, request, jsonify
from app.scraper import Scraper
import threading

# Global variable to store the current scraper instance.
current_scraper_instance = None

scraper = Blueprint("scraper", __name__, url_prefix="/scraper")


def start_scraper(scrape_type):
    global current_scraper_instance
    data = request.get_json(silent=True) or {}
    start_id = request.args.get("start_id", data.get("start_id", 1))
    end_id = request.args.get("end_id", data.get("end_id", 1000000000000))
    max_rps = request.args.get(
        "max_requests_per_second",
        data.get("max_requests_per_second", 30)
    )

    start_id = int(start_id)
    end_id = int(end_id)
    max_rps = int(max_rps)

    scraper_instance = Scraper(
        start_id=start_id,
        end_id=end_id,
        max_requests_per_second=max_rps,
        scrape_type=scrape_type
    )
    current_scraper_instance = scraper_instance

    # Start scraper.run() in a new daemon thread so that the scraping process runs in the background.
    thread = threading.Thread(target=scraper_instance.run)
    thread.daemon = True
    thread.start()
    return scraper_instance


@scraper.route("/scrape", methods=["POST"])
def trigger_scrape_alias():
    """
    /scrape endpoint alias for a fresh scan.
    """
    try:
        scraper_instance = start_scraper("fresh")
        return jsonify({
            "message": "Scraping started (fresh scan).",
            "items_scraped": scraper_instance.items_scraped
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@scraper.route("/fresh", methods=["POST"])
def trigger_fresh_scraper():
    """
    /fresh endpoint for doing a fresh scan.
    """
    try:
        scraper_instance = start_scraper("fresh")
        return jsonify({
            "message": "Fresh scraping started.",
            "items_scraped": scraper_instance.items_scraped
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@scraper.route("/missing", methods=["POST"])
def trigger_missing_scraper():
    """
    /missing endpoint for only scraping missing IDs.
    """
    try:
        scraper_instance = start_scraper("missing")
        return jsonify({
            "message": "Missing scraping started.",
            "items_scraped": scraper_instance.items_scraped
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@scraper.route("/cancel", methods=["POST"])
def cancel_scraper():
    global current_scraper_instance
    if current_scraper_instance is not None:
        current_scraper_instance.cancel()
        return jsonify({"message": "Scraping cancellation triggered."}), 200
    else:
        return jsonify({"error": "No scraping process running."}), 400
