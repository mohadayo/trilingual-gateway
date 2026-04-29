import os
import logging
from datetime import datetime, timezone
from flask import Flask, jsonify, request

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analytics")

app = Flask(__name__)

events_store: list[dict] = []

DEFAULT_PAGE_LIMIT = int(os.getenv("DEFAULT_PAGE_LIMIT", "50"))


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "analytics-py", "timestamp": datetime.now(timezone.utc).isoformat()})


@app.route("/api/events", methods=["POST"])
def track_event():
    data = request.get_json()
    if not data or "event_name" not in data:
        logger.warning("Invalid event payload received")
        return jsonify({"error": "event_name is required"}), 400

    event = {
        "event_name": data["event_name"],
        "properties": data.get("properties", {}),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    events_store.append(event)
    logger.info("Tracked event: %s", event["event_name"])
    return jsonify({"message": "Event tracked", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def list_events():
    event_name = request.args.get("event_name")
    limit = request.args.get("limit", DEFAULT_PAGE_LIMIT, type=int)
    offset = request.args.get("offset", 0, type=int)

    if limit < 0:
        limit = DEFAULT_PAGE_LIMIT
    if offset < 0:
        offset = 0

    filtered = events_store
    if event_name:
        filtered = [e for e in events_store if e["event_name"] == event_name]

    total = len(filtered)
    paginated = filtered[offset:offset + limit]

    return jsonify({"events": paginated, "count": len(paginated), "total": total, "limit": limit, "offset": offset})


@app.route("/api/events", methods=["DELETE"])
def delete_events():
    event_name = request.args.get("event_name")
    if not event_name:
        logger.warning("Delete request missing event_name parameter")
        return jsonify({"error": "event_name query parameter is required"}), 400

    before_count = len(events_store)
    events_store[:] = [e for e in events_store if e["event_name"] != event_name]
    deleted_count = before_count - len(events_store)

    if deleted_count == 0:
        logger.info("No events found for deletion: %s", event_name)
        return jsonify({"error": "No events found with the specified event_name"}), 404

    logger.info("Deleted %d events with event_name=%s", deleted_count, event_name)
    return jsonify({"message": "Events deleted", "deleted_count": deleted_count})


@app.route("/api/events/summary", methods=["GET"])
def events_summary():
    summary: dict[str, int] = {}
    for event in events_store:
        name = event["event_name"]
        summary[name] = summary.get(name, 0) + 1
    logger.info("Summary requested, %d unique event types", len(summary))
    return jsonify({"summary": summary, "total_events": len(events_store)})


if __name__ == "__main__":
    port = int(os.getenv("ANALYTICS_PORT", "8001"))
    logger.info("Starting analytics service on port %d", port)
    app.run(host="0.0.0.0", port=port)
