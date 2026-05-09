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
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "10000"))
MAX_PAGE_LIMIT = int(os.getenv("MAX_PAGE_LIMIT", "500"))
MAX_PAYLOAD_SIZE = int(os.getenv("MAX_PAYLOAD_SIZE", str(1024 * 1024)))
MAX_EVENT_NAME_LENGTH = int(os.getenv("MAX_EVENT_NAME_LENGTH", "200"))


@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "analytics-py", "timestamp": datetime.now(timezone.utc).isoformat()})


@app.route("/api/events", methods=["POST"])
def track_event():
    content_length = request.content_length or 0
    if content_length > MAX_PAYLOAD_SIZE:
        logger.warning("Payload too large: %d bytes (max %d)", content_length, MAX_PAYLOAD_SIZE)
        return jsonify({"error": "Payload too large", "max_bytes": MAX_PAYLOAD_SIZE}), 413

    data = request.get_json(silent=True)
    if not data or "event_name" not in data:
        logger.warning("Invalid event payload received")
        return jsonify({"error": "event_name is required"}), 400

    event_name = data["event_name"]
    if not isinstance(event_name, str):
        logger.warning("event_name has invalid type: %s", type(event_name).__name__)
        return jsonify({"error": "event_name must be a string"}), 400

    normalized_name = event_name.strip()
    if not normalized_name:
        logger.warning("event_name is blank")
        return jsonify({"error": "event_name must not be blank"}), 400

    if len(normalized_name) > MAX_EVENT_NAME_LENGTH:
        logger.warning("event_name too long: %d chars (max %d)", len(normalized_name), MAX_EVENT_NAME_LENGTH)
        return jsonify({
            "error": "event_name is too long",
            "max_length": MAX_EVENT_NAME_LENGTH,
        }), 400

    properties = data.get("properties", {})
    if properties is None:
        properties = {}
    if not isinstance(properties, dict):
        logger.warning("properties has invalid type: %s", type(properties).__name__)
        return jsonify({"error": "properties must be an object"}), 400

    event = {
        "event_name": normalized_name,
        "properties": properties,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    events_store.append(event)

    if len(events_store) > MAX_EVENTS:
        removed = len(events_store) - MAX_EVENTS
        del events_store[:removed]
        logger.info("Evicted %d old events (store capped at %d)", removed, MAX_EVENTS)

    logger.info("Tracked event: %s", event["event_name"])
    return jsonify({"message": "Event tracked", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def list_events():
    event_name = request.args.get("event_name")
    limit = request.args.get("limit", DEFAULT_PAGE_LIMIT, type=int)
    offset = request.args.get("offset", 0, type=int)

    if limit < 0:
        limit = DEFAULT_PAGE_LIMIT
    if limit > MAX_PAGE_LIMIT:
        limit = MAX_PAGE_LIMIT
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
