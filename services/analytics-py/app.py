import os
import logging
import threading
from datetime import datetime, timezone
from flask import Flask, jsonify, request

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analytics")

app = Flask(__name__)

events_store: list[dict] = []
events_lock = threading.Lock()

DEFAULT_PAGE_LIMIT = int(os.getenv("DEFAULT_PAGE_LIMIT", "50"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "10000"))
MAX_PAGE_LIMIT = int(os.getenv("MAX_PAGE_LIMIT", "500"))
MAX_PAYLOAD_SIZE = int(os.getenv("MAX_PAYLOAD_SIZE", str(1024 * 1024)))
MAX_EVENT_NAME_LENGTH = int(os.getenv("MAX_EVENT_NAME_LENGTH", "200"))
ALLOWED_SORT_FIELDS = {"timestamp", "event_name"}
ALLOWED_SORT_ORDERS = {"asc", "desc"}


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
    with events_lock:
        events_store.append(event)
        if len(events_store) > MAX_EVENTS:
            removed = len(events_store) - MAX_EVENTS
            del events_store[:removed]
            logger.info("Evicted %d old events (store capped at %d)", removed, MAX_EVENTS)

    logger.info("Tracked event: %s", event["event_name"])
    return jsonify({"message": "Event tracked", "event": event}), 201


def _parse_iso_datetime(value: str, name: str) -> datetime:
    raw = value.strip()
    if not raw:
        raise ValueError(f"'{name}' must not be blank")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"'{name}' must be an ISO8601 datetime: {exc}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _filter_events_by_time(events: list[dict], since: datetime | None, until: datetime | None) -> list[dict]:
    if since is None and until is None:
        return events
    kept = []
    for e in events:
        try:
            ts = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
        except (ValueError, AttributeError, KeyError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if since is not None and ts < since:
            continue
        if until is not None and ts > until:
            continue
        kept.append(e)
    return kept


@app.route("/api/events", methods=["GET"])
def list_events():
    event_name = request.args.get("event_name")
    limit = request.args.get("limit", DEFAULT_PAGE_LIMIT, type=int)
    offset = request.args.get("offset", 0, type=int)
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")
    sort_field = request.args.get("sort", "timestamp")
    sort_order = request.args.get("order", "asc")

    if sort_field not in ALLOWED_SORT_FIELDS:
        logger.warning("Invalid sort field: %s", sort_field)
        return jsonify({
            "error": "Invalid sort field",
            "allowed": sorted(ALLOWED_SORT_FIELDS),
        }), 400
    if sort_order not in ALLOWED_SORT_ORDERS:
        logger.warning("Invalid sort order: %s", sort_order)
        return jsonify({
            "error": "Invalid sort order",
            "allowed": sorted(ALLOWED_SORT_ORDERS),
        }), 400

    if limit < 0:
        limit = DEFAULT_PAGE_LIMIT
    if limit > MAX_PAGE_LIMIT:
        limit = MAX_PAGE_LIMIT
    if offset < 0:
        offset = 0

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_time(filtered, since, until)

    reverse = sort_order == "desc"
    filtered.sort(key=lambda e: e.get(sort_field, ""), reverse=reverse)

    total = len(filtered)
    paginated = filtered[offset:offset + limit]

    return jsonify({
        "events": paginated,
        "count": len(paginated),
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort_field,
        "order": sort_order,
    })


@app.route("/api/events", methods=["DELETE"])
def delete_events():
    event_name = request.args.get("event_name")
    if not event_name:
        logger.warning("Delete request missing event_name parameter")
        return jsonify({"error": "event_name query parameter is required"}), 400

    with events_lock:
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
    event_name = request.args.get("event_name")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on summary: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on summary: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_time(filtered, since, until)

    summary: dict[str, int] = {}
    for event in filtered:
        name = event["event_name"]
        summary[name] = summary.get(name, 0) + 1
    logger.info("Summary requested, %d unique event types", len(summary))
    return jsonify({"summary": summary, "total_events": len(filtered)})


if __name__ == "__main__":
    port = int(os.getenv("ANALYTICS_PORT", "8001"))
    logger.info("Starting analytics service on port %d", port)
    app.run(host="0.0.0.0", port=port)
