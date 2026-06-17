"""trilingual-gateway/services/analytics-py の Flask アプリの回帰テスト。

`app.test_client()` 経由で各エンドポイントの主要分岐を検証する。
各テストは `setup_function` でストアをクリアし、テスト間で状態を共有しない。
"""

import json

import pytest

from app import (
    MAX_EVENT_NAME_LENGTH,
    app,
    events_store,
)


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def setup_function(_func):
    events_store.clear()


def _post_event(client, name, properties=None):
    payload = {"event_name": name}
    if properties is not None:
        payload["properties"] = properties
    return client.post(
        "/api/events",
        data=json.dumps(payload),
        content_type="application/json",
    )


# ---- /health ----


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["service"] == "analytics-py"
    assert "timestamp" in body


# ---- POST /api/events ----


def test_track_event_success(client):
    resp = _post_event(client, "page_view", {"path": "/home"})
    assert resp.status_code == 201
    body = resp.get_json()
    assert body["message"] == "Event tracked"
    assert body["event"]["event_name"] == "page_view"
    assert body["event"]["properties"] == {"path": "/home"}
    assert "timestamp" in body["event"]
    assert len(events_store) == 1


def test_track_event_missing_event_name(client):
    resp = client.post(
        "/api/events",
        data=json.dumps({"properties": {}}),
        content_type="application/json",
    )
    assert resp.status_code == 400
    assert "event_name" in resp.get_json()["error"]


def test_track_event_event_name_wrong_type(client):
    resp = client.post(
        "/api/events",
        data=json.dumps({"event_name": 123}),
        content_type="application/json",
    )
    assert resp.status_code == 400


def test_track_event_event_name_blank(client):
    resp = _post_event(client, "   ")
    assert resp.status_code == 400


def test_track_event_event_name_too_long(client):
    resp = _post_event(client, "x" * (MAX_EVENT_NAME_LENGTH + 1))
    assert resp.status_code == 400
    assert resp.get_json()["max_length"] == MAX_EVENT_NAME_LENGTH


def test_track_event_properties_wrong_type(client):
    resp = client.post(
        "/api/events",
        data=json.dumps({"event_name": "x", "properties": "not-a-dict"}),
        content_type="application/json",
    )
    assert resp.status_code == 400


def test_track_event_properties_null_is_normalized(client):
    resp = client.post(
        "/api/events",
        data=json.dumps({"event_name": "x", "properties": None}),
        content_type="application/json",
    )
    assert resp.status_code == 201
    assert resp.get_json()["event"]["properties"] == {}


def test_track_event_invalid_json(client):
    resp = client.post(
        "/api/events",
        data="not json",
        content_type="application/json",
    )
    assert resp.status_code == 400


# ---- GET /api/events ----


def test_list_events_returns_recently_tracked(client):
    _post_event(client, "a")
    _post_event(client, "b")
    resp = client.get("/api/events")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 2
    assert body["count"] == 2
    names = [e["event_name"] for e in body["events"]]
    assert set(names) == {"a", "b"}


def test_list_events_filter_by_event_name(client):
    _post_event(client, "click")
    _post_event(client, "view")
    _post_event(client, "click")
    resp = client.get("/api/events?event_name=click")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 2
    assert all(e["event_name"] == "click" for e in body["events"])


def test_list_events_q_is_case_insensitive_substring(client):
    _post_event(client, "PageView")
    _post_event(client, "purchase")
    _post_event(client, "click")
    resp = client.get("/api/events?q=page")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 1
    assert body["events"][0]["event_name"] == "PageView"


def test_list_events_invalid_sort_field_returns_400(client):
    resp = client.get("/api/events?sort=bogus")
    assert resp.status_code == 400


def test_list_events_invalid_sort_order_returns_400(client):
    resp = client.get("/api/events?order=bogus")
    assert resp.status_code == 400


def test_list_events_blank_q_returns_400(client):
    resp = client.get("/api/events?q=   ")
    assert resp.status_code == 400


def test_list_events_since_greater_than_until_returns_400(client):
    resp = client.get(
        "/api/events?since=2026-06-10T00:00:00Z&until=2026-06-01T00:00:00Z"
    )
    assert resp.status_code == 400


def test_list_events_invalid_since_format_returns_400(client):
    resp = client.get("/api/events?since=not-a-date")
    assert resp.status_code == 400


# ---- DELETE /api/events ----


def test_delete_events_requires_at_least_one_filter(client):
    _post_event(client, "a")
    resp = client.delete("/api/events")
    assert resp.status_code == 400
    assert len(events_store) == 1


def test_delete_events_by_event_name(client):
    _post_event(client, "click")
    _post_event(client, "view")
    _post_event(client, "click")
    resp = client.delete("/api/events?event_name=click")
    assert resp.status_code == 200
    remaining = [e["event_name"] for e in events_store]
    assert remaining == ["view"]


# ---- GET /api/events/count ----


def test_count_events_returns_total_and_by_name(client):
    _post_event(client, "a")
    _post_event(client, "a")
    _post_event(client, "b")
    resp = client.get("/api/events/count")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 3
    assert body["distinct_names"] == 2
    assert body["by_name"] == {"a": 2, "b": 1}


def test_count_events_filter_by_q(client):
    _post_event(client, "click_home")
    _post_event(client, "click_pricing")
    _post_event(client, "view")
    resp = client.get("/api/events/count?q=click")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 2
    assert body["distinct_names"] == 2


def test_count_events_blank_q_returns_400(client):
    resp = client.get("/api/events/count?q=  ")
    assert resp.status_code == 400


def test_count_events_since_greater_than_until_returns_400(client):
    resp = client.get(
        "/api/events/count?since=2026-06-10T00:00:00Z&until=2026-06-01T00:00:00Z"
    )
    assert resp.status_code == 400


# ---- GET /api/events/summary ----


def test_summary_aggregates_by_name(client):
    _post_event(client, "signup")
    _post_event(client, "signup")
    _post_event(client, "login")
    resp = client.get("/api/events/summary")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total_events"] == 3
    assert body["summary"]["signup"] == 2
    assert body["summary"]["login"] == 1


def test_summary_with_event_name_filter(client):
    _post_event(client, "signup")
    _post_event(client, "login")
    resp = client.get("/api/events/summary?event_name=signup")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total_events"] == 1
    assert body["summary"] == {"signup": 1}


# ---- GET /api/events/names ----


def test_list_event_names_returns_distinct_sorted(client):
    _post_event(client, "view")
    _post_event(client, "click")
    _post_event(client, "view")
    resp = client.get("/api/events/names")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["names"] == ["click", "view"]
    assert body["total"] == 2


def test_list_event_names_desc_order(client):
    _post_event(client, "view")
    _post_event(client, "click")
    resp = client.get("/api/events/names?order=desc")
    assert resp.status_code == 200
    assert resp.get_json()["names"] == ["view", "click"]


# ---- GET /api/events/names/<name> ----


def test_event_name_detail_returns_first_last_and_distinct_keys(client):
    _post_event(client, "page_view", {"path": "/home", "user_id": "u1"})
    _post_event(client, "page_view", {"path": "/pricing"})
    resp = client.get("/api/events/names/page_view")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["count"] == 2
    assert sorted(body["distinct_property_keys"]) == ["path", "user_id"]
    assert body["latest_properties"] == {"path": "/pricing"}


def test_event_name_detail_unknown_returns_404(client):
    _post_event(client, "page_view")
    resp = client.get("/api/events/names/missing")
    assert resp.status_code == 404


# ---- GET /api/events/property_keys ----


def test_list_property_keys_counts_events_per_key(client):
    _post_event(client, "view", {"path": "/", "user_id": "u1"})
    _post_event(client, "click", {"path": "/pricing"})
    _post_event(client, "click", {"x": 1})
    resp = client.get("/api/events/property_keys")
    assert resp.status_code == 200
    body = resp.get_json()
    by_key = {item["key"]: item["count"] for item in body["property_keys"]}
    assert by_key["path"] == 2
    assert by_key["user_id"] == 1
    assert by_key["x"] == 1
    assert body["distinct_property_keys"] == 3


def test_list_property_keys_invalid_sort_order_returns_400(client):
    resp = client.get("/api/events/property_keys?order=bogus")
    assert resp.status_code == 400


# ---- GET /api/events/property_values/<key> ----


def test_list_property_values_counts_distinct_values_for_key(client):
    _post_event(client, "view", {"path": "/"})
    _post_event(client, "view", {"path": "/"})
    _post_event(client, "view", {"path": "/pricing"})
    _post_event(client, "view", {"other_key": "ignored"})
    resp = client.get("/api/events/property_values/path")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["events_with_key"] == 3
    assert body["distinct_property_values"] == 2
    assert body["property_values"][0]["value"] == "/"
    assert body["property_values"][0]["count"] == 2


def test_list_property_values_skips_non_scalar_values(client):
    _post_event(client, "view", {"tags": ["a", "b"]})
    _post_event(client, "view", {"tags": {"k": "v"}})
    _post_event(client, "view", {"tags": "scalar"})
    resp = client.get("/api/events/property_values/tags")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["events_with_key"] == 3
    assert body["distinct_property_values"] == 1
    assert body["property_values"][0]["value"] == "scalar"


def test_list_property_values_blank_key_returns_400(client):
    resp = client.get("/api/events/property_values/   ")
    assert resp.status_code == 400


def test_list_property_values_invalid_sort_field_returns_400(client):
    resp = client.get("/api/events/property_values/path?sort=bogus")
    assert resp.status_code == 400


def test_list_property_values_missing_key_returns_empty_distribution(client):
    _post_event(client, "view", {"other": 1})
    resp = client.get("/api/events/property_values/missing_key")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["events_with_key"] == 0
    assert body["distinct_property_values"] == 0
    assert body["property_values"] == []
