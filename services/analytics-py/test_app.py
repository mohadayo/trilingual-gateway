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


# ---- GET /api/events/by_day ----


def _seed_event_at(name, iso_ts, properties=None):
    """テスト用に events_store へ直接イベントを差し込むヘルパ。

    POST 経由だと timestamp が `datetime.now(timezone.utc)` で上書きされてしまい、
    日付ビニングのテストが書けないため、ストアに直接 push する。テスト規約として
    `setup_function` で毎回クリアされるので状態リークの心配は無い。
    """
    events_store.append({
        "event_name": name,
        "properties": properties or {},
        "timestamp": iso_ts,
    })


def test_events_by_day_empty_store_returns_empty(client):
    resp = client.get("/api/events/by_day")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body == {"total": 0, "distinct_days": 0, "by_day": []}


def test_events_by_day_groups_by_utc_date(client):
    _seed_event_at("login", "2026-06-20T10:00:00+00:00")
    _seed_event_at("login", "2026-06-20T23:59:59+00:00")
    _seed_event_at("login", "2026-06-21T00:00:00+00:00")
    resp = client.get("/api/events/by_day")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 3
    assert body["distinct_days"] == 2
    assert body["by_day"] == [
        {"day": "2026-06-20", "count": 2},
        {"day": "2026-06-21", "count": 1},
    ]


def test_events_by_day_sorted_lex_ascending(client):
    _seed_event_at("a", "2026-06-22T05:00:00+00:00")
    _seed_event_at("a", "2026-06-01T05:00:00+00:00")
    _seed_event_at("a", "2026-06-15T05:00:00+00:00")
    resp = client.get("/api/events/by_day")
    days = [row["day"] for row in resp.get_json()["by_day"]]
    assert days == ["2026-06-01", "2026-06-15", "2026-06-22"]


def test_events_by_day_filters_by_event_name(client):
    _seed_event_at("login", "2026-06-20T10:00:00+00:00")
    _seed_event_at("logout", "2026-06-20T11:00:00+00:00")
    _seed_event_at("login", "2026-06-21T10:00:00+00:00")
    resp = client.get("/api/events/by_day?event_name=login")
    body = resp.get_json()
    assert body["total"] == 2
    assert body["by_day"] == [
        {"day": "2026-06-20", "count": 1},
        {"day": "2026-06-21", "count": 1},
    ]


def test_events_by_day_filters_by_q_case_insensitive(client):
    _seed_event_at("UserSignup", "2026-06-20T10:00:00+00:00")
    _seed_event_at("user_login", "2026-06-20T11:00:00+00:00")
    _seed_event_at("page_view", "2026-06-21T10:00:00+00:00")
    resp = client.get("/api/events/by_day?q=USER")
    body = resp.get_json()
    assert body["total"] == 2
    assert body["distinct_days"] == 1


def test_events_by_day_filters_by_since_until(client):
    _seed_event_at("e", "2026-06-19T00:00:00+00:00")
    _seed_event_at("e", "2026-06-20T00:00:00+00:00")
    _seed_event_at("e", "2026-06-21T00:00:00+00:00")
    _seed_event_at("e", "2026-06-22T00:00:00+00:00")
    resp = client.get(
        "/api/events/by_day?since=2026-06-20T00:00:00Z&until=2026-06-21T23:59:59Z"
    )
    body = resp.get_json()
    assert body["total"] == 2
    assert body["by_day"] == [
        {"day": "2026-06-20", "count": 1},
        {"day": "2026-06-21", "count": 1},
    ]


def test_events_by_day_converts_non_utc_timestamps_to_utc(client):
    # JST 2026-06-21 08:00 → UTC 2026-06-20 23:00（前日になる）
    _seed_event_at("evt", "2026-06-21T08:00:00+09:00")
    # JST 2026-06-21 09:00 → UTC 2026-06-21 00:00（同日）
    _seed_event_at("evt", "2026-06-21T09:00:00+09:00")
    resp = client.get("/api/events/by_day")
    body = resp.get_json()
    assert body["distinct_days"] == 2
    days = {row["day"]: row["count"] for row in body["by_day"]}
    assert days == {"2026-06-20": 1, "2026-06-21": 1}


def test_events_by_day_ignores_broken_timestamps(client):
    _seed_event_at("good", "2026-06-20T10:00:00+00:00")
    _seed_event_at("bad_iso", "not-a-timestamp")
    events_store.append({"event_name": "missing_ts", "properties": {}})
    resp = client.get("/api/events/by_day")
    body = resp.get_json()
    assert body["total"] == 1
    assert body["by_day"] == [{"day": "2026-06-20", "count": 1}]


def test_events_by_day_invalid_since_returns_400(client):
    resp = client.get("/api/events/by_day?since=not-a-date")
    assert resp.status_code == 400
    assert "since" in resp.get_json()["error"]


def test_events_by_day_since_greater_than_until_returns_400(client):
    resp = client.get(
        "/api/events/by_day?since=2026-06-22T00:00:00Z&until=2026-06-20T00:00:00Z"
    )
    assert resp.status_code == 400


def test_events_by_day_blank_q_returns_400(client):
    resp = client.get("/api/events/by_day?q=%20%20%20")
    assert resp.status_code == 400


# ---- GET /api/events/by_hour_of_day ----


def test_events_by_hour_of_day_empty_store_returns_empty(client):
    resp = client.get("/api/events/by_hour_of_day")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body == {"total": 0, "distinct_hours": 0, "by_hour": []}


def test_events_by_hour_of_day_groups_by_utc_hour(client):
    _seed_event_at("login", "2026-06-20T10:00:00+00:00")
    _seed_event_at("login", "2026-06-20T10:59:59+00:00")
    _seed_event_at("login", "2026-06-21T10:30:00+00:00")
    _seed_event_at("login", "2026-06-20T23:00:00+00:00")
    resp = client.get("/api/events/by_hour_of_day")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 4
    assert body["distinct_hours"] == 2
    assert body["by_hour"] == [
        {"hour": "10", "count": 3},
        {"hour": "23", "count": 1},
    ]


def test_events_by_hour_of_day_sorted_lex_ascending_with_zero_padding(client):
    # 2 桁ゼロ詰め (`"01"`〜`"23"`) で lex 順 = 時間順になることを確認
    _seed_event_at("a", "2026-06-22T23:00:00+00:00")
    _seed_event_at("a", "2026-06-22T01:00:00+00:00")
    _seed_event_at("a", "2026-06-22T09:00:00+00:00")
    _seed_event_at("a", "2026-06-22T15:00:00+00:00")
    resp = client.get("/api/events/by_hour_of_day")
    hours = [row["hour"] for row in resp.get_json()["by_hour"]]
    assert hours == ["01", "09", "15", "23"]


def test_events_by_hour_of_day_filters_by_event_name(client):
    _seed_event_at("login", "2026-06-20T10:00:00+00:00")
    _seed_event_at("logout", "2026-06-20T10:00:00+00:00")
    _seed_event_at("login", "2026-06-20T11:00:00+00:00")
    resp = client.get("/api/events/by_hour_of_day?event_name=login")
    body = resp.get_json()
    assert body["total"] == 2
    assert body["by_hour"] == [
        {"hour": "10", "count": 1},
        {"hour": "11", "count": 1},
    ]


def test_events_by_hour_of_day_filters_by_q_case_insensitive(client):
    _seed_event_at("UserSignup", "2026-06-20T10:00:00+00:00")
    _seed_event_at("user_login", "2026-06-20T10:30:00+00:00")
    _seed_event_at("page_view", "2026-06-20T11:00:00+00:00")
    resp = client.get("/api/events/by_hour_of_day?q=USER")
    body = resp.get_json()
    assert body["total"] == 2
    assert body["distinct_hours"] == 1
    assert body["by_hour"] == [{"hour": "10", "count": 2}]


def test_events_by_hour_of_day_filters_by_since_until(client):
    _seed_event_at("e", "2026-06-20T09:00:00+00:00")
    _seed_event_at("e", "2026-06-20T10:00:00+00:00")
    _seed_event_at("e", "2026-06-20T11:00:00+00:00")
    _seed_event_at("e", "2026-06-20T12:00:00+00:00")
    resp = client.get(
        "/api/events/by_hour_of_day?since=2026-06-20T10:00:00Z&until=2026-06-20T11:30:00Z"
    )
    body = resp.get_json()
    assert body["total"] == 2
    assert body["by_hour"] == [
        {"hour": "10", "count": 1},
        {"hour": "11", "count": 1},
    ]


def test_events_by_hour_of_day_converts_non_utc_timestamps_to_utc(client):
    # JST 2026-06-21 08:00 → UTC 2026-06-20 23:00 (hour=23)
    _seed_event_at("evt", "2026-06-21T08:00:00+09:00")
    # JST 2026-06-21 09:00 → UTC 2026-06-21 00:00 (hour=00)
    _seed_event_at("evt", "2026-06-21T09:00:00+09:00")
    # JST 2026-06-21 10:00 → UTC 2026-06-21 01:00 (hour=01)
    _seed_event_at("evt", "2026-06-21T10:00:00+09:00")
    resp = client.get("/api/events/by_hour_of_day")
    body = resp.get_json()
    assert body["distinct_hours"] == 3
    hours = {row["hour"]: row["count"] for row in body["by_hour"]}
    assert hours == {"23": 1, "00": 1, "01": 1}


def test_events_by_hour_of_day_ignores_broken_timestamps(client):
    _seed_event_at("good", "2026-06-20T10:00:00+00:00")
    _seed_event_at("bad_iso", "not-a-timestamp")
    events_store.append({"event_name": "missing_ts", "properties": {}})
    resp = client.get("/api/events/by_hour_of_day")
    body = resp.get_json()
    assert body["total"] == 1
    assert body["by_hour"] == [{"hour": "10", "count": 1}]


def test_events_by_hour_of_day_invalid_since_returns_400(client):
    resp = client.get("/api/events/by_hour_of_day?since=not-a-date")
    assert resp.status_code == 400
    assert "since" in resp.get_json()["error"]


def test_events_by_hour_of_day_since_greater_than_until_returns_400(client):
    resp = client.get(
        "/api/events/by_hour_of_day?since=2026-06-22T00:00:00Z&until=2026-06-20T00:00:00Z"
    )
    assert resp.status_code == 400


def test_events_by_hour_of_day_blank_q_returns_400(client):
    resp = client.get("/api/events/by_hour_of_day?q=%20%20%20")
    assert resp.status_code == 400


# ---- GET /api/events/by_day_of_week ----


def test_events_by_day_of_week_empty_store_returns_empty(client):
    resp = client.get("/api/events/by_day_of_week")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body == {"total": 0, "distinct_days_of_week": 0, "by_day_of_week": []}


def test_events_by_day_of_week_groups_by_iso_weekday(client):
    # 2026-06-22 月曜 (1), 2026-06-23 火曜 (2), 2026-06-28 日曜 (7)
    _seed_event_at("login", "2026-06-22T10:00:00+00:00")
    _seed_event_at("login", "2026-06-22T22:00:00+00:00")
    _seed_event_at("login", "2026-06-23T08:00:00+00:00")
    _seed_event_at("login", "2026-06-28T15:00:00+00:00")
    resp = client.get("/api/events/by_day_of_week")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 4
    assert body["distinct_days_of_week"] == 3
    assert body["by_day_of_week"] == [
        {"day_of_week": "1", "count": 2},
        {"day_of_week": "2", "count": 1},
        {"day_of_week": "7", "count": 1},
    ]


def test_events_by_day_of_week_sorted_lex_ascending(client):
    # "1"〜"7" の単一桁文字列は lex 順 = 曜日順
    _seed_event_at("a", "2026-06-28T10:00:00+00:00")  # 日 (7)
    _seed_event_at("a", "2026-06-22T10:00:00+00:00")  # 月 (1)
    _seed_event_at("a", "2026-06-25T10:00:00+00:00")  # 木 (4)
    _seed_event_at("a", "2026-06-23T10:00:00+00:00")  # 火 (2)
    resp = client.get("/api/events/by_day_of_week")
    days = [row["day_of_week"] for row in resp.get_json()["by_day_of_week"]]
    assert days == ["1", "2", "4", "7"]


def test_events_by_day_of_week_sunday_maps_to_seven(client):
    # 2026-06-28 は日曜 → ISO 8601 で 7
    _seed_event_at("evt", "2026-06-28T12:00:00+00:00")
    resp = client.get("/api/events/by_day_of_week")
    body = resp.get_json()
    assert body["by_day_of_week"] == [{"day_of_week": "7", "count": 1}]


def test_events_by_day_of_week_filters_by_event_name(client):
    _seed_event_at("login", "2026-06-22T10:00:00+00:00")
    _seed_event_at("logout", "2026-06-22T10:00:00+00:00")
    _seed_event_at("login", "2026-06-23T11:00:00+00:00")
    resp = client.get("/api/events/by_day_of_week?event_name=login")
    body = resp.get_json()
    assert body["total"] == 2
    assert body["by_day_of_week"] == [
        {"day_of_week": "1", "count": 1},
        {"day_of_week": "2", "count": 1},
    ]


def test_events_by_day_of_week_filters_by_q_case_insensitive(client):
    _seed_event_at("UserSignup", "2026-06-22T10:00:00+00:00")
    _seed_event_at("user_login", "2026-06-22T10:30:00+00:00")
    _seed_event_at("page_view", "2026-06-23T11:00:00+00:00")
    resp = client.get("/api/events/by_day_of_week?q=USER")
    body = resp.get_json()
    assert body["total"] == 2
    assert body["distinct_days_of_week"] == 1
    assert body["by_day_of_week"] == [{"day_of_week": "1", "count": 2}]


def test_events_by_day_of_week_filters_by_since_until(client):
    # 4 つの曜日に分散
    _seed_event_at("e", "2026-06-22T09:00:00+00:00")  # 月 (1)
    _seed_event_at("e", "2026-06-23T10:00:00+00:00")  # 火 (2)
    _seed_event_at("e", "2026-06-24T11:00:00+00:00")  # 水 (3)
    _seed_event_at("e", "2026-06-25T12:00:00+00:00")  # 木 (4)
    resp = client.get(
        "/api/events/by_day_of_week?since=2026-06-23T00:00:00Z&until=2026-06-24T23:59:59Z"
    )
    body = resp.get_json()
    assert body["total"] == 2
    assert body["by_day_of_week"] == [
        {"day_of_week": "2", "count": 1},
        {"day_of_week": "3", "count": 1},
    ]


def test_events_by_day_of_week_converts_non_utc_timestamps_to_utc(client):
    # 2026-06-29 月曜 02:00 JST → UTC 2026-06-28 17:00 (日曜=7)
    _seed_event_at("evt", "2026-06-29T02:00:00+09:00")
    # 2026-06-23 火曜 02:00 UTC → 火曜 (2)
    _seed_event_at("evt", "2026-06-23T02:00:00+00:00")
    resp = client.get("/api/events/by_day_of_week")
    body = resp.get_json()
    counts = {row["day_of_week"]: row["count"] for row in body["by_day_of_week"]}
    assert counts == {"7": 1, "2": 1}


def test_events_by_day_of_week_ignores_broken_timestamps(client):
    _seed_event_at("good", "2026-06-22T10:00:00+00:00")
    _seed_event_at("bad_iso", "not-a-timestamp")
    events_store.append({"event_name": "missing_ts", "properties": {}})
    resp = client.get("/api/events/by_day_of_week")
    body = resp.get_json()
    assert body["total"] == 1
    assert body["by_day_of_week"] == [{"day_of_week": "1", "count": 1}]


def test_events_by_day_of_week_invalid_since_returns_400(client):
    resp = client.get("/api/events/by_day_of_week?since=not-a-date")
    assert resp.status_code == 400
    assert "since" in resp.get_json()["error"]


def test_events_by_day_of_week_since_greater_than_until_returns_400(client):
    resp = client.get(
        "/api/events/by_day_of_week?since=2026-06-22T00:00:00Z&until=2026-06-20T00:00:00Z"
    )
    assert resp.status_code == 400


def test_events_by_day_of_week_blank_q_returns_400(client):
    resp = client.get("/api/events/by_day_of_week?q=%20%20%20")
    assert resp.status_code == 400
