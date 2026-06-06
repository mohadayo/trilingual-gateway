import pytest
from app import app, events_store


@pytest.fixture(autouse=True)
def clear_store():
    events_store.clear()
    yield
    events_store.clear()


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["service"] == "analytics-py"
    assert "timestamp" in data


def test_track_event(client):
    resp = client.post("/api/events", json={"event_name": "page_view", "properties": {"page": "/home"}})
    assert resp.status_code == 201
    data = resp.get_json()
    assert data["event"]["event_name"] == "page_view"


def test_track_event_missing_name(client):
    resp = client.post("/api/events", json={"properties": {"page": "/home"}})
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_track_event_empty_body(client):
    resp = client.post("/api/events", content_type="application/json")
    assert resp.status_code == 400


def test_list_events(client):
    client.post("/api/events", json={"event_name": "click"})
    client.post("/api/events", json={"event_name": "click"})
    client.post("/api/events", json={"event_name": "scroll"})
    resp = client.get("/api/events")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 3
    assert data["count"] == 3


def test_list_events_filtered(client):
    client.post("/api/events", json={"event_name": "filter_test"})
    resp = client.get("/api/events?event_name=filter_test")
    assert resp.status_code == 200
    data = resp.get_json()
    assert all(e["event_name"] == "filter_test" for e in data["events"])


def test_list_events_pagination_limit(client):
    for i in range(5):
        client.post("/api/events", json={"event_name": f"ev_{i}"})
    resp = client.get("/api/events?limit=2")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 5
    assert data["count"] == 2
    assert data["limit"] == 2
    assert data["offset"] == 0


def test_list_events_pagination_offset(client):
    for i in range(5):
        client.post("/api/events", json={"event_name": f"ev_{i}"})
    resp = client.get("/api/events?limit=2&offset=3")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 5
    assert data["count"] == 2
    assert data["offset"] == 3
    assert data["events"][0]["event_name"] == "ev_3"


def test_list_events_pagination_offset_beyond(client):
    for i in range(3):
        client.post("/api/events", json={"event_name": f"ev_{i}"})
    resp = client.get("/api/events?limit=10&offset=10")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 3
    assert data["count"] == 0


def test_list_events_pagination_with_filter(client):
    for i in range(4):
        client.post("/api/events", json={"event_name": "target"})
    client.post("/api/events", json={"event_name": "other"})
    resp = client.get("/api/events?event_name=target&limit=2&offset=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 4
    assert data["count"] == 2


def test_list_events_negative_limit(client):
    client.post("/api/events", json={"event_name": "neg"})
    resp = client.get("/api/events?limit=-1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 1
    assert data["count"] == 1


def test_list_events_negative_offset(client):
    client.post("/api/events", json={"event_name": "neg"})
    resp = client.get("/api/events?offset=-5")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["offset"] == 0


def test_delete_events_success(client):
    client.post("/api/events", json={"event_name": "to_delete"})
    client.post("/api/events", json={"event_name": "to_delete"})
    client.post("/api/events", json={"event_name": "keep"})

    resp = client.delete("/api/events?event_name=to_delete")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_count"] == 2

    list_resp = client.get("/api/events")
    assert list_resp.get_json()["total"] == 1


def test_delete_events_not_found(client):
    # processor-go の DELETE /api/messages と同じく、フィルタ未マッチは 200 + deleted_count=0
    resp = client.delete("/api/events?event_name=nonexistent")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_count"] == 0
    assert data["event_name"] == "nonexistent"


def test_delete_events_missing_param(client):
    resp = client.delete("/api/events")
    assert resp.status_code == 400
    assert "at least one of" in resp.get_json()["error"]


def test_delete_events_since(client):
    events_store.append({"event_name": "old", "properties": {}, "timestamp": "2020-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "mid", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "new", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    resp = client.delete("/api/events?since=2024-01-01T00:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_count"] == 2
    remaining = [e["event_name"] for e in events_store]
    assert remaining == ["old"]


def test_delete_events_until(client):
    events_store.append({"event_name": "old", "properties": {}, "timestamp": "2020-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "mid", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "new", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    resp = client.delete("/api/events?until=2024-06-01T00:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_count"] == 2
    remaining = [e["event_name"] for e in events_store]
    assert remaining == ["new"]


def test_delete_events_since_and_until_and_event_name(client):
    events_store.append({"event_name": "click", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "click", "properties": {}, "timestamp": "2025-06-01T00:00:00+00:00"})
    events_store.append({"event_name": "click", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "scroll", "properties": {}, "timestamp": "2025-06-01T00:00:00+00:00"})
    resp = client.delete(
        "/api/events?event_name=click&since=2025-01-01T00:00:00Z&until=2025-12-31T23:59:59Z"
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_count"] == 1
    remaining = sorted(e["event_name"] for e in events_store)
    assert remaining == ["click", "click", "scroll"]


def test_delete_events_invalid_since(client):
    resp = client.delete("/api/events?since=not-a-date")
    assert resp.status_code == 400
    assert "since" in resp.get_json()["error"]


def test_delete_events_since_greater_than_until(client):
    resp = client.delete(
        "/api/events?since=2026-01-01T00:00:00Z&until=2024-01-01T00:00:00Z"
    )
    assert resp.status_code == 400
    assert "since" in resp.get_json()["error"]


def test_events_summary(client):
    client.post("/api/events", json={"event_name": "summary_a"})
    client.post("/api/events", json={"event_name": "summary_a"})
    client.post("/api/events", json={"event_name": "summary_b"})
    resp = client.get("/api/events/summary")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "summary" in data
    assert data["total_events"] == 3
    assert data["summary"]["summary_a"] == 2
    assert data["summary"]["summary_b"] == 1


def test_events_store_max_capacity(client, monkeypatch):
    monkeypatch.setattr("app.MAX_EVENTS", 3)
    for i in range(5):
        client.post("/api/events", json={"event_name": f"cap_{i}"})
    resp = client.get("/api/events")
    data = resp.get_json()
    assert data["total"] == 3
    names = [e["event_name"] for e in data["events"]]
    assert "cap_0" not in names
    assert "cap_1" not in names
    assert "cap_4" in names


def test_track_event_blank_name(client):
    resp = client.post("/api/events", json={"event_name": "   "})
    assert resp.status_code == 400
    assert "blank" in resp.get_json()["error"].lower()


def test_track_event_non_string_name(client):
    resp = client.post("/api/events", json={"event_name": 123})
    assert resp.status_code == 400
    assert "string" in resp.get_json()["error"].lower()


def test_track_event_long_name(client, monkeypatch):
    monkeypatch.setattr("app.MAX_EVENT_NAME_LENGTH", 10)
    resp = client.post("/api/events", json={"event_name": "x" * 100})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "too long" in data["error"].lower()
    assert data["max_length"] == 10


def test_track_event_strips_whitespace(client):
    resp = client.post("/api/events", json={"event_name": "  page_view  "})
    assert resp.status_code == 201
    assert resp.get_json()["event"]["event_name"] == "page_view"


def test_track_event_invalid_properties(client):
    resp = client.post("/api/events", json={"event_name": "ev", "properties": ["not", "a", "dict"]})
    assert resp.status_code == 400
    assert "object" in resp.get_json()["error"].lower()


def test_track_event_null_properties_accepted(client):
    resp = client.post("/api/events", json={"event_name": "ev", "properties": None})
    assert resp.status_code == 201
    assert resp.get_json()["event"]["properties"] == {}


def test_track_event_payload_too_large(client, monkeypatch):
    monkeypatch.setattr("app.MAX_PAYLOAD_SIZE", 50)
    big_payload = {"event_name": "ev", "properties": {"data": "x" * 100}}
    resp = client.post("/api/events", json=big_payload)
    assert resp.status_code == 413
    assert "too large" in resp.get_json()["error"].lower()


def test_list_events_limit_clamped_to_max(client, monkeypatch):
    monkeypatch.setattr("app.MAX_PAGE_LIMIT", 3)
    for i in range(10):
        client.post("/api/events", json={"event_name": f"ev_{i}"})
    resp = client.get("/api/events?limit=100")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["limit"] == 3
    assert data["count"] == 3
    assert data["total"] == 10


def test_events_store_within_capacity(client, monkeypatch):
    monkeypatch.setattr("app.MAX_EVENTS", 10)
    for i in range(3):
        client.post("/api/events", json={"event_name": f"ok_{i}"})
    resp = client.get("/api/events")
    data = resp.get_json()
    assert data["total"] == 3


def test_list_events_filter_since(client):
    # Inject events with controlled timestamps
    events_store.append({"event_name": "old", "properties": {}, "timestamp": "2020-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "mid", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "new", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    resp = client.get("/api/events?since=2024-01-01T00:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 2
    names = sorted(e["event_name"] for e in data["events"])
    assert names == ["mid", "new"]


def test_list_events_filter_until(client):
    events_store.append({"event_name": "old", "properties": {}, "timestamp": "2020-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "mid", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "new", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    resp = client.get("/api/events?until=2024-06-01T00:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 2
    names = sorted(e["event_name"] for e in data["events"])
    assert names == ["mid", "old"]


def test_list_events_filter_since_and_until(client):
    events_store.append({"event_name": "a", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "b", "properties": {}, "timestamp": "2025-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "c", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    resp = client.get("/api/events?since=2024-06-01T00:00:00Z&until=2025-06-01T00:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 1
    assert data["events"][0]["event_name"] == "b"


def test_list_events_rejects_invalid_since(client):
    resp = client.get("/api/events?since=not-a-date")
    assert resp.status_code == 400
    assert "since" in resp.get_json()["error"]


def test_list_events_rejects_invalid_until(client):
    resp = client.get("/api/events?until=foo")
    assert resp.status_code == 400
    assert "until" in resp.get_json()["error"]


def test_list_events_rejects_since_greater_than_until(client):
    resp = client.get("/api/events?since=2026-01-01T00:00:00Z&until=2024-01-01T00:00:00Z")
    assert resp.status_code == 400
    assert "since" in resp.get_json()["error"]


def test_list_events_combines_event_name_and_time_range(client):
    events_store.append({"event_name": "click", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "click", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "scroll", "properties": {}, "timestamp": "2026-01-01T00:00:00+00:00"})
    resp = client.get("/api/events?event_name=click&since=2025-01-01T00:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 1
    assert data["events"][0]["event_name"] == "click"


def test_list_events_sort_default(client):
    events_store.append({"event_name": "b", "properties": {}, "timestamp": "2024-02-01T00:00:00+00:00"})
    events_store.append({"event_name": "a", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "c", "properties": {}, "timestamp": "2024-03-01T00:00:00+00:00"})
    resp = client.get("/api/events")
    assert resp.status_code == 200
    data = resp.get_json()
    assert [e["event_name"] for e in data["events"]] == ["a", "b", "c"]
    assert data["sort"] == "timestamp"
    assert data["order"] == "asc"


def test_list_events_sort_desc(client):
    events_store.append({"event_name": "a", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "b", "properties": {}, "timestamp": "2024-02-01T00:00:00+00:00"})
    events_store.append({"event_name": "c", "properties": {}, "timestamp": "2024-03-01T00:00:00+00:00"})
    resp = client.get("/api/events?order=desc")
    data = resp.get_json()
    assert [e["event_name"] for e in data["events"]] == ["c", "b", "a"]


def test_list_events_sort_by_event_name(client):
    events_store.append({"event_name": "zeta", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "alpha", "properties": {}, "timestamp": "2024-02-01T00:00:00+00:00"})
    resp = client.get("/api/events?sort=event_name")
    names = [e["event_name"] for e in resp.get_json()["events"]]
    assert names == ["alpha", "zeta"]


def test_list_events_invalid_sort_field(client):
    resp = client.get("/api/events?sort=bogus")
    assert resp.status_code == 400
    assert "allowed" in resp.get_json()


def test_list_events_invalid_sort_order(client):
    resp = client.get("/api/events?order=sideways")
    assert resp.status_code == 400
    assert "allowed" in resp.get_json()


def test_summary_filter_by_event_name(client):
    events_store.append({"event_name": "click", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "click", "properties": {}, "timestamp": "2024-01-02T00:00:00+00:00"})
    events_store.append({"event_name": "scroll", "properties": {}, "timestamp": "2024-01-03T00:00:00+00:00"})
    resp = client.get("/api/events/summary?event_name=click")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["summary"] == {"click": 2}
    assert data["total_events"] == 2


def test_summary_filter_by_time_range(client):
    events_store.append({"event_name": "x", "properties": {}, "timestamp": "2024-01-01T00:00:00+00:00"})
    events_store.append({"event_name": "x", "properties": {}, "timestamp": "2024-06-01T00:00:00+00:00"})
    events_store.append({"event_name": "x", "properties": {}, "timestamp": "2024-12-01T00:00:00+00:00"})
    resp = client.get("/api/events/summary?since=2024-04-01T00:00:00Z&until=2024-09-01T00:00:00Z")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_events"] == 1


def test_summary_invalid_since(client):
    resp = client.get("/api/events/summary?since=notanumber")
    assert resp.status_code == 400


def test_summary_until_before_since(client):
    resp = client.get("/api/events/summary?since=2024-06-01T00:00:00Z&until=2024-01-01T00:00:00Z")
    assert resp.status_code == 400


def test_list_events_q_substring_case_insensitive(client):
    client.post("/api/events", json={"event_name": "PageView"})
    client.post("/api/events", json={"event_name": "page_click"})
    client.post("/api/events", json={"event_name": "scroll"})
    resp = client.get("/api/events?q=page")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 2
    assert {e["event_name"] for e in data["events"]} == {"PageView", "page_click"}


def test_list_events_q_no_match(client):
    client.post("/api/events", json={"event_name": "page_view"})
    resp = client.get("/api/events?q=nonexistent")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total"] == 0
    assert data["count"] == 0


def test_list_events_q_with_event_name_filter_is_and(client):
    client.post("/api/events", json={"event_name": "page_view"})
    client.post("/api/events", json={"event_name": "page_click"})
    # event_name=page_view (完全一致) かつ q=click（部分一致） → 0 件
    resp = client.get("/api/events?event_name=page_view&q=click")
    assert resp.status_code == 200
    assert resp.get_json()["total"] == 0
    # event_name=page_view かつ q=page → 1 件（page_view のみ）
    resp = client.get("/api/events?event_name=page_view&q=page")
    assert resp.status_code == 200
    assert resp.get_json()["total"] == 1


def test_list_events_q_blank_returns_400(client):
    resp = client.get("/api/events?q=%20%20")  # スペースのみ
    assert resp.status_code == 400
    assert "blank" in resp.get_json()["error"]


def test_list_events_q_too_long_returns_400(client):
    long_q = "x" * 201  # MAX_EVENT_NAME_LENGTH=200
    resp = client.get(f"/api/events?q={long_q}")
    assert resp.status_code == 400
    assert "too long" in resp.get_json()["error"]


def test_summary_q_substring_case_insensitive(client):
    client.post("/api/events", json={"event_name": "PageView"})
    client.post("/api/events", json={"event_name": "PageClick"})
    client.post("/api/events", json={"event_name": "scroll"})
    resp = client.get("/api/events/summary?q=page")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_events"] == 2
    assert data["summary"]["PageView"] == 1
    assert data["summary"]["PageClick"] == 1


def test_summary_q_blank_returns_400(client):
    resp = client.get("/api/events/summary?q=")
    # 空文字は Flask が None として扱う可能性があるため、ここでは strip 後空 (%20) でテスト
    # ただし "" は raw=="" なので _normalize_q では blank として 400
    assert resp.status_code == 400


def test_events_lock_concurrent_writes():
    from app import events_store as store, events_lock
    import threading as _threading

    store.clear()

    def writer(tag):
        for i in range(40):
            with events_lock:
                store.append({
                    "event_name": f"{tag}-{i}",
                    "properties": {},
                    "timestamp": "2024-01-01T00:00:00+00:00",
                })

    threads = [_threading.Thread(target=writer, args=(f"t{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(store) == 4 * 40


# ---------------------------------------------------------------------------
# GET /api/events/names — distinct event_name 一覧のみを返す軽量エンドポイント
# ---------------------------------------------------------------------------


def test_event_names_empty_store(client):
    resp = client.get("/api/events/names")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["names"] == []
    assert data["total"] == 0
    assert data["count"] == 0
    assert data["order"] == "asc"


def test_event_names_distinct_and_sorted_asc(client):
    # 同じ event_name を複数回投入しても 1 件にまとめられる、かつ昇順で返る
    for name in ("zeta", "alpha", "beta", "alpha", "beta"):
        client.post("/api/events", json={"event_name": name})
    resp = client.get("/api/events/names")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["names"] == ["alpha", "beta", "zeta"]
    assert data["total"] == 3
    assert data["count"] == 3


def test_event_names_order_desc(client):
    for name in ("alpha", "beta", "zeta"):
        client.post("/api/events", json={"event_name": name})
    resp = client.get("/api/events/names?order=desc")
    assert resp.status_code == 200
    assert resp.get_json()["names"] == ["zeta", "beta", "alpha"]


def test_event_names_invalid_order_returns_400(client):
    resp = client.get("/api/events/names?order=upside_down")
    assert resp.status_code == 400
    assert "allowed" in resp.get_json()


def test_event_names_pagination(client):
    for name in ("a", "b", "c", "d", "e"):
        client.post("/api/events", json={"event_name": name})
    resp = client.get("/api/events/names?limit=2&offset=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["names"] == ["b", "c"]
    assert data["count"] == 2
    assert data["total"] == 5
    assert data["limit"] == 2
    assert data["offset"] == 1


def test_event_names_limit_clamped_to_max(client, monkeypatch):
    # MAX_PAGE_LIMIT を 3 に下げて、limit=999 が 3 にクランプされることを回帰する
    monkeypatch.setattr("app.MAX_PAGE_LIMIT", 3)
    for name in ("a", "b", "c", "d", "e"):
        client.post("/api/events", json={"event_name": name})
    resp = client.get("/api/events/names?limit=999")
    assert resp.status_code == 200
    assert resp.get_json()["limit"] == 3


def test_event_names_negative_limit_falls_back_to_default(client):
    client.post("/api/events", json={"event_name": "x"})
    resp = client.get("/api/events/names?limit=-5")
    assert resp.status_code == 200
    # 既定 limit にフォールバックする（既存 list_events と挙動を揃える）
    assert resp.get_json()["limit"] > 0


def test_event_names_negative_offset_clamps_to_zero(client):
    client.post("/api/events", json={"event_name": "x"})
    resp = client.get("/api/events/names?offset=-1")
    assert resp.status_code == 200
    assert resp.get_json()["offset"] == 0


def test_event_names_q_filter_case_insensitive(client):
    for name in ("page_view", "Page_Click", "signup", "API_Call"):
        client.post("/api/events", json={"event_name": name})
    resp = client.get("/api/events/names?q=page")
    assert resp.status_code == 200
    data = resp.get_json()
    # "page" を大文字小文字無視で含む 2 件
    assert set(data["names"]) == {"page_view", "Page_Click"}
    assert data["total"] == 2


def test_event_names_q_blank_returns_400(client):
    resp = client.get("/api/events/names?q=%20%20%20")
    assert resp.status_code == 400


def test_event_names_since_until_filter(client):
    # 直近のイベントだけが残るように、過去にあった event を直接 events_store に注入する
    from app import events_store, events_lock
    with events_lock:
        events_store.clear()
        events_store.append({
            "event_name": "old",
            "properties": {},
            "timestamp": "2020-01-01T00:00:00+00:00",
        })
        events_store.append({
            "event_name": "new",
            "properties": {},
            "timestamp": "2030-01-01T00:00:00+00:00",
        })
    resp = client.get("/api/events/names?since=2025-01-01T00:00:00Z")
    assert resp.status_code == 200
    assert resp.get_json()["names"] == ["new"]


def test_event_names_invalid_since_returns_400(client):
    resp = client.get("/api/events/names?since=not-a-date")
    assert resp.status_code == 400


def test_event_names_since_greater_than_until_returns_400(client):
    resp = client.get(
        "/api/events/names?since=2030-01-01T00:00:00Z&until=2020-01-01T00:00:00Z"
    )
    assert resp.status_code == 400


def test_event_names_does_not_collide_with_summary(client):
    # `/api/events/names` は names 配列を返し、`/api/events/summary` は集計を返す
    client.post("/api/events", json={"event_name": "click"})
    client.post("/api/events", json={"event_name": "click"})
    client.post("/api/events", json={"event_name": "view"})

    names_resp = client.get("/api/events/names")
    assert names_resp.status_code == 200
    assert "names" in names_resp.get_json()
    assert "summary" not in names_resp.get_json()

    summary_resp = client.get("/api/events/summary")
    assert summary_resp.status_code == 200
    assert "summary" in summary_resp.get_json()
    assert summary_resp.get_json()["summary"] == {"click": 2, "view": 1}
