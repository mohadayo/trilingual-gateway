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
    resp = client.delete("/api/events?event_name=nonexistent")
    assert resp.status_code == 404


def test_delete_events_missing_param(client):
    resp = client.delete("/api/events")
    assert resp.status_code == 400


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


def test_events_store_within_capacity(client, monkeypatch):
    monkeypatch.setattr("app.MAX_EVENTS", 10)
    for i in range(3):
        client.post("/api/events", json={"event_name": f"ok_{i}"})
    resp = client.get("/api/events")
    data = resp.get_json()
    assert data["total"] == 3
