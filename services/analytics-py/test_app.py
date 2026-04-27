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
    assert data["count"] == 3


def test_list_events_filtered(client):
    client.post("/api/events", json={"event_name": "filter_test"})
    resp = client.get("/api/events?event_name=filter_test")
    assert resp.status_code == 200
    data = resp.get_json()
    assert all(e["event_name"] == "filter_test" for e in data["events"])


def test_delete_events_success(client):
    client.post("/api/events", json={"event_name": "to_delete"})
    client.post("/api/events", json={"event_name": "to_delete"})
    client.post("/api/events", json={"event_name": "keep"})

    resp = client.delete("/api/events?event_name=to_delete")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["deleted_count"] == 2

    list_resp = client.get("/api/events")
    assert list_resp.get_json()["count"] == 1


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
