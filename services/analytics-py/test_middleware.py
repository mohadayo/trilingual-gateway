import pytest
from app import app, events_store


@pytest.fixture
def client():
    """`test_app.py` と独立した fixture。

    既存 `test_app.py` 内に同名 `client` fixture が定義されているため、
    モジュール分離して middleware の挙動だけを検証する。
    """
    app.config["TESTING"] = True
    events_store.clear()
    with app.test_client() as c:
        yield c


def test_access_log_middleware_attaches_response_time_header_on_2xx(client):
    """成功レスポンスに `X-Response-Time-Ms` ヘッダが付与されること。"""
    resp = client.get("/health")
    assert resp.status_code == 200
    header = resp.headers.get("X-Response-Time-Ms")
    assert header is not None, "middleware should attach X-Response-Time-Ms"
    assert float(header) >= 0.0


def test_access_log_middleware_runs_on_404(client):
    """未定義パス (404) でも middleware は走り、ヘッダが付与されること。

    Flask の `after_request` は 404 等の HTTPException 経路でも実行される。
    エラー応答にも応答時間ヘッダが付くことを担保する。
    """
    resp = client.get("/__no_such_route__")
    assert resp.status_code == 404
    assert "X-Response-Time-Ms" in resp.headers
    assert float(resp.headers["X-Response-Time-Ms"]) >= 0.0
