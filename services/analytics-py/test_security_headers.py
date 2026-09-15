import pytest
from app import app, events_store


@pytest.fixture
def client():
    """`test_app.py` / `test_middleware.py` と独立した fixture。

    セキュリティレスポンスヘッダの回帰検証のみを扱うため、モジュール分離して
    ハンドラ側のグローバル state (`events_store`) をクリーンな状態から開始する。
    """
    app.config["TESTING"] = True
    events_store.clear()
    with app.test_client() as c:
        yield c


def _assert_security_headers(resp):
    """`X-Content-Type-Options` / `X-Frame-Options` / `Referrer-Policy` の
    3 ヘッダが `_security_headers` ミドルウェア規約通りに付与されていることを検証する。

    `usermgmt-ts` の `security_headers.test.ts` と同じ値を期待し、
    3 サービスの回帰検証で挙動を揃える。
    """
    assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    assert resp.headers.get("X-Frame-Options") == "DENY"
    assert resp.headers.get("Referrer-Policy") == "no-referrer"


def test_security_headers_on_health_2xx(client):
    """`/health` (200) の成功応答にセキュリティヘッダが付与されること。"""
    resp = client.get("/health")
    assert resp.status_code == 200
    _assert_security_headers(resp)


def test_security_headers_on_404(client):
    """未定義パス (404) でも `after_request` は走り、ヘッダが付与されること。

    Flask の `after_request` は 4xx / 5xx を含む HTTPException 経路でも実行される。
    エラー応答経路にもセキュリティヘッダが漏れなく付与されることを担保する。
    """
    resp = client.get("/__no_such_route__")
    assert resp.status_code == 404
    _assert_security_headers(resp)


def test_security_headers_on_validation_error_400(client):
    """バリデーションエラー (400) の応答にもセキュリティヘッダが付与されること。

    ハンドラ内で `return jsonify({"error": ...}), 400` のように早期リターンした
    経路でも `after_request` が走ることを、`event_name` を欠いた POST で確認する。
    """
    resp = client.post("/api/events", json={})
    assert resp.status_code == 400
    _assert_security_headers(resp)


def test_security_headers_on_created_201(client):
    """イベント作成 (201) の応答にもセキュリティヘッダが付与されること。

    通常経路 (成功系のハンドラ) でヘッダが上書き・削除されていないことを確認する。
    """
    resp = client.post("/api/events", json={"event_name": "signup"})
    assert resp.status_code == 201
    _assert_security_headers(resp)


def test_security_headers_on_payload_too_large_413(client):
    """ペイロード超過 (413) の応答にもセキュリティヘッダが付与されること。

    `MAX_PAYLOAD_SIZE` (既定 1MiB) 超えの early-return 経路でも `after_request`
    が走ることを、実データを 2MiB 以上送って確認する。
    """
    big_payload = b'{"event_name":"x","padding":"' + (b"a" * (2 * 1024 * 1024)) + b'"}'
    resp = client.post(
        "/api/events",
        data=big_payload,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 413
    _assert_security_headers(resp)
