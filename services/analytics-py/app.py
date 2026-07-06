import os
import logging
import threading
import time
from datetime import datetime, timezone
from flask import Flask, jsonify, request, g

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("analytics")

app = Flask(__name__)


@app.before_request
def _access_log_start():
    """リクエスト着信時刻を Flask の `g` に積み、応答時に消費する。

    ``time.perf_counter_ns()`` を使い、ウォールクロック補正に左右されない
    単調増加な計測を行う（システムタイムジャンプで負値にならない）。
    """
    g._access_log_start_ns = time.perf_counter_ns()


@app.after_request
def _access_log_end(response):
    """応答ステータスと処理時間を 1 行の INFO ログに集約する。

    各ハンドラ内の ``logger.info(...)`` は機能単位の出来事を記録するもので、
    HTTP リクエスト横断で「method/path/status/duration」を一貫して観測する
    軸が無かった。本ミドルウェアでアクセスログを集約することで、ハンドラ側
    ログを追わなくとも 4xx 偏りや遅延傾向を構造化ログから即座に追える。

    レスポンスヘッダ ``X-Response-Time-Ms`` にも同値を返し、クライアント側
    (BFF / SPA) からも応答時間が読み取れるようにする。``before_request`` を
    経ない極端な経路（WSGI スタック側の異常等）は ``g._access_log_start_ns``
    が未設定なので duration を出さずに早期 return する。
    """
    start_ns = getattr(g, "_access_log_start_ns", None)
    if start_ns is None:
        return response
    duration_ms = round((time.perf_counter_ns() - start_ns) / 1_000_000.0, 3)
    logger.info(
        "%s %s -> %d (%.3fms)",
        request.method,
        request.path,
        response.status_code,
        duration_ms,
    )
    response.headers["X-Response-Time-Ms"] = f"{duration_ms}"
    return response


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


def _normalize_q(raw: str | None) -> tuple[str | None, str | None]:
    """`q` クエリパラメータを正規化する。

    戻り値は (正規化後の値, エラーメッセージ)。
    - None → (None, None) ：未指定（フィルタしない）
    - trim 後が空 → (None, "q must not be blank") ：400 を返す対象
    - 上限超過 → (None, ".. too long") ：400 を返す対象
    - 正常 → (trimmed, None)
    """
    if raw is None:
        return None, None
    stripped = raw.strip()
    if not stripped:
        return None, "'q' must not be blank"
    if len(stripped) > MAX_EVENT_NAME_LENGTH:
        return None, f"'q' is too long (max {MAX_EVENT_NAME_LENGTH})"
    return stripped, None


def _filter_events_by_q(events: list[dict], q: str | None) -> list[dict]:
    """`event_name` に対する大文字小文字無視の部分一致検索。"""
    if not q:
        return events
    needle = q.lower()
    return [e for e in events if needle in str(e.get("event_name", "")).lower()]


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
    q_raw = request.args.get("q")
    limit = request.args.get("limit", DEFAULT_PAGE_LIMIT, type=int)
    offset = request.args.get("offset", 0, type=int)
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")
    sort_field = request.args.get("sort", "timestamp")
    sort_order = request.args.get("order", "asc")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter: %s", q_err)
        return jsonify({"error": q_err}), 400

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
    filtered = _filter_events_by_q(filtered, q)
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
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    # 全件削除事故を防ぐため、最低 1 つのフィルタを必須にする。
    if not event_name and since_raw is None and until_raw is None:
        logger.warning("Delete request missing filter parameters")
        return jsonify({
            "error": "at least one of 'event_name', 'since', 'until' is required",
        }), 400

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on delete: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on delete: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    def _ts(event: dict) -> datetime | None:
        raw = event.get("timestamp")
        if not isinstance(raw, str):
            return None
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _matches(event: dict) -> bool:
        if event_name and event.get("event_name") != event_name:
            return False
        if since is not None or until is not None:
            ts = _ts(event)
            if ts is None:
                return False
            if since is not None and ts < since:
                return False
            if until is not None and ts > until:
                return False
        return True

    with events_lock:
        kept = [e for e in events_store if not _matches(e)]
        deleted_count = len(events_store) - len(kept)
        events_store[:] = kept

    logger.info(
        "Deleted %d events (event_name=%s since=%s until=%s)",
        deleted_count, event_name, since_raw, until_raw,
    )
    return jsonify({
        "message": "Events deleted",
        "deleted_count": deleted_count,
        "event_name": event_name,
        "since": since_raw,
        "until": until_raw,
    })


@app.route("/api/events/names", methods=["GET"])
def list_event_names():
    """フィルタ後のイベントから distinct な event_name 一覧のみを返す軽量エンドポイント。

    `/api/events/summary` は名前ごとの件数集計を含むため、UI のドロップダウン
    populate / オートコンプリート（「名前そのもののリストだけが欲しい」）用途には
    過剰になりがち。このエンドポイントは集計を行わず、重複排除した event_name 名のみを
    並べ替えてページングして返す。

    クエリ:
    - `q`: event_name 部分一致（大文字小文字無視。既存 list_events / summary と挙動を揃える）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ（既存と同じパース）
    - `order`: `asc` / `desc`（既定 `asc`、event_name 昇順）
    - `limit` / `offset`: `DEFAULT_PAGE_LIMIT` / `MAX_PAGE_LIMIT` を流用してページング
    """
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")
    sort_order = request.args.get("order", "asc")
    limit = request.args.get("limit", DEFAULT_PAGE_LIMIT, type=int)
    offset = request.args.get("offset", 0, type=int)

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on names: %s", q_err)
        return jsonify({"error": q_err}), 400

    if sort_order not in ALLOWED_SORT_ORDERS:
        logger.warning("Invalid sort order on names: %s", sort_order)
        return jsonify({
            "error": "Invalid sort order",
            "allowed": sorted(ALLOWED_SORT_ORDERS),
        }), 400

    # limit/offset を list_events と同じ規約で正規化する(負値→既定 / 上限クランプ)。
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
        logger.warning("Invalid timestamp filter on names: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on names: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    # ロック内ではスナップショットのみ取り、distinct / sort / filter は外で行う。
    with events_lock:
        filtered = list(events_store)
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    distinct = sorted({e["event_name"] for e in filtered}, reverse=(sort_order == "desc"))
    total = len(distinct)
    page = distinct[offset:offset + limit]
    logger.info(
        "Listed %d distinct event_name(s) (total=%d limit=%d offset=%d order=%s)",
        len(page), total, limit, offset, sort_order,
    )
    return jsonify({
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "order": sort_order,
        "names": page,
    })


@app.route("/api/events/names/<path:name>", methods=["GET"])
def event_name_detail(name: str):
    """単一の event_name に対する集約詳細を返す。

    `/api/events/names` は distinct 名一覧のみを返すのに対し、こちらは「名前 1 つ
    分の詳細」を返す。`/api/events/summary?event_name=...` でも件数は得られるが、
    `first_seen` / `last_seen` / `latest_properties` / `distinct_property_keys` は
    summary には含まれないため、イベント名のドリルダウン UI で複数リクエストに
    分解せず 1 リクエストで描画できるようにする。

    クエリ:
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ（既存と同じパース）

    戻り値:
    - `event_name`: 入力された name（フィルタ後 0 件でも 404 になる前にエコーは不要）
    - `count`: フィルタ後の件数
    - `first_seen` / `last_seen`: ISO8601 文字列。timestamp 昇順での最小/最大
    - `latest_properties`: `last_seen` の event の `properties`（無ければ `{}`）
    - `distinct_property_keys`: フィルタ後の全 event の properties 内に出現したキー（ソート済み）

    フィルタ後にレコードが 1 件も無ければ 404 を返す。
    """
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
        logger.warning("Invalid timestamp filter on name detail: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on name detail: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    # name は事故防止のため `strip()` のみで正規化（大小文字は区別する）。
    # 既存の event_name 完全一致 (`list_events?event_name=...`) と同じ挙動。
    normalized_name = name.strip()
    if not normalized_name:
        return jsonify({"error": "event_name must not be blank"}), 400
    if len(normalized_name) > MAX_EVENT_NAME_LENGTH:
        return jsonify({
            "error": "event_name is too long",
            "max_length": MAX_EVENT_NAME_LENGTH,
        }), 400

    with events_lock:
        snapshot = list(events_store)
    matched = [e for e in snapshot if e.get("event_name") == normalized_name]
    matched = _filter_events_by_time(matched, since, until)
    if not matched:
        logger.info("No events for name=%s (since=%s until=%s)", normalized_name, since_raw, until_raw)
        return jsonify({"error": f"No events found for '{normalized_name}'"}), 404

    # first_seen / last_seen は timestamp の昇順最小/最大で求める。
    # `_filter_events_by_time` が壊れた timestamp を弾いているので、ここでは
    # 文字列 ISO8601 ソートで安全に最小/最大を取れる（タイムゾーン揃いの保証は無いが、
    # POST 時に UTC 固定で書き込んでいるため実運用では問題ない）。
    timestamps = [e.get("timestamp", "") for e in matched if isinstance(e.get("timestamp"), str)]
    timestamps.sort()
    first_seen = timestamps[0] if timestamps else None
    last_seen = timestamps[-1] if timestamps else None

    # latest_properties は last_seen を持つレコードの properties(複数あれば最後に見たもの)。
    latest_properties: dict = {}
    if last_seen is not None:
        for e in matched:
            if e.get("timestamp") == last_seen:
                props = e.get("properties")
                if isinstance(props, dict):
                    latest_properties = props
                # 同一 timestamp の重複があれば後勝ち(FIFO 順で書き込まれているため、
                # ループの最後に見るのが「実時間で最後の観測」になる)

    # 全レコードの properties キーをユニオンしてソートして返す。
    keys: set[str] = set()
    for e in matched:
        props = e.get("properties")
        if isinstance(props, dict):
            keys.update(k for k in props.keys() if isinstance(k, str))

    logger.info(
        "Returned detail for event_name=%s (count=%d, distinct_keys=%d)",
        normalized_name, len(matched), len(keys),
    )
    return jsonify({
        "event_name": normalized_name,
        "count": len(matched),
        "first_seen": first_seen,
        "last_seen": last_seen,
        "latest_properties": latest_properties,
        "distinct_property_keys": sorted(keys),
    })


@app.route("/api/events/count", methods=["GET"])
def count_events():
    """保持中イベントの件数のみを返す軽量エンドポイント。

    `/api/events/summary` は per-name 集計込みの応答を返すが、UI 側で
    バッジ表示・ページャ初期化など「件数だけ知りたい」ケースには過剰。
    本エンドポイントはレコード本体を返さず、`total` / `distinct_names` /
    `by_name` の 3 つだけを返す。`by_name` は登場した event_name のみで、
    count 0 のキーは埋めない（軽量化）。

    クエリ:
    - `event_name`: 完全一致フィルタ（既存 `/summary` と同じ）
    - `q`: event_name の部分一致（大文字小文字無視、既存 `/summary` と同じ）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ
    """
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on count: %s", q_err)
        return jsonify({"error": q_err}), 400

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on count: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on count: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    by_name: dict[str, int] = {}
    for event in filtered:
        name = event["event_name"]
        by_name[name] = by_name.get(name, 0) + 1

    total = len(filtered)
    logger.info(
        "Count requested: total=%d distinct_names=%d (event_name=%s q=%s)",
        total, len(by_name), event_name, q,
    )
    return jsonify({
        "total": total,
        "distinct_names": len(by_name),
        "by_name": by_name,
    })


@app.route("/api/events/property_keys", methods=["GET"])
def list_property_keys():
    """フィルタ後のイベントに登場した properties のキーと「そのキーを持つイベント件数」を返す。

    `/api/events/names/<name>` の `distinct_property_keys` は特定の event_name に限定された
    キー一覧しか返せないため、UI で「保持中の全イベントを横断して、どんな properties
    キーが使われているか」を一覧したいケースで複数リクエストの集約が必要になる。
    このエンドポイントは event_name に依存せず、フィルタ後の全イベントを横断して
    properties キーとその出現件数を返す。1 イベント内で同じキーは 1 度だけ数えるため、
    `count` は「そのキーを少なくとも 1 つ持つイベント数」になる。

    クエリ:
    - `event_name`: 完全一致フィルタ（既存 `/summary` `/count` と同じ）
    - `q`: event_name の部分一致（大文字小文字無視、既存と同じ）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ
    - `order`: `asc` / `desc`(既定 `asc`、キー名の昇順)
    - `limit` / `offset`: `DEFAULT_PAGE_LIMIT` / `MAX_PAGE_LIMIT` を流用してページング
    """
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")
    sort_order = request.args.get("order", "asc")
    limit = request.args.get("limit", DEFAULT_PAGE_LIMIT, type=int)
    offset = request.args.get("offset", 0, type=int)

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on property_keys: %s", q_err)
        return jsonify({"error": q_err}), 400

    if sort_order not in ALLOWED_SORT_ORDERS:
        logger.warning("Invalid sort order on property_keys: %s", sort_order)
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
        logger.warning("Invalid timestamp filter on property_keys: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on property_keys: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    key_counts: dict[str, int] = {}
    for event in filtered:
        props = event.get("properties")
        if not isinstance(props, dict):
            continue
        for key in {k for k in props.keys() if isinstance(k, str)}:
            key_counts[key] = key_counts.get(key, 0) + 1

    items = [{"key": k, "count": c} for k, c in key_counts.items()]
    reverse = sort_order == "desc"
    items.sort(key=lambda x: x["key"], reverse=reverse)

    total = len(items)
    page = items[offset:offset + limit]
    logger.info(
        "Property keys requested: total_events=%d distinct_keys=%d (event_name=%s q=%s)",
        len(filtered), total, event_name, q,
    )
    return jsonify({
        "total_events": len(filtered),
        "distinct_property_keys": total,
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "order": sort_order,
        "property_keys": page,
    })


_ALLOWED_PROPERTY_VALUE_SORT_FIELDS = {"value", "count"}


def _is_jsonable_scalar(v: object) -> bool:
    """`property_values` で集計対象とするスカラ値かを判定する。

    JSON のスカラとしてそのまま返せる型に限定する。`dict` や `list` は
    キーとして hashable でないため Counter 集計に乗らないことに加え、
    ドロップダウン用途では値ごとの絞り込みができないので除外する。
    `bool` は `int` のサブクラスだが、明示的に列挙して将来の型増加に追従しやすくする。
    """
    return v is None or isinstance(v, (str, int, float, bool))


@app.route("/api/events/property_values/<path:key>", methods=["GET"])
def list_property_values(key: str):
    """指定 `key` の distinct な property 値とその出現回数を返す。

    `/api/events/property_keys` がキー名のリストを返すのに対し、本エンドポイントは
    特定キーの値の distribution を返す。UI のフィルタドロップダウン populate や
    「最も多い値トップ N」表示など、`/api/events` 全件取得を避けたい用途を想定。

    パスパラメータ:
        key: properties オブジェクトのキー名（trim 後の長さは MAX_EVENT_NAME_LENGTH 以内）。

    クエリパラメータ:
        - `event_name`: 完全一致フィルタ（property_keys と整合）
        - `q`: event_name の部分一致（大文字小文字無視）
        - `since` / `until`: ISO8601 範囲フィルタ
        - `sort`: `count` (既定) または `value`。
        - `order`: `asc` / `desc`(既定 `desc`、頻度の高い順)。
        - `limit` / `offset`: DEFAULT_PAGE_LIMIT / MAX_PAGE_LIMIT を流用。

    値の型は str / int / float / bool / None を許容。dict / list 等の非スカラ型は
    集計対象から除外し、`property_values` に登場させない（hashable でないため
    Counter 集計が成立しないことと、UI でフィルタ条件として使いにくいため）。

    レスポンス:
        {
          key, total_events, events_with_key, distinct_property_values,
          count, total, limit, offset, sort, order,
          property_values: [{value, count}, ...]
        }
    """
    normalized_key = key.strip()
    if not normalized_key:
        logger.warning("Blank property key on property_values")
        return jsonify({"error": "'key' must not be blank"}), 400
    if len(normalized_key) > MAX_EVENT_NAME_LENGTH:
        logger.warning("Property key too long on property_values: %d", len(normalized_key))
        return jsonify({
            "error": "'key' is too long",
            "max_length": MAX_EVENT_NAME_LENGTH,
        }), 400

    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")
    sort_field = request.args.get("sort", "count")
    sort_order = request.args.get("order", "desc")
    limit = request.args.get("limit", DEFAULT_PAGE_LIMIT, type=int)
    offset = request.args.get("offset", 0, type=int)

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on property_values: %s", q_err)
        return jsonify({"error": q_err}), 400

    if sort_field not in _ALLOWED_PROPERTY_VALUE_SORT_FIELDS:
        logger.warning("Invalid sort field on property_values: %s", sort_field)
        return jsonify({
            "error": "Invalid sort field",
            "allowed": sorted(_ALLOWED_PROPERTY_VALUE_SORT_FIELDS),
        }), 400

    if sort_order not in ALLOWED_SORT_ORDERS:
        logger.warning("Invalid sort order on property_values: %s", sort_order)
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
        logger.warning("Invalid timestamp filter on property_values: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on property_values: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    value_counts: dict[object, int] = {}
    events_with_key = 0
    skipped_non_scalar = 0
    for event in filtered:
        props = event.get("properties")
        if not isinstance(props, dict) or normalized_key not in props:
            continue
        events_with_key += 1
        raw_value = props[normalized_key]
        if not _is_jsonable_scalar(raw_value):
            skipped_non_scalar += 1
            continue
        value_counts[raw_value] = value_counts.get(raw_value, 0) + 1

    items = [{"value": v, "count": c} for v, c in value_counts.items()]
    reverse = sort_order == "desc"
    if sort_field == "count":
        # count 同値時は value 表示順を安定化するため secondary key として
        # 値の文字列表現を使う(reverse の影響は受けない)。
        items.sort(key=lambda x: str(x["value"]))
        items.sort(key=lambda x: x["count"], reverse=reverse)
    else:
        # 値の比較は型混在に弱いため文字列表現で行う(タイブレーカ用ではなく主キー)。
        items.sort(key=lambda x: str(x["value"]), reverse=reverse)

    total = len(items)
    page = items[offset:offset + limit]

    if skipped_non_scalar:
        logger.info(
            "Property values: skipped %d non-scalar values for key=%s",
            skipped_non_scalar, normalized_key,
        )
    logger.info(
        "Property values requested: key=%s total_events=%d events_with_key=%d distinct_values=%d",
        normalized_key, len(filtered), events_with_key, total,
    )
    return jsonify({
        "key": normalized_key,
        "total_events": len(filtered),
        "events_with_key": events_with_key,
        "distinct_property_values": total,
        "count": len(page),
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort_field,
        "order": sort_order,
        "property_values": page,
    })


@app.route("/api/events/by_day", methods=["GET"])
def events_by_day():
    """フィルタ通過後のイベントを UTC 日付 (YYYY-MM-DD) でビニングし、
    日付昇順の時系列カウントを返す軽量集計エンドポイント。

    `/api/events` 全件取得 → クライアントビニングに比べて、`MAX_EVENTS` 上限近くまで
    保持件数が増えた状況でのペイロード量・JSON エンコード時間を削減する。
    `processor-go` 側の `/api/messages/by_day` と同じ集計ポリシーで、母集団 0 の
    日付は省略する。日付は ISO 形式 (`YYYY-MM-DD`) の lex 昇順で固定（lex 順 =
    時系列順）。

    クエリ:
    - `event_name`: 完全一致フィルタ
    - `q`: event_name の部分一致（大文字小文字無視）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ
    """
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on by_day: %s", q_err)
        return jsonify({"error": q_err}), 400

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on by_day: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on by_day: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    counts: dict[str, int] = {}
    for event in filtered:
        # POST 時に UTC ISO8601 で書き込まれている前提だが、壊れた timestamp は
        # 集計に含めない(`_filter_events_by_time` と同じ防御)。日付抽出も UTC へ
        # 変換してから行い、日付境界での分裂を避ける(processor-go の by_day と同じ方針)。
        raw_ts = event.get("timestamp")
        if not isinstance(raw_ts, str):
            continue
        try:
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        day = dt.strftime("%Y-%m-%d")
        counts[day] = counts.get(day, 0) + 1

    # ISO 日付 (`YYYY-MM-DD`) の lex 順は時系列順と一致するため、sorted で十分。
    by_day = [{"day": d, "count": counts[d]} for d in sorted(counts.keys())]
    total = sum(counts.values())
    logger.info(
        "by_day requested: total=%d distinct_days=%d (event_name=%s q=%s)",
        total, len(by_day), event_name, q,
    )
    return jsonify({
        "total": total,
        "distinct_days": len(by_day),
        "by_day": by_day,
    })


@app.route("/api/events/by_month", methods=["GET"])
def events_by_month():
    """フィルタ通過後のイベントを UTC 月 (YYYY-MM) でビニングし、
    月昇順の時系列カウントを返す軽量集計エンドポイント。

    `/api/events/by_day` が日単位の細かい推移を見るのに対し、本エンドポイントは
    月次の長期トレンドを 1 リクエストで把握するためのより粗い粒度の集計。
    `usermgmt-ts` 側の `/api/users/by_month` と同じ集計ポリシーで、母集団 0 の
    月は省略する (populated-only)。
    月キーは `YYYY-MM` の lex 昇順で固定(lex 順 = カレンダー順)。

    クエリ:
    - `event_name`: 完全一致フィルタ
    - `q`: event_name の部分一致（大文字小文字無視）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ
    """
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on by_month: %s", q_err)
        return jsonify({"error": q_err}), 400

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on by_month: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on by_month: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    counts: dict[str, int] = {}
    for event in filtered:
        # POST 時に UTC ISO8601 で書き込まれている前提だが、壊れた timestamp は
        # 集計に含めない(by_day と同じ防御)。月抽出も UTC 正規化後に行い、
        # 月境界での分裂を避ける(by_day / usermgmt-ts の by_month と同じ方針)。
        raw_ts = event.get("timestamp")
        if not isinstance(raw_ts, str):
            continue
        try:
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        month = dt.strftime("%Y-%m")
        counts[month] = counts.get(month, 0) + 1

    # ISO 月 (`YYYY-MM`) の lex 順はカレンダー昇順と一致するため、sorted で十分。
    by_month = [{"month": m, "count": counts[m]} for m in sorted(counts.keys())]
    total = sum(counts.values())
    logger.info(
        "by_month requested: total=%d distinct_months=%d (event_name=%s q=%s)",
        total, len(by_month), event_name, q,
    )
    return jsonify({
        "total": total,
        "distinct_months": len(by_month),
        "by_month": by_month,
    })


@app.route("/api/events/by_week", methods=["GET"])
def events_by_week():
    """フィルタ通過後のイベントを ISO 8601 週 (`YYYY-Www`) でビニングし、
    週昇順の時系列カウントを返す軽量集計エンドポイント。

    `/api/events/by_day` は日単位で細かすぎ、`/api/events/by_month` は月単位で
    粗すぎるという中間粒度を埋める。運用系ダッシュボードで多い「直近 8〜12 週の
    週次推移」を 1 リクエストで取得できる。既存 `by_day` / `by_month` と同じ
    populated-only 方針で、母集団 0 の週は省略する。

    週キーは ISO 8601 の週年・週番号を `YYYY-Www` (例: `2026-W26`) の 8 文字
    文字列で返す。ISO 週年は暦年と一致しないケースがある (12 月末が翌年の第 1 週
    に、1 月頭が前年の最終週になる) ため、`dt.isocalendar()` の `year` / `week` を
    使用する。週番号は 2 桁ゼロ詰めなので lex 昇順 = カレンダー昇順になる。

    クエリ:
    - `event_name`: 完全一致フィルタ
    - `q`: event_name の部分一致（大文字小文字無視）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ
    """
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on by_week: %s", q_err)
        return jsonify({"error": q_err}), 400

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on by_week: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on by_week: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    counts: dict[str, int] = {}
    for event in filtered:
        # POST 時に UTC ISO8601 で書き込まれている前提だが、壊れた timestamp は
        # 集計に含めない(by_day / by_month と同じ防御)。週抽出も UTC 正規化後に
        # 行い、tz 越境で ISO 週が変わるケースに耐える。
        raw_ts = event.get("timestamp")
        if not isinstance(raw_ts, str):
            continue
        try:
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        iso_year, iso_week, _ = dt.isocalendar()
        # 週番号は 2 桁ゼロ詰めで `YYYY-Www` にする (lex 昇順 = カレンダー昇順)。
        week_key = f"{iso_year:04d}-W{iso_week:02d}"
        counts[week_key] = counts.get(week_key, 0) + 1

    # `YYYY-Www` の lex 順は ISO 週年・週番号順と一致するため sorted で十分。
    by_week = [{"week": w, "count": counts[w]} for w in sorted(counts.keys())]
    total = sum(counts.values())
    logger.info(
        "by_week requested: total=%d distinct_weeks=%d (event_name=%s q=%s)",
        total, len(by_week), event_name, q,
    )
    return jsonify({
        "total": total,
        "distinct_weeks": len(by_week),
        "by_week": by_week,
    })


@app.route("/api/events/by_hour_of_day", methods=["GET"])
def events_by_hour_of_day():
    """フィルタ通過後のイベントを UTC の時刻 (`"00"`〜`"23"`) でビニングし、
    時刻昇順の周期的カウントを返す軽量集計エンドポイント。

    `/api/events/by_day` が「いつ」流量があったかを直線的な時系列で見るのに対し、
    本エンドポイントは「1 日のうち、どの時間帯に流量が集中しているか」を 1 リクエスト
    で把握するための周期的集計。`processor-go` 側の `/api/messages/by_hour_of_day`
    と同じ集計ポリシーで、母集団 0 の時間帯は省略する。
    時刻は 2 桁ゼロ詰め (`"00"`〜`"23"`) なので lex 順 = 時間順になる。

    クエリ:
    - `event_name`: 完全一致フィルタ
    - `q`: event_name の部分一致（大文字小文字無視）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ
    """
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on by_hour_of_day: %s", q_err)
        return jsonify({"error": q_err}), 400

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on by_hour_of_day: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on by_hour_of_day: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    counts: dict[str, int] = {}
    for event in filtered:
        # POST 時に UTC ISO8601 で書き込まれている前提だが、壊れた timestamp は
        # 集計に含めない(`_filter_events_by_time` と同じ防御)。時刻抽出も UTC へ
        # 変換してから行い、時刻境界での分裂を避ける(by_day と同じ方針)。
        raw_ts = event.get("timestamp")
        if not isinstance(raw_ts, str):
            continue
        try:
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        hour = dt.strftime("%H")
        counts[hour] = counts.get(hour, 0) + 1

    # 2 桁ゼロ詰め時刻 (`"00"`〜`"23"`) は lex 順 = 時間順なので sorted で十分。
    by_hour = [{"hour": h, "count": counts[h]} for h in sorted(counts.keys())]
    total = sum(counts.values())
    logger.info(
        "by_hour_of_day requested: total=%d distinct_hours=%d (event_name=%s q=%s)",
        total, len(by_hour), event_name, q,
    )
    return jsonify({
        "total": total,
        "distinct_hours": len(by_hour),
        "by_hour": by_hour,
    })


@app.route("/api/events/by_day_of_week", methods=["GET"])
def events_by_day_of_week():
    """フィルタ通過後のイベントを UTC の ISO 8601 曜日 (`"1"`=月曜〜`"7"`=日曜) で
    ビニングし、曜日昇順の周期的カウントを返す軽量集計エンドポイント。

    `/api/events/by_day` が日単位の直線的な時系列、`/api/events/by_hour_of_day` が
    1 日内の時刻別分布を見るのに対し、本エンドポイントは「曜日ごとにどう分布して
    いるか」という周期的パターン（平日 vs 週末 / 月曜は多い等）を 1 リクエスト
    で把握する。`processor-go` 側の `/api/messages/by_day_of_week` と同じ集計
    ポリシーで、母集団 0 の曜日は省略する。

    キーは ISO 8601 曜日 (1=月曜〜7=日曜) の単一桁文字列なので lex 順 = 曜日順。

    クエリ:
    - `event_name`: 完全一致フィルタ
    - `q`: event_name の部分一致（大文字小文字無視）
    - `since` / `until`: ISO8601 タイムスタンプ範囲フィルタ
    """
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on by_day_of_week: %s", q_err)
        return jsonify({"error": q_err}), 400

    since = None
    until = None
    try:
        if since_raw is not None:
            since = _parse_iso_datetime(since_raw, "since")
        if until_raw is not None:
            until = _parse_iso_datetime(until_raw, "until")
    except ValueError as exc:
        logger.warning("Invalid timestamp filter on by_day_of_week: %s", exc)
        return jsonify({"error": str(exc)}), 400

    if since is not None and until is not None and since > until:
        logger.warning("Invalid range on by_day_of_week: since=%s > until=%s", since, until)
        return jsonify({"error": "'since' must be less than or equal to 'until'"}), 400

    with events_lock:
        filtered = list(events_store)
    if event_name:
        filtered = [e for e in filtered if e["event_name"] == event_name]
    filtered = _filter_events_by_q(filtered, q)
    filtered = _filter_events_by_time(filtered, since, until)

    counts: dict[str, int] = {}
    for event in filtered:
        # POST 時に UTC ISO8601 で書き込まれている前提だが、壊れた timestamp は
        # 集計に含めない(by_hour_of_day と同じ防御)。曜日抽出も UTC 正規化後に
        # 行い、tz 越境で曜日が変わるケースに耐える。
        raw_ts = event.get("timestamp")
        if not isinstance(raw_ts, str):
            continue
        try:
            dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        # isoweekday(): 1=月曜〜7=日曜
        dow = str(dt.isoweekday())
        counts[dow] = counts.get(dow, 0) + 1

    # "1"〜"7" の単一桁文字列は lex 順 = 曜日順なので sorted で十分。
    by_day_of_week = [{"day_of_week": d, "count": counts[d]} for d in sorted(counts.keys())]
    total = sum(counts.values())
    logger.info(
        "by_day_of_week requested: total=%d distinct_days_of_week=%d (event_name=%s q=%s)",
        total, len(by_day_of_week), event_name, q,
    )
    return jsonify({
        "total": total,
        "distinct_days_of_week": len(by_day_of_week),
        "by_day_of_week": by_day_of_week,
    })


@app.route("/api/events/summary", methods=["GET"])
def events_summary():
    event_name = request.args.get("event_name")
    q_raw = request.args.get("q")
    since_raw = request.args.get("since")
    until_raw = request.args.get("until")

    q, q_err = _normalize_q(q_raw)
    if q_err is not None:
        logger.warning("Invalid q filter on summary: %s", q_err)
        return jsonify({"error": q_err}), 400

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
    filtered = _filter_events_by_q(filtered, q)
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
