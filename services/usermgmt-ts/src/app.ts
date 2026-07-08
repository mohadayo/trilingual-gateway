import express, { NextFunction, Request, Response } from "express";
import { v4 as uuidv4 } from "uuid";

// JSON ペイロードの最大サイズ。express.json の既定は 100kb だが、
// 運用環境ごとに上書きできるよう環境変数で明示する。
const MAX_REQUEST_BODY = process.env.MAX_REQUEST_BODY || "100kb";

const app = express();
app.use(express.json({ limit: MAX_REQUEST_BODY }));

// express.json の limit 超過は entity.too.large になる。既定ハンドラに
// 任せると HTML 風のエラーが返るため、専用ハンドラで 413 + JSON にする。
app.use(
  (
    err: Error & { type?: string; status?: number; statusCode?: number },
    _req: Request,
    res: Response,
    next: NextFunction,
  ) => {
    const status = err.status ?? err.statusCode;
    if (err && (err.type === "entity.too.large" || status === 413)) {
      log("WARN", `Request body too large (limit=${MAX_REQUEST_BODY})`);
      res.status(413).json({ error: "request body too large" });
      return;
    }
    next(err);
  },
);

interface User {
  id: string;
  username: string;
  email: string;
  role: string;
  created_at: string;
  updated_at: string;
}

const users: Map<string, User> = new Map();

const log = (level: string, message: string) => {
  const ts = new Date().toISOString();
  console.log(`${ts} [${level}] usermgmt-ts: ${message}`);
};

const MAX_USERNAME_LENGTH = parseInt(
  process.env.MAX_USERNAME_LENGTH || "50",
  10,
);
const MAX_EMAIL_LENGTH = 254; // RFC 5321
const ALLOWED_ROLES = new Set(["user", "admin", "moderator"]);
const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

const USERS_DEFAULT_LIMIT = Math.max(
  1,
  parseInt(process.env.USERS_DEFAULT_LIMIT || "50", 10) || 50,
);
const USERS_MAX_LIMIT = Math.max(
  USERS_DEFAULT_LIMIT,
  parseInt(process.env.USERS_MAX_LIMIT || "200", 10) || 200,
);
const MAX_SEARCH_LENGTH = parseInt(
  process.env.MAX_SEARCH_LENGTH || "100",
  10,
);

type SortField =
  | "created_at"
  | "updated_at"
  | "username"
  | "email"
  | "role";
type SortOrder = "asc" | "desc";
const ALLOWED_SORT_FIELDS = new Set<SortField>([
  "created_at",
  "updated_at",
  "username",
  "email",
  "role",
]);
const ALLOWED_SORT_ORDERS = new Set<SortOrder>(["asc", "desc"]);

// ISO 8601 / RFC 3339 文字列を Date に変換する。
// `Z` サフィックスは `+00:00` として扱う（Node の Date は両方を受けるが、
// processor-go と統一するため明示的に正規化）。空文字や不正フォーマットは Error。
function parseIsoDateTime(value: string, field: string): Date {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    throw new Error(`${field} must not be blank`);
  }
  const normalized = trimmed.endsWith("Z")
    ? `${trimmed.slice(0, -1)}+00:00`
    : trimmed;
  const d = new Date(normalized);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`${field} must be an ISO 8601 / RFC 3339 datetime`);
  }
  return d;
}

function parseListQuery(
  query: Record<string, unknown>,
): {
  ok: true;
  limit: number;
  offset: number;
  role: string | null;
  q: string | null;
  sort: SortField;
  order: SortOrder;
  since: Date | null;
  until: Date | null;
} | { ok: false; error: string } {
  let limit = USERS_DEFAULT_LIMIT;
  let offset = 0;
  let role: string | null = null;
  let q: string | null = null;
  let sort: SortField = "created_at";
  let order: SortOrder = "asc";
  let since: Date | null = null;
  let until: Date | null = null;

  if (query.limit !== undefined) {
    const raw = String(query.limit);
    if (!/^-?\d+$/.test(raw)) {
      return { ok: false, error: "limit must be an integer" };
    }
    const n = parseInt(raw, 10);
    if (n < 1) {
      return { ok: false, error: "limit must be >= 1" };
    }
    if (n > USERS_MAX_LIMIT) {
      return {
        ok: false,
        error: `limit must be <= ${USERS_MAX_LIMIT}`,
      };
    }
    limit = n;
  }

  if (query.offset !== undefined) {
    const raw = String(query.offset);
    if (!/^-?\d+$/.test(raw)) {
      return { ok: false, error: "offset must be an integer" };
    }
    const n = parseInt(raw, 10);
    if (n < 0) {
      return { ok: false, error: "offset must be >= 0" };
    }
    offset = n;
  }

  if (query.role !== undefined) {
    const r = String(query.role).trim();
    if (!ALLOWED_ROLES.has(r)) {
      return {
        ok: false,
        error: `role must be one of: ${Array.from(ALLOWED_ROLES).join(", ")}`,
      };
    }
    role = r;
  }

  if (query.q !== undefined) {
    const raw = String(query.q);
    if (raw.length > MAX_SEARCH_LENGTH) {
      return {
        ok: false,
        error: `q must be at most ${MAX_SEARCH_LENGTH} characters`,
      };
    }
    const trimmed = raw.trim();
    if (trimmed.length > 0) {
      q = trimmed.toLowerCase();
    }
  }

  if (query.sort !== undefined) {
    const raw = String(query.sort);
    if (!ALLOWED_SORT_FIELDS.has(raw as SortField)) {
      return {
        ok: false,
        error: `sort must be one of: ${Array.from(ALLOWED_SORT_FIELDS).join(", ")}`,
      };
    }
    sort = raw as SortField;
  }

  if (query.order !== undefined) {
    const raw = String(query.order);
    if (!ALLOWED_SORT_ORDERS.has(raw as SortOrder)) {
      return {
        ok: false,
        error: `order must be one of: ${Array.from(ALLOWED_SORT_ORDERS).join(", ")}`,
      };
    }
    order = raw as SortOrder;
  }

  if (query.since !== undefined) {
    try {
      since = parseIsoDateTime(String(query.since), "since");
    } catch (e) {
      return { ok: false, error: (e as Error).message };
    }
  }

  if (query.until !== undefined) {
    try {
      until = parseIsoDateTime(String(query.until), "until");
    } catch (e) {
      return { ok: false, error: (e as Error).message };
    }
  }

  if (since !== null && until !== null && until < since) {
    return {
      ok: false,
      error: "until must be greater than or equal to since",
    };
  }

  return { ok: true, limit, offset, role, q, sort, order, since, until };
}

interface ValidatedInput {
  username?: string;
  email?: string;
  role?: string;
}

function validateUserInput(
  body: Record<string, unknown>,
  opts: { partial: boolean },
): { ok: true; data: ValidatedInput } | { ok: false; error: string } {
  const out: ValidatedInput = {};

  const hasUsername = body.username !== undefined && body.username !== null;
  const hasEmail = body.email !== undefined && body.email !== null;
  const hasRole = body.role !== undefined && body.role !== null;

  if (!opts.partial) {
    if (!hasUsername) return { ok: false, error: "username is required" };
    if (!hasEmail) return { ok: false, error: "email is required" };
  } else if (!hasUsername && !hasEmail && !hasRole) {
    return { ok: false, error: "at least one field is required to update" };
  }

  if (hasUsername) {
    if (typeof body.username !== "string") {
      return { ok: false, error: "username must be a string" };
    }
    const trimmed = body.username.trim();
    if (trimmed.length === 0) {
      return { ok: false, error: "username must not be blank" };
    }
    if (trimmed.length > MAX_USERNAME_LENGTH) {
      return {
        ok: false,
        error: `username must be at most ${MAX_USERNAME_LENGTH} characters`,
      };
    }
    out.username = trimmed;
  }

  if (hasEmail) {
    if (typeof body.email !== "string") {
      return { ok: false, error: "email must be a string" };
    }
    const trimmed = body.email.trim();
    if (trimmed.length === 0) {
      return { ok: false, error: "email must not be blank" };
    }
    if (trimmed.length > MAX_EMAIL_LENGTH) {
      return {
        ok: false,
        error: `email must be at most ${MAX_EMAIL_LENGTH} characters`,
      };
    }
    if (!EMAIL_REGEX.test(trimmed)) {
      return { ok: false, error: "email format is invalid" };
    }
    out.email = trimmed.toLowerCase();
  }

  if (hasRole) {
    if (typeof body.role !== "string") {
      return { ok: false, error: "role must be a string" };
    }
    const trimmed = body.role.trim();
    if (!ALLOWED_ROLES.has(trimmed)) {
      return {
        ok: false,
        error: `role must be one of: ${Array.from(ALLOWED_ROLES).join(", ")}`,
      };
    }
    out.role = trimmed;
  }

  return { ok: true, data: out };
}

app.get("/health", (_req: Request, res: Response) => {
  res.json({
    status: "ok",
    service: "usermgmt-ts",
    timestamp: new Date().toISOString(),
  });
});

app.post("/api/users", (req: Request, res: Response) => {
  const validated = validateUserInput(req.body || {}, { partial: false });
  if (!validated.ok) {
    log("WARN", `POST /api/users rejected: ${validated.error}`);
    res.status(400).json({ error: validated.error });
    return;
  }
  const { username, email, role } = validated.data;

  const existing = Array.from(users.values()).find((u) => u.email === email);
  if (existing) {
    log("WARN", `Duplicate email: ${email}`);
    res.status(409).json({ error: "email already exists" });
    return;
  }

  const now = new Date().toISOString();
  const user: User = {
    id: uuidv4(),
    username: username as string,
    email: email as string,
    role: role || "user",
    created_at: now,
    updated_at: now,
  };

  users.set(user.id, user);
  log("INFO", `Created user: ${user.username} (${user.id})`);
  res.status(201).json(user);
});

app.get("/api/users", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { limit, offset, role, q, sort, order, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      // created_at は POST/PUT 時に new Date().toISOString() で書き込んでいるため
      // パース失敗は通常起き得ないが、保険として除外扱いにする
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  const reverse = order === "desc";
  list.sort((a, b) => {
    const av = a[sort];
    const bv = b[sort];
    if (av < bv) return reverse ? 1 : -1;
    if (av > bv) return reverse ? -1 : 1;
    return 0;
  });

  const total = list.length;
  const page = list.slice(offset, offset + limit);
  res.json({
    users: page,
    count: page.length,
    total,
    limit,
    offset,
    sort,
    order,
  });
});

// `/api/users/:id` より前に登録して、`:id == "count"` 衝突を防ぐ。
// `parseListQuery` のうちフィルタ系（role / q / since / until）のみ評価し、
// `limit / offset / sort / order` は count では意味を持たないため無視する。
app.get("/api/users/count", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users/count rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { role, q, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  // ALLOWED_ROLES の全キーを 0 初期化。クライアントは存在チェック無しで参照できる。
  const byRole: Record<string, number> = {};
  for (const r of ALLOWED_ROLES) {
    byRole[r] = 0;
  }
  // `created_at` の最小・最大を 1 スキャンで集計する（processor-go の
  // `/api/stats` の `oldest`/`newest` と同じセマンティクス）。
  // 0 件のときは null を返し、1 件以上のときは ISO8601 文字列に戻す。
  // `Number.NaN` な `created_at`（壊れた値）は安全側に倒して無視する。
  let oldestMs = Number.POSITIVE_INFINITY;
  let newestMs = Number.NEGATIVE_INFINITY;
  let oldestIso: string | null = null;
  let newestIso: string | null = null;
  for (const u of list) {
    byRole[u.role] = (byRole[u.role] ?? 0) + 1;
    const t = new Date(u.created_at).getTime();
    if (Number.isNaN(t)) continue;
    if (t < oldestMs) {
      oldestMs = t;
      oldestIso = u.created_at;
    }
    if (t > newestMs) {
      newestMs = t;
      newestIso = u.created_at;
    }
  }
  res.json({
    total: list.length,
    by_role: byRole,
    oldest_created_at: oldestIso,
    newest_created_at: newestIso,
  });
});

// `/api/users/:id` より前に登録して、`:id == "by_day_of_week"` 衝突を防ぐ。
// `parseListQuery` のうちフィルタ系（role / q / since / until）のみ評価し、
// `limit / offset / sort / order` は集計では意味を持たないため無視する。
//
// 集計は UTC 上の ISO 8601 曜日 (1=月曜〜7=日曜) で行う。
// JavaScript の `Date.getUTCDay()` は 0=日曜〜6=土曜を返すため、
// `(day === 0 ? 7 : day)` で ISO 8601 に正規化する。
// populated-only: 母集団 0 の曜日は配列に含めない（`processor-go`/`analytics-py`
// の `by_day_of_week` と同じ方針）。
app.get("/api/users/by_day_of_week", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users/by_day_of_week rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { role, q, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  // ISO 8601 曜日キー ("1"〜"7") → 件数。
  // 壊れた `created_at` (パース不能) は安全側で集計対象外。
  const counts = new Map<string, number>();
  let total = 0;
  for (const u of list) {
    const ts = new Date(u.created_at);
    if (Number.isNaN(ts.getTime())) continue;
    const day = ts.getUTCDay(); // 0=Sun..6=Sat
    const iso = day === 0 ? 7 : day; // 1=Mon..7=Sun
    const key = String(iso);
    counts.set(key, (counts.get(key) ?? 0) + 1);
    total += 1;
  }

  // 曜日キーの lex 昇順 = 曜日順 ("1" < "2" < ... < "7")。
  // populated-only: 値が 0 の曜日は含めない。
  const byDayOfWeek = Array.from(counts.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([day_of_week, count]) => ({ day_of_week, count }));

  res.json({
    total,
    distinct_days_of_week: byDayOfWeek.length,
    by_day_of_week: byDayOfWeek,
  });
});

// `/api/users/:id` より前に登録して、`:id == "by_hour_of_day"` 衝突を防ぐ。
// `parseListQuery` のうちフィルタ系（role / q / since / until）のみ評価し、
// `limit / offset / sort / order` は集計では意味を持たないため無視する。
//
// 集計は UTC 上の時刻 (`"00"`〜`"23"`) で行う。2 桁ゼロ詰めで lex 順 = 時間順を
// 保つ（`processor-go`/`analytics-py` の `by_hour_of_day` と同じ規約）。
// populated-only: 母集団 0 の時間帯は配列に含めない（既存 `by_day_of_week` /
// 他 2 サービスの方針に揃える）。
app.get("/api/users/by_hour_of_day", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users/by_hour_of_day rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { role, q, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  // UTC 時刻キー ("00"〜"23") → 件数。
  // 壊れた `created_at` (パース不能) は安全側で集計対象外(既存 by_day_of_week と同じ)。
  // 2 桁ゼロ詰めで lex 順 = 時間順を保つ（processor-go / analytics-py と同じ規約）。
  const counts = new Map<string, number>();
  let total = 0;
  for (const u of list) {
    const ts = new Date(u.created_at);
    if (Number.isNaN(ts.getTime())) continue;
    const hour = ts.getUTCHours();
    const key = hour.toString().padStart(2, "0");
    counts.set(key, (counts.get(key) ?? 0) + 1);
    total += 1;
  }

  // 時刻キーの lex 昇順 = 時間順 ("00" < "01" < ... < "23")。
  // populated-only: 値が 0 の時刻は含めない（by_day_of_week と同じ）。
  const byHourOfDay = Array.from(counts.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([hour, count]) => ({ hour, count }));

  res.json({
    total,
    distinct_hours: byHourOfDay.length,
    by_hour_of_day: byHourOfDay,
  });
});

// `/api/users/:id` より前に登録して、`:id == "by_day"` 衝突を防ぐ。
// `parseListQuery` のうちフィルタ系（role / q / since / until）のみ評価し、
// `limit / offset / sort / order` は集計では意味を持たないため無視する。
//
// バケットキーは UTC カレンダー日付（`YYYY-MM-DD`）。`toISOString().slice(0,10)`
// で常に UTC の日付文字列を得られる。lex 昇順 = カレンダー昇順を保つため
// ソートは自然順で足りる（`processor-go` / `analytics-py` の `by_day` と同じ規約）。
// populated-only: 母集団 0 の日付は配列に含めない（既存 `by_day_of_week` /
// `by_hour_of_day` と同じ方針）。3 サービスの時間軸集計の対称性を完成させる。
app.get("/api/users/by_day", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users/by_day rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { role, q, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  // UTC 日付キー ("YYYY-MM-DD") → 件数。
  // 壊れた `created_at` (パース不能) は安全側で集計対象外（既存の他集計と同じ）。
  const counts = new Map<string, number>();
  let total = 0;
  for (const u of list) {
    const ts = new Date(u.created_at);
    if (Number.isNaN(ts.getTime())) continue;
    const key = ts.toISOString().slice(0, 10);
    counts.set(key, (counts.get(key) ?? 0) + 1);
    total += 1;
  }

  // 日付キーの lex 昇順 = カレンダー昇順（"2026-06-30" < "2026-07-01"）。
  // populated-only: 件数 0 の日付は含めない（by_day_of_week / by_hour_of_day と同じ）。
  const byDay = Array.from(counts.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([day, count]) => ({ day, count }));

  res.json({
    total,
    distinct_days: byDay.length,
    by_day: byDay,
  });
});

// `/api/users/:id` より前に登録して、`:id == "by_month"` 衝突を防ぐ。
// `parseListQuery` のうちフィルタ系（role / q / since / until）のみ評価し、
// `limit / offset / sort / order` は集計では意味を持たないため無視する。
//
// バケットキーは UTC カレンダー月（`YYYY-MM`）。`toISOString().slice(0,7)`
// で常に UTC の年月文字列を得られる。lex 昇順 = カレンダー昇順を保つため
// ソートは自然順で足りる（既存 `by_day` の `YYYY-MM-DD` と同じ設計思想）。
// populated-only: 母集団 0 の月は配列に含めない（既存 `by_day` /
// `by_hour_of_day` / `by_day_of_week` と同じ方針）。日次より粗い粒度で
// 月次の新規登録推移を 1 リクエストで把握するための集計。
app.get("/api/users/by_month", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users/by_month rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { role, q, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  // UTC 月キー ("YYYY-MM") → 件数。
  // 壊れた `created_at` (パース不能) は安全側で集計対象外（既存の他集計と同じ）。
  const counts = new Map<string, number>();
  let total = 0;
  for (const u of list) {
    const ts = new Date(u.created_at);
    if (Number.isNaN(ts.getTime())) continue;
    const key = ts.toISOString().slice(0, 7);
    counts.set(key, (counts.get(key) ?? 0) + 1);
    total += 1;
  }

  // 月キーの lex 昇順 = カレンダー昇順（"2026-01" < "2026-12"）。
  // populated-only: 件数 0 の月は含めない（by_day / by_hour_of_day / by_day_of_week と同じ）。
  const byMonth = Array.from(counts.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([month, count]) => ({ month, count }));

  res.json({
    total,
    distinct_months: byMonth.length,
    by_month: byMonth,
  });
});

// ISO 8601 週番号を「YYYY-Www」形式で返すヘルパ。木曜合わせアルゴリズム:
//   1. その日の週の木曜を求める（月曜起点、木曜が属する年 = ISO 週年）
//   2. その年 1/1 の木曜と何日離れているかで週番号を計算
// `Intl.DateTimeFormat` や `Date.toLocaleString` はランタイム / ロケール依存で
// ISO 週を返さないため、`Date.getUTCDay()` ベースで純関数として計算する。
// 入力 `Date` は既に UTC 側の値で渡す前提（呼び元で `new Date(iso).toISOString()`
// 経由するため、UTC 正規化は行われている）。
function toIsoWeekString(date: Date): string {
  // Copy in UTC space so mutations don't leak into the caller.
  const d = new Date(
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate(),
    ),
  );
  // ISO 週規則: 1 (月) 〜 7 (日)。 `getUTCDay()` は 0 (日) 〜 6 (土) を返すので変換。
  const dayNum = d.getUTCDay() === 0 ? 7 : d.getUTCDay();
  // その週の木曜へシフト（4 - dayNum 日分ずらす）。木曜が属する年が ISO 週年。
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const isoYear = d.getUTCFullYear();
  const yearStart = new Date(Date.UTC(isoYear, 0, 1));
  // 週番号 = ((その日 - 年始) / 7 日) + 1
  const weekNo = Math.ceil(
    ((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7,
  );
  const weekStr = weekNo < 10 ? `0${weekNo}` : `${weekNo}`;
  return `${isoYear}-W${weekStr}`;
}

// `/api/users/:id` より前に登録して、`:id == "by_week"` 衝突を防ぐ。
// `parseListQuery` のうちフィルタ系（role / q / since / until）のみ評価し、
// `limit / offset / sort / order` は集計では意味を持たないため無視する。
//
// バケットキーは ISO 週 (`YYYY-Www`、例 `"2026-W27"`)。`toIsoWeekString` で
// UTC 正規化済みの Date から ISO 8601 週規則（月曜起点・木曜合わせ）で計算する。
// lex 昇順 = カレンダー週昇順を保つため（"2026-W01" < "2026-W53"）、追加のソート
// キー変換は不要。populated-only: 母集団 0 の週は配列に含めない（既存 `by_day` /
// `by_month` / `by_hour_of_day` / `by_day_of_week` と同じ方針）。日次より粗く、
// 月次より細かい中間解像度で、四半期・半期スパンの登録推移を 1 リクエストで
// 把握するための集計。
app.get("/api/users/by_week", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users/by_week rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { role, q, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  // ISO 週キー ("YYYY-Www") → 件数。
  // 壊れた `created_at` (パース不能) は安全側で集計対象外（既存の他集計と同じ）。
  const counts = new Map<string, number>();
  let total = 0;
  for (const u of list) {
    const ts = new Date(u.created_at);
    if (Number.isNaN(ts.getTime())) continue;
    const key = toIsoWeekString(ts);
    counts.set(key, (counts.get(key) ?? 0) + 1);
    total += 1;
  }

  // 週キーの lex 昇順 = カレンダー昇順（"2026-W01" < "2026-W53"）。
  // populated-only: 件数 0 の週は含めない（by_day / by_month と同じ）。
  const byWeek = Array.from(counts.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([week, count]) => ({ week, count }));

  res.json({
    total,
    distinct_weeks: byWeek.length,
    by_week: byWeek,
  });
});

// email の `@` 以降を取り出して小文字化したドメイン文字列を返す。
// `@` を含まない不正 email や、`@` が末尾にあるだけの email (`foo@`) は null。
// バリデーション時に `EMAIL_REGEX` を通過している前提だが、破損保険として null を許す。
function extractEmailDomain(email: string): string | null {
  const idx = email.lastIndexOf("@");
  if (idx < 0 || idx === email.length - 1) {
    return null;
  }
  const domain = email.slice(idx + 1).trim();
  if (domain.length === 0) {
    return null;
  }
  return domain.toLowerCase();
}

// `/api/users/:id` より前に登録して、`:id == "by_domain"` 衝突を防ぐ。
// `parseListQuery` のうちフィルタ系（role / q / since / until）のみ評価し、
// `limit / offset / sort / order` は集計では意味を持たないため無視する。
//
// バケットキーは `email` の `@` 以降を小文字化したドメイン文字列。
// バリデーション時に `EMAIL_REGEX` と `toLowerCase()` を通過している前提だが、
// 破損保険として `extractEmailDomain` は null を返すことがあり、その場合は集計対象外。
// lex 昇順（domain 文字列の自然順）で返す。UI 側で count 降順が必要なら
// クライアント再ソートで対応（他 `by_*` と応答形状を揃えるため）。
// populated-only: 母集団 0 のドメインは含めない（他 `by_*` と同じ方針）。
//
// B2B SaaS のワークスペース単位の採用トラッキング、企業ドメインごとの分布把握、
// `example.com` などのテストドメイン混入検知などに使う。
app.get("/api/users/by_domain", (req: Request, res: Response) => {
  const parsed = parseListQuery(req.query as Record<string, unknown>);
  if (!parsed.ok) {
    log("WARN", `GET /api/users/by_domain rejected: ${parsed.error}`);
    res.status(400).json({ error: parsed.error });
    return;
  }
  const { role, q, since, until } = parsed;

  let list = Array.from(users.values());
  if (role !== null) {
    list = list.filter((u) => u.role === role);
  }
  if (q !== null) {
    list = list.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        u.email.toLowerCase().includes(q),
    );
  }
  if (since !== null || until !== null) {
    list = list.filter((u) => {
      const ts = new Date(u.created_at);
      if (Number.isNaN(ts.getTime())) {
        return false;
      }
      if (since !== null && ts < since) return false;
      if (until !== null && ts > until) return false;
      return true;
    });
  }

  // ドメインキー → 件数。extractEmailDomain が null のレコードは集計対象外。
  const counts = new Map<string, number>();
  let total = 0;
  for (const u of list) {
    const domain = extractEmailDomain(u.email);
    if (domain === null) continue;
    counts.set(domain, (counts.get(domain) ?? 0) + 1);
    total += 1;
  }

  // ドメインキーの lex 昇順（自然順）。UI 側で count 降順が必要ならクライアント再ソート。
  // populated-only: 件数 0 のドメインは含めない（他 by_* と同じ）。
  const byDomain = Array.from(counts.entries())
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([domain, count]) => ({ domain, count }));

  res.json({
    total,
    distinct_domains: byDomain.length,
    by_domain: byDomain,
  });
});

app.get("/api/users/:id", (req: Request<{ id: string }>, res: Response) => {
  const user = users.get(req.params.id);
  if (!user) {
    log("WARN", `GET /api/users/${req.params.id} not found`);
    res.status(404).json({ error: "user not found" });
    return;
  }
  res.json(user);
});

app.put("/api/users/:id", (req: Request<{ id: string }>, res: Response) => {
  const user = users.get(req.params.id);
  if (!user) {
    log("WARN", `PUT /api/users/${req.params.id} not found`);
    res.status(404).json({ error: "user not found" });
    return;
  }

  const validated = validateUserInput(req.body || {}, { partial: true });
  if (!validated.ok) {
    log("WARN", `PUT /api/users/${user.id} rejected: ${validated.error}`);
    res.status(400).json({ error: validated.error });
    return;
  }
  const { username, email, role } = validated.data;

  if (email && email !== user.email) {
    const duplicate = Array.from(users.values()).find(
      (u) => u.email === email && u.id !== user.id
    );
    if (duplicate) {
      log("WARN", `Duplicate email on update: ${email}`);
      res.status(409).json({ error: "email already exists" });
      return;
    }
  }

  if (username) user.username = username;
  if (email) user.email = email;
  if (role) user.role = role;
  user.updated_at = new Date().toISOString();

  users.set(user.id, user);
  log("INFO", `Updated user: ${user.username} (${user.id})`);
  res.json(user);
});

app.delete("/api/users/:id", (req: Request<{ id: string }>, res: Response) => {
  if (!users.has(req.params.id)) {
    log("WARN", `DELETE /api/users/${req.params.id} not found`);
    res.status(404).json({ error: "user not found" });
    return;
  }
  users.delete(req.params.id);
  log("INFO", `Deleted user: ${req.params.id}`);
  res.status(204).send();
});

export { app, users, MAX_REQUEST_BODY, toIsoWeekString, extractEmailDomain };

if (require.main === module) {
  const port = process.env.USERMGMT_PORT || 8003;
  app.listen(port, () => {
    log("INFO", `Starting user management service on port ${port}`);
  });
}
