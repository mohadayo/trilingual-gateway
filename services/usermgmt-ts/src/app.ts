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

app.get("/api/users/:id", (req: Request<{ id: string }>, res: Response) => {
  const user = users.get(req.params.id);
  if (!user) {
    res.status(404).json({ error: "user not found" });
    return;
  }
  res.json(user);
});

app.put("/api/users/:id", (req: Request<{ id: string }>, res: Response) => {
  const user = users.get(req.params.id);
  if (!user) {
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
    res.status(404).json({ error: "user not found" });
    return;
  }
  users.delete(req.params.id);
  log("INFO", `Deleted user: ${req.params.id}`);
  res.status(204).send();
});

export { app, users, MAX_REQUEST_BODY };

if (require.main === module) {
  const port = process.env.USERMGMT_PORT || 8003;
  app.listen(port, () => {
    log("INFO", `Starting user management service on port ${port}`);
  });
}
