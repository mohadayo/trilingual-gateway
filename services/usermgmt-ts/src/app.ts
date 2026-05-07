import express, { Request, Response } from "express";
import { v4 as uuidv4 } from "uuid";

const app = express();
app.use(express.json());

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
    out.email = trimmed;
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

app.get("/api/users", (_req: Request, res: Response) => {
  const list = Array.from(users.values());
  res.json({ users: list, count: list.length });
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

export { app, users };

if (require.main === module) {
  const port = process.env.USERMGMT_PORT || 8003;
  app.listen(port, () => {
    log("INFO", `Starting user management service on port ${port}`);
  });
}
