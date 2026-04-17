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
}

const users: Map<string, User> = new Map();

const log = (level: string, message: string) => {
  const ts = new Date().toISOString();
  console.log(`${ts} [${level}] usermgmt-ts: ${message}`);
};

app.get("/health", (_req: Request, res: Response) => {
  res.json({
    status: "ok",
    service: "usermgmt-ts",
    timestamp: new Date().toISOString(),
  });
});

app.post("/api/users", (req: Request, res: Response) => {
  const { username, email, role } = req.body;

  if (!username || !email) {
    log("WARN", "Missing username or email in request");
    res.status(400).json({ error: "username and email are required" });
    return;
  }

  const existing = Array.from(users.values()).find((u) => u.email === email);
  if (existing) {
    log("WARN", `Duplicate email: ${email}`);
    res.status(409).json({ error: "email already exists" });
    return;
  }

  const user: User = {
    id: uuidv4(),
    username,
    email,
    role: role || "user",
    created_at: new Date().toISOString(),
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
