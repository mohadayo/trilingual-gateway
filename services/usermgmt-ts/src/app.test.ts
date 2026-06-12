import request from "supertest";
import { app, users, MAX_REQUEST_BODY } from "./app";

beforeEach(() => {
  users.clear();
});

describe("GET /health", () => {
  it("returns ok status", async () => {
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body.status).toBe("ok");
    expect(res.body.service).toBe("usermgmt-ts");
    expect(res.body.timestamp).toBeDefined();
  });
});

describe("POST /api/users", () => {
  it("creates a new user", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "alice", email: "alice@example.com" });
    expect(res.status).toBe(201);
    expect(res.body.username).toBe("alice");
    expect(res.body.email).toBe("alice@example.com");
    expect(res.body.role).toBe("user");
    expect(res.body.id).toBeDefined();
    expect(res.body.updated_at).toBeDefined();
  });

  it("creates a user with custom role", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "bob", email: "bob@example.com", role: "admin" });
    expect(res.status).toBe(201);
    expect(res.body.role).toBe("admin");
  });

  it("rejects missing username", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ email: "no-name@example.com" });
    expect(res.status).toBe(400);
    expect(res.body.error).toBeDefined();
  });

  it("rejects missing email", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "nomail" });
    expect(res.status).toBe(400);
  });

  it("rejects duplicate email", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "u1", email: "dup@example.com" });
    const res = await request(app)
      .post("/api/users")
      .send({ username: "u2", email: "dup@example.com" });
    expect(res.status).toBe(409);
  });

  it("rejects blank username (whitespace only)", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "   ", email: "ws@example.com" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/blank/i);
  });

  it("rejects overlong username", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "x".repeat(51), email: "long@example.com" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/at most/i);
  });

  it("trims whitespace in username and email", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "  alice  ", email: "  alice@example.com  " });
    expect(res.status).toBe(201);
    expect(res.body.username).toBe("alice");
    expect(res.body.email).toBe("alice@example.com");
  });

  it("rejects invalid email format", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "not-an-email" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/email format/i);
  });

  it("rejects email without TLD", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "u@host" });
    expect(res.status).toBe(400);
  });

  it("rejects role outside allowlist", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "x@example.com", role: "superadmin" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/role must be one of/i);
  });

  it("accepts allowed roles", async () => {
    for (const role of ["user", "admin", "moderator"]) {
      const res = await request(app)
        .post("/api/users")
        .send({ username: `u-${role}`, email: `${role}@example.com`, role });
      expect(res.status).toBe(201);
      expect(res.body.role).toBe(role);
    }
  });

  it("rejects non-string username", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: 12345, email: "n@example.com" });
    expect(res.status).toBe(400);
  });

  it("rejects overlong email", async () => {
    const local = "x".repeat(250);
    const res = await request(app)
      .post("/api/users")
      .send({ username: "u", email: `${local}@e.co` });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/at most/i);
  });

  it("normalizes email to lowercase on create", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "case", email: "Case.User@Example.COM" });
    expect(res.status).toBe(201);
    expect(res.body.email).toBe("case.user@example.com");
  });

  it("rejects duplicate email differing only in case", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "first", email: "Same@Example.com" });
    const res = await request(app)
      .post("/api/users")
      .send({ username: "second", email: "SAME@example.COM" });
    expect(res.status).toBe(409);
  });
});

describe("GET /api/users", () => {
  it("returns all users with pagination metadata", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "a", email: "a@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "b", email: "b@example.com" });
    const res = await request(app).get("/api/users");
    expect(res.status).toBe(200);
    expect(res.body.count).toBe(2);
    expect(res.body.total).toBe(2);
    expect(res.body.limit).toBeGreaterThanOrEqual(2);
    expect(res.body.offset).toBe(0);
    expect(res.body.users).toHaveLength(2);
  });

  it("paginates with limit and offset", async () => {
    for (let i = 0; i < 5; i++) {
      await request(app)
        .post("/api/users")
        .send({ username: `u${i}`, email: `u${i}@example.com` });
    }
    const res = await request(app).get("/api/users?limit=2&offset=1");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(5);
    expect(res.body.limit).toBe(2);
    expect(res.body.offset).toBe(1);
    expect(res.body.users).toHaveLength(2);
  });

  it("filters by role", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "a", email: "a@example.com", role: "admin" });
    await request(app)
      .post("/api/users")
      .send({ username: "b", email: "b@example.com" });
    const res = await request(app).get("/api/users?role=admin");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.users[0].role).toBe("admin");
  });

  it("rejects invalid limit", async () => {
    const res = await request(app).get("/api/users?limit=0");
    expect(res.status).toBe(400);
  });

  it("rejects non-integer offset", async () => {
    const res = await request(app).get("/api/users?offset=abc");
    expect(res.status).toBe(400);
  });

  it("rejects unknown role", async () => {
    const res = await request(app).get("/api/users?role=ceo");
    expect(res.status).toBe(400);
  });

  it("rejects limit above the configured maximum", async () => {
    const res = await request(app).get("/api/users?limit=999999");
    expect(res.status).toBe(400);
  });
});

describe("GET /api/users/:id", () => {
  it("returns a specific user", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "c", email: "c@example.com" });
    const res = await request(app).get(`/api/users/${created.body.id}`);
    expect(res.status).toBe(200);
    expect(res.body.username).toBe("c");
  });

  it("returns 404 for unknown id", async () => {
    const res = await request(app).get("/api/users/nonexistent");
    expect(res.status).toBe(404);
  });
});

describe("PUT /api/users/:id", () => {
  it("updates username", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "old", email: "put@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ username: "new" });
    expect(res.status).toBe(200);
    expect(res.body.username).toBe("new");
    expect(res.body.email).toBe("put@example.com");
    expect(res.body.updated_at).not.toBe(created.body.updated_at);
  });

  it("updates email", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "old@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ email: "new@example.com" });
    expect(res.status).toBe(200);
    expect(res.body.email).toBe("new@example.com");
  });

  it("updates role", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "role@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ role: "admin" });
    expect(res.status).toBe(200);
    expect(res.body.role).toBe("admin");
  });

  it("rejects duplicate email on update", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "u1", email: "taken@example.com" });
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u2", email: "mine@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ email: "taken@example.com" });
    expect(res.status).toBe(409);
  });

  it("allows keeping the same email", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "same@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ email: "same@example.com", username: "updated" });
    expect(res.status).toBe(200);
    expect(res.body.username).toBe("updated");
  });

  it("rejects duplicate email on update even with different case", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "u1", email: "Owner@Example.com" });
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u2", email: "other@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ email: "OWNER@example.com" });
    expect(res.status).toBe(409);
  });

  it("normalizes email to lowercase on update", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "old@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ email: "New.Mixed@Example.COM" });
    expect(res.status).toBe(200);
    expect(res.body.email).toBe("new.mixed@example.com");
  });

  it("returns 404 for unknown id", async () => {
    const res = await request(app)
      .put("/api/users/nonexistent")
      .send({ username: "x" });
    expect(res.status).toBe(404);
  });

  it("rejects empty update body", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "empty@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({});
    expect(res.status).toBe(400);
  });

  it("rejects invalid email on update", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "valid@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ email: "broken" });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/email format/i);
  });

  it("rejects blank username on update", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "blank@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ username: "  " });
    expect(res.status).toBe(400);
  });

  it("rejects role outside allowlist on update", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u", email: "role@example.com" });
    const res = await request(app)
      .put(`/api/users/${created.body.id}`)
      .send({ role: "ROOT" });
    expect(res.status).toBe(400);
  });
});

describe("DELETE /api/users/:id", () => {
  it("deletes an existing user", async () => {
    const created = await request(app)
      .post("/api/users")
      .send({ username: "d", email: "d@example.com" });
    const res = await request(app).delete(`/api/users/${created.body.id}`);
    expect(res.status).toBe(204);

    const check = await request(app).get(`/api/users/${created.body.id}`);
    expect(check.status).toBe(404);
  });

  it("returns 404 for unknown id", async () => {
    const res = await request(app).delete("/api/users/nonexistent");
    expect(res.status).toBe(404);
  });
});

describe("GET /api/users search and sort", () => {
  async function seed(): Promise<void> {
    await request(app)
      .post("/api/users")
      .send({ username: "alice", email: "alice@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "bob", email: "bob@example.com", role: "admin" });
    await request(app)
      .post("/api/users")
      .send({ username: "charlie", email: "charlie@elsewhere.org" });
  }

  it("filters by case-insensitive substring on username", async () => {
    await seed();
    const res = await request(app).get("/api/users?q=BOB");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.users[0].username).toBe("bob");
  });

  it("filters by substring on email", async () => {
    await seed();
    const res = await request(app).get("/api/users?q=elsewhere");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.users[0].username).toBe("charlie");
  });

  it("matches against both username and email", async () => {
    await seed();
    const res = await request(app).get("/api/users?q=example.com");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
  });

  it("ignores blank q parameter", async () => {
    await seed();
    const res = await request(app).get("/api/users?q=%20%20");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(3);
  });

  it("rejects q exceeding max length", async () => {
    const res = await request(app).get(`/api/users?q=${"a".repeat(101)}`);
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("q must be at most");
  });

  it("returns sort and order in response", async () => {
    await seed();
    const res = await request(app).get("/api/users");
    expect(res.body.sort).toBe("created_at");
    expect(res.body.order).toBe("asc");
  });

  it("sorts by username asc", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "zoe", email: "zoe@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "amy", email: "amy@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "max", email: "max@example.com" });
    const res = await request(app).get("/api/users?sort=username");
    const names = res.body.users.map((u: { username: string }) => u.username);
    expect(names).toEqual(["amy", "max", "zoe"]);
  });

  it("sorts by username desc", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "zoe", email: "zoe@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "amy", email: "amy@example.com" });
    const res = await request(app).get("/api/users?sort=username&order=desc");
    const names = res.body.users.map((u: { username: string }) => u.username);
    expect(names).toEqual(["zoe", "amy"]);
  });

  it("sorts by email", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "u1", email: "z@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "u2", email: "a@example.com" });
    const res = await request(app).get("/api/users?sort=email");
    const emails = res.body.users.map((u: { email: string }) => u.email);
    expect(emails).toEqual(["a@example.com", "z@example.com"]);
  });

  it("rejects invalid sort field", async () => {
    const res = await request(app).get("/api/users?sort=bogus");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("sort must be one of");
  });

  it("rejects invalid order", async () => {
    const res = await request(app).get("/api/users?order=sideways");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("order must be one of");
  });

  it("combines role filter with q search", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "admin1", email: "admin1@x.com", role: "admin" });
    await request(app)
      .post("/api/users")
      .send({ username: "user1", email: "admin@y.com" });
    const res = await request(app).get("/api/users?role=admin&q=admin");
    expect(res.body.total).toBe(1);
    expect(res.body.users[0].username).toBe("admin1");
  });

  it("paginates sorted results", async () => {
    for (const name of ["alpha", "bravo", "charlie", "delta", "echo"]) {
      await request(app)
        .post("/api/users")
        .send({ username: name, email: `${name}@example.com` });
    }
    const res = await request(app).get(
      "/api/users?sort=username&limit=2&offset=1",
    );
    const names = res.body.users.map((u: { username: string }) => u.username);
    expect(names).toEqual(["bravo", "charlie"]);
    expect(res.body.total).toBe(5);
  });
});

describe("GET /api/users created_at range filter (since/until)", () => {
  // created_at は POST 時に new Date().toISOString() で設定されてしまうため、
  // 制御されたタイムスタンプでの絞り込みを検証するには users Map に直接挿入する。
  function seed(date: string, idx: number) {
    const id = `seed-${idx}-${Date.now()}`;
    users.set(id, {
      id,
      username: `user${idx}`,
      email: `user${idx}@example.com`,
      role: "user",
      created_at: date,
      updated_at: date,
    });
    return id;
  }

  it("filters by since", async () => {
    seed("2026-01-01T00:00:00Z", 1);
    seed("2026-05-01T00:00:00Z", 2);
    seed("2026-06-01T00:00:00Z", 3);
    const res = await request(app).get("/api/users?since=2026-04-01T00:00:00Z");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    const dates = res.body.users.map((u: { created_at: string }) => u.created_at);
    expect(dates).toEqual(
      expect.arrayContaining(["2026-05-01T00:00:00Z", "2026-06-01T00:00:00Z"]),
    );
  });

  it("filters by until", async () => {
    seed("2026-01-01T00:00:00Z", 1);
    seed("2026-05-01T00:00:00Z", 2);
    seed("2026-06-01T00:00:00Z", 3);
    const res = await request(app).get("/api/users?until=2026-05-01T00:00:00Z");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
  });

  it("filters by since and until combined", async () => {
    seed("2026-01-01T00:00:00Z", 1);
    seed("2026-03-15T00:00:00Z", 2);
    seed("2026-05-01T00:00:00Z", 3);
    seed("2026-06-01T00:00:00Z", 4);
    const res = await request(app).get(
      "/api/users?since=2026-03-01T00:00:00Z&until=2026-05-31T00:00:00Z",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
  });

  it("rejects since with invalid format", async () => {
    const res = await request(app).get("/api/users?since=not-a-date");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("since");
  });

  it("rejects until with invalid format", async () => {
    const res = await request(app).get("/api/users?until=tomorrow");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("until");
  });

  it("rejects since > until", async () => {
    const res = await request(app).get(
      "/api/users?since=2026-06-01T00:00:00Z&until=2026-05-01T00:00:00Z",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/until.*since/);
  });

  it("accepts Z and +00:00 forms interchangeably", async () => {
    seed("2026-05-15T00:00:00Z", 1);
    const resZ = await request(app).get("/api/users?since=2026-05-01T00:00:00Z");
    const resOffset = await request(app).get(
      "/api/users?since=2026-05-01T00:00:00%2B00:00",
    );
    expect(resZ.body.total).toBe(1);
    expect(resOffset.body.total).toBe(1);
  });

  it("returns full set when neither since nor until is provided", async () => {
    seed("2026-01-01T00:00:00Z", 1);
    seed("2026-06-01T00:00:00Z", 2);
    const res = await request(app).get("/api/users");
    expect(res.body.total).toBe(2);
  });
});

describe("JSON body size limit", () => {
  it("uses the documented default of 100kb when env var is not set", () => {
    // 既定値が明示されていることを回帰検証する。
    expect(MAX_REQUEST_BODY).toBe("100kb");
  });

  it("returns 413 when POST body exceeds the configured limit", async () => {
    // 既定 100kb を確実に超える 200KB の username で送る。
    const huge = "a".repeat(200 * 1024);
    const res = await request(app)
      .post("/api/users")
      .set("Content-Type", "application/json")
      .send({ username: huge, email: "huge@example.com" });
    expect(res.status).toBe(413);
    expect(res.body.error).toBe("request body too large");
  });

  it("accepts a normal-sized POST", async () => {
    const res = await request(app)
      .post("/api/users")
      .send({ username: "small", email: "small@example.com" });
    expect(res.status).toBe(201);
  });
});

describe("GET /api/users/count", () => {
  it("returns zero totals for an empty store", async () => {
    const res = await request(app).get("/api/users/count");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(0);
    expect(res.body.by_role).toEqual({ user: 0, admin: 0, moderator: 0 });
  });

  it("counts users by role with all roles initialized to 0", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "u1", email: "u1@example.com", role: "user" });
    await request(app)
      .post("/api/users")
      .send({ username: "u2", email: "u2@example.com", role: "admin" });
    await request(app)
      .post("/api/users")
      .send({ username: "u3", email: "u3@example.com", role: "admin" });
    await request(app)
      .post("/api/users")
      .send({ username: "u4", email: "u4@example.com" }); // default role = "user"

    const res = await request(app).get("/api/users/count");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(4);
    expect(res.body.by_role.user).toBe(2);
    expect(res.body.by_role.admin).toBe(2);
    expect(res.body.by_role.moderator).toBe(0);
  });

  it("filters by ?role=", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "a", email: "a@example.com", role: "admin" });
    await request(app)
      .post("/api/users")
      .send({ username: "b", email: "b@example.com", role: "user" });
    const res = await request(app).get("/api/users/count?role=admin");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_role.admin).toBe(1);
    expect(res.body.by_role.user).toBe(0);
  });

  it("filters by ?q= (case-insensitive partial match on username or email)", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "alice", email: "alice@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "bob", email: "bob@example.com" });
    const res = await request(app).get("/api/users/count?q=ALI");
    expect(res.body.total).toBe(1);
  });

  it("rejects invalid role", async () => {
    const res = await request(app).get("/api/users/count?role=superuser");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("role must be one of");
  });

  it("rejects invalid since", async () => {
    const res = await request(app).get("/api/users/count?since=not-a-date");
    expect(res.status).toBe(400);
  });

  it("rejects since > until", async () => {
    const res = await request(app).get(
      "/api/users/count?since=2030-01-01T00:00:00Z&until=2020-01-01T00:00:00Z",
    );
    expect(res.status).toBe(400);
  });

  it("rejects q above MAX_SEARCH_LENGTH", async () => {
    const longQ = "x".repeat(1000);
    const res = await request(app).get(
      `/api/users/count?q=${encodeURIComponent(longQ)}`,
    );
    expect(res.status).toBe(400);
  });

  it("ignores limit/offset/sort/order parameters silently when within valid range", async () => {
    // count はレコードを返さないため limit/offset/sort/order は count 結果に
    // 影響しない。許容範囲内の値で渡された場合は 200 を返し、total は変わらない。
    await request(app)
      .post("/api/users")
      .send({ username: "a", email: "a@example.com" });
    const res = await request(app).get(
      "/api/users/count?limit=10&offset=0&sort=email&order=desc",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
  });

  it("does not collide with the /:id route", async () => {
    // ユーザ id が "count" になることは UUID v4 では起きないが、
    // ルート登録順で `count` が `:id` より前にあるため、`/count` は常に count を返す。
    const res = await request(app).get("/api/users/count");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("by_role");
    expect(res.body).not.toHaveProperty("error");
  });

  it("returns null oldest/newest on empty store", async () => {
    // 0 件のときは oldest_created_at / newest_created_at は null
    // （processor-go の oldest/newest と同じ「観測なし=null」セマンティクス）。
    const res = await request(app).get("/api/users/count");
    expect(res.status).toBe(200);
    expect(res.body.oldest_created_at).toBeNull();
    expect(res.body.newest_created_at).toBeNull();
  });

  it("returns same oldest/newest for a single user", async () => {
    // 1 件のみのときは oldest と newest が一致し、その値は user.created_at に等しい。
    const created = await request(app)
      .post("/api/users")
      .send({ username: "u1", email: "u1@example.com" });
    expect(created.status).toBe(201);
    const res = await request(app).get("/api/users/count");
    expect(res.body.total).toBe(1);
    expect(res.body.oldest_created_at).toBe(created.body.created_at);
    expect(res.body.newest_created_at).toBe(created.body.created_at);
  });

  it("tracks min and max created_at across multiple users", async () => {
    // 連続作成で created_at が時間順に進むことを利用し、
    // 最初の user が oldest、最後の user が newest になることを確認する。
    const first = await request(app)
      .post("/api/users")
      .send({ username: "first", email: "first@example.com" });
    // Date.now() の解像度はミリ秒単位。同一ミリ秒の衝突を避けるため微小待機。
    await new Promise((r) => setTimeout(r, 5));
    await request(app)
      .post("/api/users")
      .send({ username: "middle", email: "middle@example.com" });
    await new Promise((r) => setTimeout(r, 5));
    const last = await request(app)
      .post("/api/users")
      .send({ username: "last", email: "last@example.com" });

    const res = await request(app).get("/api/users/count");
    expect(res.body.total).toBe(3);
    expect(res.body.oldest_created_at).toBe(first.body.created_at);
    expect(res.body.newest_created_at).toBe(last.body.created_at);
  });

  it("recomputes oldest/newest after role filter", async () => {
    // role=admin で絞ったとき、user ロールのレコードは oldest/newest 計算に含めない。
    const adminA = await request(app)
      .post("/api/users")
      .send({ username: "a", email: "a@example.com", role: "admin" });
    await new Promise((r) => setTimeout(r, 5));
    await request(app)
      .post("/api/users")
      .send({ username: "regular", email: "regular@example.com", role: "user" });
    await new Promise((r) => setTimeout(r, 5));
    const adminB = await request(app)
      .post("/api/users")
      .send({ username: "b", email: "b@example.com", role: "admin" });

    const res = await request(app).get("/api/users/count?role=admin");
    expect(res.body.total).toBe(2);
    expect(res.body.oldest_created_at).toBe(adminA.body.created_at);
    expect(res.body.newest_created_at).toBe(adminB.body.created_at);
  });

  it("recomputes oldest/newest after since/until filter", async () => {
    // 範囲外のレコードが oldest/newest に漏れないこと。
    const earliest = await request(app)
      .post("/api/users")
      .send({ username: "earliest", email: "earliest@example.com" });
    await new Promise((r) => setTimeout(r, 5));
    const middle = await request(app)
      .post("/api/users")
      .send({ username: "middle", email: "middle@example.com" });
    await new Promise((r) => setTimeout(r, 5));
    const latest = await request(app)
      .post("/api/users")
      .send({ username: "latest", email: "latest@example.com" });

    // since=middle.created_at で絞ると middle と latest だけ残る
    const res = await request(app).get(
      `/api/users/count?since=${encodeURIComponent(middle.body.created_at)}`,
    );
    expect(res.body.total).toBe(2);
    expect(res.body.oldest_created_at).toBe(middle.body.created_at);
    expect(res.body.newest_created_at).toBe(latest.body.created_at);
    expect(res.body.oldest_created_at).not.toBe(earliest.body.created_at);
  });
});
