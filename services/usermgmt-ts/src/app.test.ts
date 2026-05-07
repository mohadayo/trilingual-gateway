import request from "supertest";
import { app, users } from "./app";

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
});

describe("GET /api/users", () => {
  it("returns all users", async () => {
    await request(app)
      .post("/api/users")
      .send({ username: "a", email: "a@example.com" });
    await request(app)
      .post("/api/users")
      .send({ username: "b", email: "b@example.com" });
    const res = await request(app).get("/api/users");
    expect(res.status).toBe(200);
    expect(res.body.count).toBe(2);
    expect(res.body.users).toHaveLength(2);
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
