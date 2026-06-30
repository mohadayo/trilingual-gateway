import request from "supertest";
import { app, users } from "./app";

// `/api/users/by_hour_of_day` の境界・回帰テスト。
// 既存 `by_day_of_week` テスト群と対称な構造で、巨大な app.test.ts に
// 追記せず別ファイルに切り出して push 単位を小さく保つ。jest は
// `testMatch: ["**/*.test.ts"]` で自動的に拾う。
beforeEach(() => {
  users.clear();
});

describe("GET /api/users/by_hour_of_day", () => {
  // 既存 `by_day_of_week` テスト群と同じ seed 関数を持ち回す。POST だと
  // `created_at` が現在時刻になり時刻を制御できないため、`users.set` で
  // 直接挿入する。`by_day_of_week` と完全に対称な構造でテストを並べる。
  function seed(
    date: string,
    idx: number,
    role: "user" | "admin" | "moderator" = "user",
    extra?: { username?: string; email?: string },
  ): string {
    const id = `seed-hour-${idx}-${Date.now()}`;
    users.set(id, {
      id,
      username: extra?.username ?? `user${idx}`,
      email: extra?.email ?? `user${idx}@example.com`,
      role,
      created_at: date,
      updated_at: date,
    });
    return id;
  }

  it("returns empty aggregation on empty store", async () => {
    const res = await request(app).get("/api/users/by_hour_of_day");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(0);
    expect(res.body.distinct_hours).toBe(0);
    expect(res.body.by_hour_of_day).toEqual([]);
  });

  it("aggregates by UTC hour with 2-digit zero-padded keys", async () => {
    // UTC で 09:30 = "09", 09:45 = "09", 14:00 = "14", 23:59 = "23"
    seed("2026-01-05T09:30:00Z", 1);
    seed("2026-01-05T09:45:00Z", 2);
    seed("2026-01-05T14:00:00Z", 3);
    seed("2026-01-05T23:59:00Z", 4);

    const res = await request(app).get("/api/users/by_hour_of_day");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(4);
    expect(res.body.distinct_hours).toBe(3);
    expect(res.body.by_hour_of_day).toEqual([
      { hour: "09", count: 2 },
      { hour: "14", count: 1 },
      { hour: "23", count: 1 },
    ]);
  });

  it("results are sorted in lexical (= hour) ascending order", async () => {
    // バラバラの挿入順でも結果は常に "00" → "23" の lex 昇順 = 時間順。
    seed("2026-01-05T23:00:00Z", 1);
    seed("2026-01-05T00:00:00Z", 2);
    seed("2026-01-05T12:00:00Z", 3);
    seed("2026-01-05T05:00:00Z", 4);

    const res = await request(app).get("/api/users/by_hour_of_day");
    expect(res.status).toBe(200);
    const keys = res.body.by_hour_of_day.map(
      (b: { hour: string }) => b.hour,
    );
    expect(keys).toEqual(["00", "05", "12", "23"]);
  });

  it("does not include buckets with zero count (populated-only)", async () => {
    // 1 件しか挿入しなければ、その時刻以外は配列に含まれない。
    seed("2026-01-05T10:00:00Z", 1);

    const res = await request(app).get("/api/users/by_hour_of_day");
    expect(res.status).toBe(200);
    expect(res.body.by_hour_of_day).toEqual([
      { hour: "10", count: 1 },
    ]);
    expect(res.body.distinct_hours).toBe(1);
  });

  it("filters by ?role=", async () => {
    seed("2026-01-05T09:00:00Z", 1, "admin");
    seed("2026-01-05T09:00:00Z", 2, "user");
    seed("2026-01-05T14:00:00Z", 3, "admin");

    const res = await request(app).get(
      "/api/users/by_hour_of_day?role=admin",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    expect(res.body.by_hour_of_day).toEqual([
      { hour: "09", count: 1 },
      { hour: "14", count: 1 },
    ]);
  });

  it("filters by ?q= (case-insensitive partial match)", async () => {
    seed("2026-01-05T09:00:00Z", 1, "user", { username: "alice", email: "alice@x.com" });
    seed("2026-01-05T09:00:00Z", 2, "user", { username: "bob", email: "bob@x.com" });
    seed("2026-01-05T14:00:00Z", 3, "user", { username: "alex", email: "alex@x.com" });

    const res = await request(app).get("/api/users/by_hour_of_day?q=al");
    expect(res.status).toBe(200);
    // alice (09) + alex (14) のみ
    expect(res.body.total).toBe(2);
    expect(res.body.by_hour_of_day).toEqual([
      { hour: "09", count: 1 },
      { hour: "14", count: 1 },
    ]);
  });

  it("filters by since/until range on created_at", async () => {
    seed("2026-01-05T09:00:00Z", 1); // hour=09
    seed("2026-01-12T10:00:00Z", 2); // hour=10
    seed("2026-01-19T11:00:00Z", 3); // hour=11

    // 2026-01-10〜2026-01-15 だけ → 1 件のみ (hour=10)
    const res = await request(app).get(
      "/api/users/by_hour_of_day?since=2026-01-10T00:00:00Z&until=2026-01-15T00:00:00Z",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_hour_of_day).toEqual([
      { hour: "10", count: 1 },
    ]);
  });

  it("returns 400 for invalid role", async () => {
    const res = await request(app).get(
      "/api/users/by_hour_of_day?role=superuser",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("role");
  });

  it("returns 400 for invalid since", async () => {
    const res = await request(app).get(
      "/api/users/by_hour_of_day?since=not-a-date",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("since");
  });

  it("returns 400 when since > until", async () => {
    const res = await request(app).get(
      "/api/users/by_hour_of_day?since=2026-02-01T00:00:00Z&until=2026-01-01T00:00:00Z",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("until");
  });

  it("does not collide with /api/users/:id (route order)", async () => {
    // ルートの登録順で /api/users/by_hour_of_day は /api/users/:id より先。
    // パスをこの文字列で呼んでも 404 にはならず、集計が返ること。
    const res = await request(app).get("/api/users/by_hour_of_day");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("by_hour_of_day");
    expect(res.body).not.toHaveProperty("error");
  });

  it("normalizes non-UTC timestamps via UTC conversion", async () => {
    // 入力 created_at が +09:00 オフセットで、現地時刻と UTC で時刻が変わるケース。
    // 2026-01-05 09:00:00 +09:00 → UTC では 2026-01-05 00:00:00 (hour=00)
    seed("2026-01-05T09:00:00+09:00", 1);
    const res = await request(app).get("/api/users/by_hour_of_day");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_hour_of_day).toEqual([
      { hour: "00", count: 1 },
    ]);
  });

  it("skips users with malformed created_at (safe fallback)", async () => {
    // 不正な created_at を持つユーザは集計から除外され、total が下がる。
    const id = `seed-broken-hour-${Date.now()}`;
    users.set(id, {
      id,
      username: "broken-hour",
      email: "broken-hour@example.com",
      role: "user",
      created_at: "not-a-valid-date",
      updated_at: "not-a-valid-date",
    });
    seed("2026-01-05T10:00:00Z", 99); // hour=10 - 集計対象

    const res = await request(app).get("/api/users/by_hour_of_day");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_hour_of_day).toEqual([
      { hour: "10", count: 1 },
    ]);
  });
});
