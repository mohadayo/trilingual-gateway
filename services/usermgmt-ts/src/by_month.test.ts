import request from "supertest";
import { app, users } from "./app";

// `/api/users/by_month` の境界・回帰テスト。
// 既存 `by_day` / `by_hour_of_day` / `by_day_of_week` テスト群と対称な構造で、
// 巨大な app.test.ts に追記せず別ファイルに切り出して push 単位を小さく保つ。
// jest は `testMatch: ["**/*.test.ts"]` で自動的に拾う。
beforeEach(() => {
  users.clear();
});

describe("GET /api/users/by_month", () => {
  // 既存の `by_day` テスト群と同じ seed 関数を持ち回す。POST だと
  // `created_at` が現在時刻になり時刻を制御できないため、`users.set` で
  // 直接挿入する。他 3 種の集計テストと完全に対称な構造でテストを並べる。
  function seed(
    date: string,
    idx: number,
    role: "user" | "admin" | "moderator" = "user",
    extra?: { username?: string; email?: string },
  ): string {
    const id = `seed-month-${idx}-${Date.now()}`;
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
    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(0);
    expect(res.body.distinct_months).toBe(0);
    expect(res.body.by_month).toEqual([]);
  });

  it("aggregates by UTC calendar month with YYYY-MM keys", async () => {
    // 同一 UTC 月は同じバケットに集計される。異なる日付・時刻でも月が同じなら同じ key。
    seed("2026-01-01T00:00:00Z", 1);
    seed("2026-01-15T14:00:00Z", 2);
    seed("2026-01-31T23:59:00Z", 3);
    seed("2026-02-01T00:00:00Z", 4);
    seed("2026-03-01T00:00:00Z", 5);

    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(5);
    expect(res.body.distinct_months).toBe(3);
    expect(res.body.by_month).toEqual([
      { month: "2026-01", count: 3 },
      { month: "2026-02", count: 1 },
      { month: "2026-03", count: 1 },
    ]);
  });

  it("results are sorted in lexical (= calendar) ascending order", async () => {
    // バラバラの挿入順でも結果は常にカレンダー昇順 ("2026-01" < "2026-12")。
    // lex 順 = カレンダー順を保つのが `YYYY-MM` 形式の設計目的（既存 by_day と同じ）。
    seed("2026-12-01T00:00:00Z", 1);
    seed("2026-01-15T00:00:00Z", 2);
    seed("2026-06-10T00:00:00Z", 3);
    seed("2026-03-20T00:00:00Z", 4);

    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    const keys = res.body.by_month.map((b: { month: string }) => b.month);
    expect(keys).toEqual([
      "2026-01",
      "2026-03",
      "2026-06",
      "2026-12",
    ]);
  });

  it("does not include buckets with zero count (populated-only)", async () => {
    // 1 件しか挿入しなければ、その月以外は配列に含まれない。
    // by_day / by_hour_of_day / by_day_of_week と同じ populated-only 方針。
    seed("2026-01-15T10:00:00Z", 1);

    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    expect(res.body.by_month).toEqual([{ month: "2026-01", count: 1 }]);
    expect(res.body.distinct_months).toBe(1);
  });

  it("filters by ?role=", async () => {
    seed("2026-01-15T09:00:00Z", 1, "admin");
    seed("2026-01-20T09:00:00Z", 2, "user");
    seed("2026-02-05T14:00:00Z", 3, "admin");

    const res = await request(app).get("/api/users/by_month?role=admin");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    expect(res.body.by_month).toEqual([
      { month: "2026-01", count: 1 },
      { month: "2026-02", count: 1 },
    ]);
  });

  it("filters by ?q= (case-insensitive partial match)", async () => {
    seed("2026-01-05T09:00:00Z", 1, "user", { username: "alice", email: "alice@x.com" });
    seed("2026-01-10T09:00:00Z", 2, "user", { username: "bob", email: "bob@x.com" });
    seed("2026-02-15T14:00:00Z", 3, "user", { username: "alex", email: "alex@x.com" });

    const res = await request(app).get("/api/users/by_month?q=al");
    expect(res.status).toBe(200);
    // alice (2026-01) + alex (2026-02) のみ
    expect(res.body.total).toBe(2);
    expect(res.body.by_month).toEqual([
      { month: "2026-01", count: 1 },
      { month: "2026-02", count: 1 },
    ]);
  });

  it("filters by since/until range on created_at", async () => {
    seed("2026-01-05T09:00:00Z", 1);
    seed("2026-03-12T10:00:00Z", 2);
    seed("2026-06-19T11:00:00Z", 3);

    // 2026-02〜2026-05 だけ → 1 件のみ (2026-03)
    const res = await request(app).get(
      "/api/users/by_month?since=2026-02-01T00:00:00Z&until=2026-05-31T23:59:59Z",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_month).toEqual([{ month: "2026-03", count: 1 }]);
  });

  it("returns 400 for invalid role", async () => {
    const res = await request(app).get("/api/users/by_month?role=superuser");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("role");
  });

  it("returns 400 for invalid since", async () => {
    const res = await request(app).get("/api/users/by_month?since=not-a-date");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("since");
  });

  it("returns 400 when since > until", async () => {
    const res = await request(app).get(
      "/api/users/by_month?since=2026-02-01T00:00:00Z&until=2026-01-01T00:00:00Z",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("until");
  });

  it("does not collide with /api/users/:id (route order)", async () => {
    // ルートの登録順で /api/users/by_month は /api/users/:id より先。
    // パスをこの文字列で呼んでも 404 にはならず、集計が返ること。
    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("by_month");
    expect(res.body).not.toHaveProperty("error");
  });

  it("normalizes non-UTC timestamps via UTC conversion", async () => {
    // 入力 created_at が +09:00 で、現地月と UTC 月がまたぐケース。
    // 2026-02-01 08:30:00 +09:00 = UTC 2026-01-31 23:30:00 → key="2026-01"
    seed("2026-02-01T08:30:00+09:00", 1);
    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_month).toEqual([{ month: "2026-01", count: 1 }]);
  });

  it("skips users with malformed created_at (safe fallback)", async () => {
    // 不正な created_at を持つユーザは集計から除外され、total が下がる。
    const id = `seed-broken-month-${Date.now()}`;
    users.set(id, {
      id,
      username: "broken-month",
      email: "broken-month@example.com",
      role: "user",
      created_at: "not-a-valid-date",
      updated_at: "not-a-valid-date",
    });
    seed("2026-01-15T10:00:00Z", 99); // month=2026-01 - 集計対象

    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_month).toEqual([{ month: "2026-01", count: 1 }]);
  });

  it("aggregates within-year and cross-year buckets independently", async () => {
    // 同月・別年は別バケット（"2025-06" と "2026-06"）。
    // lex ソートで年→月順に並ぶことを担保する。
    seed("2025-06-15T10:00:00Z", 1);
    seed("2025-06-20T10:00:00Z", 2);
    seed("2026-06-01T10:00:00Z", 3);
    seed("2026-05-15T10:00:00Z", 4);

    const res = await request(app).get("/api/users/by_month");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(4);
    expect(res.body.distinct_months).toBe(3);
    expect(res.body.by_month).toEqual([
      { month: "2025-06", count: 2 },
      { month: "2026-05", count: 1 },
      { month: "2026-06", count: 1 },
    ]);
  });
});
