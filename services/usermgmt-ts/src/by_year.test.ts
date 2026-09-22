import request from "supertest";
import { app, users } from "./app";

// `/api/users/by_year` の境界・回帰テスト。
// 既存 `by_month` / `by_day` / `by_week` / `by_hour_of_day` / `by_day_of_week`
// テスト群と対称な構造で、巨大な app.test.ts に追記せず別ファイルに切り出して
// push 単位を小さく保つ。jest は `testMatch: ["**/*.test.ts"]` で自動的に拾う。
beforeEach(() => {
  users.clear();
});

describe("GET /api/users/by_year", () => {
  // 既存の `by_month` テスト群と同じ seed 関数を持ち回す。POST だと
  // `created_at` が現在時刻になり時刻を制御できないため、`users.set` で
  // 直接挿入する。他集計テストと完全に対称な構造でテストを並べる。
  function seed(
    date: string,
    idx: number,
    role: "user" | "admin" | "moderator" = "user",
    extra?: { username?: string; email?: string },
  ): string {
    const id = `seed-year-${idx}-${Date.now()}`;
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
    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(0);
    expect(res.body.distinct_years).toBe(0);
    expect(res.body.by_year).toEqual([]);
  });

  it("aggregates by UTC calendar year with YYYY keys", async () => {
    // 同一 UTC 年は同じバケットに集計される。異なる月・日・時刻でも年が同じなら同じ key。
    seed("2025-01-01T00:00:00Z", 1);
    seed("2025-06-15T14:00:00Z", 2);
    seed("2025-12-31T23:59:00Z", 3);
    seed("2026-01-01T00:00:00Z", 4);
    seed("2026-07-01T00:00:00Z", 5);

    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(5);
    expect(res.body.distinct_years).toBe(2);
    expect(res.body.by_year).toEqual([
      { year: "2025", count: 3 },
      { year: "2026", count: 2 },
    ]);
  });

  it("results are sorted in lexical (= calendar) ascending order", async () => {
    // バラバラの挿入順でも結果は常にカレンダー昇順 ("2024" < "2025" < "2026")。
    // lex 順 = カレンダー順を保つのが `YYYY` 形式の設計目的（既存 by_month と同じ）。
    seed("2028-01-01T00:00:00Z", 1);
    seed("2024-01-01T00:00:00Z", 2);
    seed("2026-06-15T00:00:00Z", 3);
    seed("2025-03-20T00:00:00Z", 4);

    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    const keys = res.body.by_year.map((b: { year: string }) => b.year);
    expect(keys).toEqual(["2024", "2025", "2026", "2028"]);
  });

  it("does not include buckets with zero count (populated-only)", async () => {
    // 1 件しか挿入しなければ、その年以外は配列に含まれない。
    // by_month / by_day と同じ populated-only 方針。連続年でなくても
    // 空年はスキップされる（"2025" 単独、"2026" は現れない）。
    seed("2025-06-15T10:00:00Z", 1);

    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    expect(res.body.by_year).toEqual([{ year: "2025", count: 1 }]);
    expect(res.body.distinct_years).toBe(1);
  });

  it("filters by ?role=", async () => {
    seed("2025-01-15T09:00:00Z", 1, "admin");
    seed("2025-06-20T09:00:00Z", 2, "user");
    seed("2026-02-05T14:00:00Z", 3, "admin");

    const res = await request(app).get("/api/users/by_year?role=admin");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    expect(res.body.by_year).toEqual([
      { year: "2025", count: 1 },
      { year: "2026", count: 1 },
    ]);
  });

  it("filters by ?q= (case-insensitive partial match)", async () => {
    seed("2025-01-05T09:00:00Z", 1, "user", { username: "alice", email: "alice@x.com" });
    seed("2025-11-10T09:00:00Z", 2, "user", { username: "bob", email: "bob@x.com" });
    seed("2026-02-15T14:00:00Z", 3, "user", { username: "alex", email: "alex@x.com" });

    const res = await request(app).get("/api/users/by_year?q=al");
    expect(res.status).toBe(200);
    // alice (2025) + alex (2026) のみ
    expect(res.body.total).toBe(2);
    expect(res.body.by_year).toEqual([
      { year: "2025", count: 1 },
      { year: "2026", count: 1 },
    ]);
  });

  it("filters by since/until range on created_at", async () => {
    seed("2024-06-01T00:00:00Z", 1);
    seed("2025-03-12T10:00:00Z", 2);
    seed("2026-11-19T11:00:00Z", 3);

    // 2025-01〜2025-12 だけ → 1 件のみ (2025-03)
    const res = await request(app).get(
      "/api/users/by_year?since=2025-01-01T00:00:00Z&until=2025-12-31T23:59:59Z",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_year).toEqual([{ year: "2025", count: 1 }]);
  });

  it("returns 400 for invalid role", async () => {
    const res = await request(app).get("/api/users/by_year?role=superuser");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("role");
  });

  it("returns 400 for invalid since", async () => {
    const res = await request(app).get("/api/users/by_year?since=not-a-date");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("since");
  });

  it("returns 400 when since > until", async () => {
    const res = await request(app).get(
      "/api/users/by_year?since=2026-01-01T00:00:00Z&until=2025-01-01T00:00:00Z",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("until");
  });

  it("does not collide with /api/users/:id (route order)", async () => {
    // ルートの登録順で /api/users/by_year は /api/users/:id より先。
    // パスをこの文字列で呼んでも 404 にはならず、集計が返ること。
    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("by_year");
    expect(res.body).not.toHaveProperty("error");
  });

  it("normalizes non-UTC timestamps via UTC conversion (year boundary)", async () => {
    // 入力 created_at が +09:00 で、現地年と UTC 年がまたぐケース。
    // 2026-01-01 08:30:00 +09:00 = UTC 2025-12-31 23:30:00 → key="2025"
    seed("2026-01-01T08:30:00+09:00", 1);
    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_year).toEqual([{ year: "2025", count: 1 }]);
  });

  it("skips users with malformed created_at (safe fallback)", async () => {
    // 不正な created_at を持つユーザは集計から除外され、total が下がる。
    const id = `seed-broken-year-${Date.now()}`;
    users.set(id, {
      id,
      username: "broken-year",
      email: "broken-year@example.com",
      role: "user",
      created_at: "not-a-valid-date",
      updated_at: "not-a-valid-date",
    });
    seed("2025-01-15T10:00:00Z", 99); // year=2025 - 集計対象

    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_year).toEqual([{ year: "2025", count: 1 }]);
  });

  it("aggregates decade-spanning data with lex sort intact", async () => {
    // 2019, 2020, 2021, 2029 → lex 昇順で "2019" < "2020" < "2021" < "2029"
    seed("2020-06-15T10:00:00Z", 1);
    seed("2019-06-15T10:00:00Z", 2);
    seed("2029-06-15T10:00:00Z", 3);
    seed("2021-06-15T10:00:00Z", 4);

    const res = await request(app).get("/api/users/by_year");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(4);
    expect(res.body.distinct_years).toBe(4);
    expect(res.body.by_year).toEqual([
      { year: "2025", count: 1 },
    ]);
  });
});
