import request from "supertest";
import { app, users, toIsoWeekString } from "./app";

// `/api/users/by_week` の境界・回帰テスト。
// 既存 `by_day` / `by_month` / `by_hour_of_day` / `by_day_of_week` テスト群と
// 対称な構造で、巨大な app.test.ts に追記せず別ファイルに切り出して push 単位を
// 小さく保つ。jest は `testMatch: ["**/*.test.ts"]` で自動的に拾う。
beforeEach(() => {
  users.clear();
});

describe("toIsoWeekString helper", () => {
  // ISO 週規則の代表例を単体で回帰する。ハンドラの by_week 結果はこの helper
  // に依存するため、helper 側で先にアルゴリズムの正しさを固定する。
  it("returns 2026-W01 for 2025-12-29 (Monday of W01)", () => {
    // 2025-12-29 は月曜。ISO 週規則では「その週の木曜 (2026-01-01) が属する
    // 2026 年の W01」を持つ月曜として、2026-W01 になる。
    // カレンダー上の年（2025）ではなく、ISO 週数ベース年（2026）が採用される。
    expect(toIsoWeekString(new Date("2025-12-29T00:00:00Z"))).toBe("2026-W01");
  });

  it("returns 2026-W02 for 2026-01-05 (Monday of W02)", () => {
    // 2026-01-05 は月曜だが、その週の木曜 (2026-01-08) が 2026 年に属するため
    // ISO 2026-W02 になる（W01 は 2025-12-29〜2026-01-04）。
    // 「カレンダー上の 1 月最初の月曜 = ISO W01 の初日」ではないことを回帰する。
    expect(toIsoWeekString(new Date("2026-01-05T00:00:00Z"))).toBe("2026-W02");
  });

  it("returns 2026-W01 for 2026-01-01 (Thursday of W01)", () => {
    // 2026-01-01 は木曜。ISO 週規則で「木曜が属する年」ルールにより 2026-W01。
    expect(toIsoWeekString(new Date("2026-01-01T00:00:00Z"))).toBe("2026-W01");
  });

  it("returns 2026-W53 for 2027-01-01 (Friday belongs to prior ISO year)", () => {
    // 2027-01-01 は金曜。その週の木曜は 2026-12-31 なので ISO 2026-W53。
    expect(toIsoWeekString(new Date("2027-01-01T00:00:00Z"))).toBe("2026-W53");
  });

  it("returns 2028-W01 for 2028-01-03 (Monday of W01)", () => {
    // 2028-01-03 は月曜。2028-W01 の初日。
    expect(toIsoWeekString(new Date("2028-01-03T00:00:00Z"))).toBe("2028-W01");
  });

  it("pads single-digit week numbers with zero", () => {
    // 2 桁ゼロ詰め: W01 〜 W09 は "W0N" 形式。
    // 2026-02-02 は月曜、2026-W06。
    expect(toIsoWeekString(new Date("2026-02-02T00:00:00Z"))).toBe("2026-W06");
  });
});

describe("GET /api/users/by_week", () => {
  // 既存の `by_day` / `by_month` テスト群と同じ seed 関数を持ち回す。POST だと
  // `created_at` が現在時刻になり時刻を制御できないため、`users.set` で
  // 直接挿入する。他 4 種の集計テストと対称な構造でテストを並べる。
  function seed(
    date: string,
    idx: number,
    role: "user" | "admin" | "moderator" = "user",
    extra?: { username?: string; email?: string },
  ): string {
    const id = `seed-week-${idx}-${Date.now()}`;
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
    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(0);
    expect(res.body.distinct_weeks).toBe(0);
    expect(res.body.by_week).toEqual([]);
  });

  it("aggregates by ISO week with YYYY-Www keys", async () => {
    // 2026-01-05 (月) 〜 2026-01-11 (日) が 2026-W02。
    // 2026-01-12 (月) 〜 2026-01-18 (日) が 2026-W03。
    seed("2026-01-05T00:00:00Z", 1);  // W02
    seed("2026-01-08T14:00:00Z", 2);  // W02
    seed("2026-01-11T23:59:00Z", 3);  // W02
    seed("2026-01-12T00:00:00Z", 4);  // W03
    seed("2026-01-19T00:00:00Z", 5);  // W04

    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(5);
    expect(res.body.distinct_weeks).toBe(3);
    expect(res.body.by_week).toEqual([
      { week: "2026-W02", count: 3 },
      { week: "2026-W03", count: 1 },
      { week: "2026-W04", count: 1 },
    ]);
  });

  it("results are sorted in lexical (= calendar) ascending order", async () => {
    // バラバラの挿入順でも結果は常にカレンダー週昇順 ("2026-W01" < "2026-W52")。
    seed("2026-12-14T00:00:00Z", 1);  // W51
    seed("2026-01-05T00:00:00Z", 2);  // W02
    seed("2026-06-15T00:00:00Z", 3);  // W25
    seed("2026-03-16T00:00:00Z", 4);  // W12

    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    const keys = res.body.by_week.map((b: { week: string }) => b.week);
    expect(keys).toEqual(sortedCopy(keys));
    // 具体的にも確認
    expect(keys).toEqual([
      "2026-W02",
      "2026-W12",
      "2026-W25",
      "2026-W51",
    ]);
  });

  it("does not include buckets with zero count (populated-only)", async () => {
    seed("2026-01-05T10:00:00Z", 1);

    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    expect(res.body.by_week).toEqual([{ week: "2026-W02", count: 1 }]);
    expect(res.body.distinct_weeks).toBe(1);
  });

  it("handles ISO year boundary (2027-01-01 belongs to 2026-W53)", async () => {
    // 2026-12-31 (木) と 2027-01-01 (金) は共に ISO 2026-W53 に属する
    // （その週の木曜が 2026-12-31 のため）。
    seed("2026-12-31T00:00:00Z", 1);
    seed("2027-01-01T00:00:00Z", 2);
    // 2027-01-04 (月) は ISO 2027-W01 の初日。
    seed("2027-01-04T00:00:00Z", 3);

    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    const counts: Record<string, number> = {};
    for (const b of res.body.by_week as Array<{ week: string; count: number }>) {
      counts[b.week] = b.count;
    }
    expect(counts["2026-W53"]).toBe(2);
    expect(counts["2027-W01"]).toBe(1);
  });

  it("filters by ?role=", async () => {
    seed("2026-01-05T09:00:00Z", 1, "admin");
    seed("2026-01-06T09:00:00Z", 2, "user");
    seed("2026-01-12T14:00:00Z", 3, "admin");

    const res = await request(app).get("/api/users/by_week?role=admin");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    expect(res.body.by_week).toEqual([
      { week: "2026-W02", count: 1 },
      { week: "2026-W03", count: 1 },
    ]);
  });

  it("filters by ?q= (case-insensitive partial match)", async () => {
    seed("2026-01-05T09:00:00Z", 1, "user", {
      username: "alice", email: "alice@x.com",
    });
    seed("2026-01-05T10:00:00Z", 2, "user", {
      username: "bob", email: "bob@x.com",
    });
    seed("2026-01-12T09:00:00Z", 3, "user", {
      username: "alex", email: "alex@x.com",
    });

    const res = await request(app).get("/api/users/by_week?q=al");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    expect(res.body.by_week).toEqual([
      { week: "2026-W02", count: 1 },
      { week: "2026-W03", count: 1 },
    ]);
  });

  it("filters by since/until range on created_at", async () => {
    seed("2026-01-05T09:00:00Z", 1);  // W02
    seed("2026-02-16T10:00:00Z", 2);  // W08
    seed("2026-06-15T11:00:00Z", 3);  // W25

    const res = await request(app).get(
      "/api/users/by_week?since=2026-02-01T00:00:00Z&until=2026-05-31T23:59:59Z",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_week).toEqual([{ week: "2026-W08", count: 1 }]);
  });

  it("returns 400 for invalid role", async () => {
    const res = await request(app).get("/api/users/by_week?role=superuser");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("role");
  });

  it("returns 400 for invalid since", async () => {
    const res = await request(app).get("/api/users/by_week?since=not-a-date");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("since");
  });

  it("returns 400 when since > until", async () => {
    const res = await request(app).get(
      "/api/users/by_week?since=2026-02-01T00:00:00Z&until=2026-01-01T00:00:00Z",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("until");
  });

  it("does not collide with /api/users/:id (route order)", async () => {
    // /api/users/by_week は /api/users/:id より前に登録されているため、
    // このパスは集計エンドポイントにマッチし、404 にはならない。
    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("by_week");
    expect(res.body).not.toHaveProperty("error");
  });

  it("normalizes non-UTC timestamps via UTC conversion", async () => {
    // 2026-01-12 08:30 +09:00 → UTC 2026-01-11 23:30 → 2026-W02 (日曜側)
    seed("2026-01-12T08:30:00+09:00", 1);
    // 2026-01-12 09:00 +00:00 → UTC 2026-01-12 09:00 → 2026-W03 (月曜側)
    seed("2026-01-12T09:00:00+00:00", 2);
    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    expect(res.body.by_week).toEqual([
      { week: "2026-W02", count: 1 },
      { week: "2026-W03", count: 1 },
    ]);
  });

  it("skips users with malformed created_at (safe fallback)", async () => {
    const id = `seed-broken-week-${Date.now()}`;
    users.set(id, {
      id,
      username: "broken-week",
      email: "broken-week@example.com",
      role: "user",
      created_at: "not-a-valid-date",
      updated_at: "not-a-valid-date",
    });
    seed("2026-01-05T10:00:00Z", 99);  // W02

    const res = await request(app).get("/api/users/by_week");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_week).toEqual([{ week: "2026-W02", count: 1 }]);
  });
});

function sortedCopy(arr: string[]): string[] {
  const copy = arr.slice();
  copy.sort();
  return copy;
}
