import request from "supertest";
import { app, users, extractEmailDomain } from "./app";

// usermgmt-ts の `GET /api/users/by_domain` の回帰テスト。
// `by_day_of_week.test.ts` / `by_hour_of_day.test.ts` と同じ fixture 規約で、
// バリデーション・フィルタ・ドメイン抽出・登録順衝突回避を網羅する。

beforeEach(() => {
  users.clear();
});

// テスト用: POST でユーザを作成して created_at を意図的な値に上書きするヘルパ。
// by_day / by_month 系と違って by_domain は created_at 依存性が薄いが、
// since/until フィルタの検証には created_at 制御が必要なので、直接 users.set する。
function seedUser(
  username: string,
  email: string,
  opts: { role?: string; createdAt?: string } = {},
): void {
  const created = opts.createdAt ?? new Date().toISOString();
  users.set(username, {
    id: username,
    username,
    email: email.toLowerCase(),
    role: opts.role ?? "user",
    created_at: created,
    updated_at: created,
  });
}

describe("extractEmailDomain (unit)", () => {
  it("returns lowercased domain for standard emails", () => {
    expect(extractEmailDomain("foo@Example.COM")).toBe("example.com");
    expect(extractEmailDomain("bar@sub.example.co.jp")).toBe(
      "sub.example.co.jp",
    );
  });

  it("returns null for malformed emails", () => {
    // by_domain の集計対象外になるパターンを網羅的に確認する。
    expect(extractEmailDomain("no-at-symbol")).toBeNull();
    expect(extractEmailDomain("trailing-at@")).toBeNull();
    expect(extractEmailDomain("only-whitespace-domain@   ")).toBeNull();
    expect(extractEmailDomain("")).toBeNull();
  });

  it("uses the last @ (correct for edge-case emails)", () => {
    // RFC 上は quoted local-part に @ を含める余地があるが、実装は最後の @ を
    // ドメイン区切りとして扱う（extractEmailDomain 内 lastIndexOf 依存）。
    expect(extractEmailDomain("weird@user@example.com")).toBe("example.com");
  });
});

describe("GET /api/users/by_domain", () => {
  it("returns empty aggregation when store is empty", async () => {
    const res = await request(app).get("/api/users/by_domain");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      total: 0,
      distinct_domains: 0,
      by_domain: [],
    });
  });

  it("groups users by lowercased email domain", async () => {
    seedUser("alice", "alice@example.com");
    seedUser("bob", "bob@example.com");
    seedUser("carol", "carol@other.example.com");
    seedUser("dave", "dave@EXAMPLE.COM"); // 大文字混じり — POST では小文字化されるが safety-net
    const res = await request(app).get("/api/users/by_domain");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(4);
    expect(res.body.distinct_domains).toBe(2);
    expect(res.body.by_domain).toEqual([
      { domain: "example.com", count: 3 },
      { domain: "other.example.com", count: 1 },
    ]);
  });

  it("returns domains in lex ascending order", async () => {
    seedUser("z1", "z@zzz.example");
    seedUser("a1", "a@aaa.example");
    seedUser("m1", "m@mmm.example");
    const res = await request(app).get("/api/users/by_domain");
    expect(res.status).toBe(200);
    const domains = res.body.by_domain.map(
      (row: { domain: string }) => row.domain,
    );
    // lex 昇順を検証（他 by_* と同じ規約）。
    const sorted = [...domains].sort();
    expect(domains).toEqual(sorted);
    expect(domains).toEqual(["aaa.example", "mmm.example", "zzz.example"]);
  });

  it("filters by role", async () => {
    seedUser("alice", "alice@example.com", { role: "user" });
    seedUser("bob", "bob@example.com", { role: "admin" });
    seedUser("carol", "carol@other.example.com", { role: "admin" });
    const res = await request(app).get("/api/users/by_domain?role=admin");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    expect(res.body.by_domain).toEqual([
      { domain: "example.com", count: 1 },
      { domain: "other.example.com", count: 1 },
    ]);
  });

  it("filters by q (matches username OR email substring, case-insensitive)", async () => {
    seedUser("alice", "alice@example.com");
    seedUser("bob", "bob@example.com");
    seedUser("carol", "carol@corp.example");
    const res = await request(app).get("/api/users/by_domain?q=CORP");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_domain).toEqual([
      { domain: "corp.example", count: 1 },
    ]);
  });

  it("filters by since/until on created_at", async () => {
    // 3 日連続で 1 件ずつ挿入し、真ん中 2 日だけを since/until で拾う。
    seedUser("day1", "u1@a.example", { createdAt: "2026-06-01T00:00:00Z" });
    seedUser("day2", "u2@b.example", { createdAt: "2026-06-02T00:00:00Z" });
    seedUser("day3", "u3@c.example", { createdAt: "2026-06-03T00:00:00Z" });
    seedUser("day4", "u4@d.example", { createdAt: "2026-06-04T00:00:00Z" });
    // `+00:00` は URL 上で `%2B00:00` にエンコード。
    const res = await request(app).get(
      "/api/users/by_domain" +
        "?since=2026-06-02T00:00:00%2B00:00&until=2026-06-03T23:59:59%2B00:00",
    );
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(2);
    const domains = res.body.by_domain.map(
      (row: { domain: string }) => row.domain,
    );
    expect(domains.sort()).toEqual(["b.example", "c.example"]);
  });

  it("returns 400 on invalid role", async () => {
    const res = await request(app).get("/api/users/by_domain?role=bogus");
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("role");
  });

  it("returns 400 on invalid since", async () => {
    const res = await request(app).get(
      "/api/users/by_domain?since=not-a-date",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("since");
  });

  it("returns 400 when since > until", async () => {
    const res = await request(app).get(
      "/api/users/by_domain" +
        "?since=2026-06-05T00:00:00Z&until=2026-06-01T00:00:00Z",
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("until");
  });

  it("returns 400 when q exceeds max search length", async () => {
    // MAX_SEARCH_LENGTH は 100 (既定) なので 101 文字で 400。
    const longQ = "a".repeat(101);
    const res = await request(app).get(
      `/api/users/by_domain?q=${encodeURIComponent(longQ)}`,
    );
    expect(res.status).toBe(400);
    expect(res.body.error).toContain("q");
  });

  it("does not collide with the /:id lookup route", async () => {
    // "/api/users/by_domain" は :id ルートより前に登録されているため、
    // by_domain handler にマッチし、404 (user not found) には落ちない。
    const res = await request(app).get("/api/users/by_domain");
    expect(res.status).toBe(200);
    expect(res.body).toHaveProperty("by_domain");
    expect(res.body).not.toHaveProperty("error");
  });

  it("skips users whose email cannot be parsed (safety-net)", async () => {
    // バリデーションを通ったユーザには通常 @ が含まれるが、破損レコード
    // (extractEmailDomain が null を返すケース) を直接 users.set で入れて
    // 集計対象外になることを確認する。
    seedUser("ok", "ok@example.com");
    // 直接 users を破損データで書く（seedUser は toLowerCase 経由なので分岐しやすい）。
    users.set("broken", {
      id: "broken",
      username: "broken",
      email: "no-at-symbol",
      role: "user",
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });
    const res = await request(app).get("/api/users/by_domain");
    expect(res.status).toBe(200);
    expect(res.body.total).toBe(1);
    expect(res.body.by_domain).toEqual([
      { domain: "example.com", count: 1 },
    ]);
  });
});
