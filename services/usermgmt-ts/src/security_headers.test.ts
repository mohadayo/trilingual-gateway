import request from "supertest";
import { app } from "./app";

/**
 * usermgmt-ts の全応答に付与される最小限のセキュリティヘッダを回帰検証する。
 *
 * - `X-Content-Type-Options: nosniff` (MIME sniffing 抑止)
 * - `X-Frame-Options: DENY` (clickjacking 抑止)
 * - `Referrer-Policy: no-referrer` (Referrer 漏洩抑止)
 * - `X-Powered-By` は露出しない (Express の版数情報を隠す)
 *
 * `/health` (200) と存在しないパス (404) と 400 の 3 経路で同じ挙動を固定し、
 * 「特定ルートだけ抜ける」リグレッションを検出する。
 */
describe("Security response headers", () => {
  it("adds nosniff / DENY / no-referrer and omits x-powered-by on /health (200)", async () => {
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.headers["x-content-type-options"]).toBe("nosniff");
    expect(res.headers["x-frame-options"]).toBe("DENY");
    expect(res.headers["referrer-policy"]).toBe("no-referrer");
    expect(res.headers["x-powered-by"]).toBeUndefined();
  });

  it("adds the same headers to 404 responses", async () => {
    const res = await request(app).get(
      "/definitely-not-a-real-endpoint-please",
    );
    expect(res.status).toBe(404);
    expect(res.headers["x-content-type-options"]).toBe("nosniff");
    expect(res.headers["x-frame-options"]).toBe("DENY");
    expect(res.headers["referrer-policy"]).toBe("no-referrer");
    expect(res.headers["x-powered-by"]).toBeUndefined();
  });

  it("adds the same headers to a 400 (validation error) response", async () => {
    // POST /api/users は username/email 不足で 400 を返す想定。
    // エラー応答経路でもセキュリティヘッダが漏れないことを確認する。
    const res = await request(app).post("/api/users").send({});
    expect([400, 422]).toContain(res.status);
    expect(res.headers["x-content-type-options"]).toBe("nosniff");
    expect(res.headers["x-frame-options"]).toBe("DENY");
    expect(res.headers["referrer-policy"]).toBe("no-referrer");
    expect(res.headers["x-powered-by"]).toBeUndefined();
  });
});
