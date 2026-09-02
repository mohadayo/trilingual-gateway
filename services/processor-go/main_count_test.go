package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// このファイルは /api/messages/count の回帰テストを集める。
// count エンドポイントは `/api/stats` の軽量版として `total` / `distinct_channels` /
// `by_channel` の 3 フィールドのみを返す。analytics-py の `/api/events/count` と
// usermgmt-ts の `/api/users/count` に対称的な処理を processor-go にも揃える。
//
// フィルタセマンティクスは他エンドポイントと同一（parseMessageFilters に集約）で、
// channel / q / since / until の 4 種を受け付ける。以下では 200 レスポンスの構造・
// フィルタリング挙動・400/405 エラーパスを網羅的に検証する。

// seedCountMessages は count エンドポイント向けに 4 件の固定データを注入する。
// alerts × 2 (異なる時刻)、orders × 1、metrics × 1 の構成で
// distinct_channels=3, total=4 のシンプルなケースを提供し、
// channel / q / since / until のそれぞれのフィルタで想定件数が変化することを検証しやすくする。
func seedCountMessages(t *testing.T) {
	t.Helper()
	resetMessages()
	mu.Lock()
	messages = []Message{
		{ID: "id-1", Channel: "alerts", Payload: "cpu-high", CreatedAt: time.Date(2030, 1, 1, 8, 0, 0, 0, time.UTC)},
		{ID: "id-2", Channel: "alerts", Payload: "disk-full", CreatedAt: time.Date(2030, 1, 2, 8, 0, 0, 0, time.UTC)},
		{ID: "id-3", Channel: "orders", Payload: "new-order", CreatedAt: time.Date(2030, 1, 3, 8, 0, 0, 0, time.UTC)},
		{ID: "id-4", Channel: "metrics", Payload: "latency-report", CreatedAt: time.Date(2030, 1, 4, 8, 0, 0, 0, time.UTC)},
	}
	mu.Unlock()
}

func TestMessagesCount_EmptyStoreReturnsZero(t *testing.T) {
	resetMessages()
	req := httptest.NewRequest(http.MethodGet, "/api/messages/count", nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected application/json, got %q", got)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if total, _ := resp["total"].(float64); total != 0 {
		t.Errorf("expected total=0, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_channels"].(float64); distinct != 0 {
		t.Errorf("expected distinct_channels=0, got %v", resp["distinct_channels"])
	}
	byChannel, ok := resp["by_channel"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected by_channel object, got %T", resp["by_channel"])
	}
	if len(byChannel) != 0 {
		t.Errorf("expected empty by_channel map, got %v", byChannel)
	}
}

func TestMessagesCount_NoFiltersReturnsAll(t *testing.T) {
	seedCountMessages(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/count", nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 4 {
		t.Errorf("expected total=4, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_channels"].(float64); distinct != 3 {
		t.Errorf("expected distinct_channels=3, got %v", resp["distinct_channels"])
	}
	byChannel, _ := resp["by_channel"].(map[string]interface{})
	if len(byChannel) != 3 {
		t.Fatalf("expected 3 channels in by_channel, got %d: %v", len(byChannel), byChannel)
	}
	if v, _ := byChannel["alerts"].(float64); v != 2 {
		t.Errorf("expected alerts=2, got %v", byChannel["alerts"])
	}
	if v, _ := byChannel["orders"].(float64); v != 1 {
		t.Errorf("expected orders=1, got %v", byChannel["orders"])
	}
	if v, _ := byChannel["metrics"].(float64); v != 1 {
		t.Errorf("expected metrics=1, got %v", byChannel["metrics"])
	}
}

func TestMessagesCount_FiltersByChannel(t *testing.T) {
	seedCountMessages(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/count?channel=alerts", nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 2 {
		t.Errorf("expected total=2, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_channels"].(float64); distinct != 1 {
		t.Errorf("expected distinct_channels=1, got %v", resp["distinct_channels"])
	}
	byChannel, _ := resp["by_channel"].(map[string]interface{})
	if len(byChannel) != 1 {
		t.Fatalf("expected 1 channel in by_channel, got %d: %v", len(byChannel), byChannel)
	}
	if v, _ := byChannel["alerts"].(float64); v != 2 {
		t.Errorf("expected alerts=2 under filter, got %v", byChannel["alerts"])
	}
}

func TestMessagesCount_FiltersByQCaseInsensitive(t *testing.T) {
	seedCountMessages(t)
	// "REPORT" (大文字) は payload "latency-report" (id-4) にヒットし、
	// 大文字小文字を無視する既存の parseMessageFilters/normalizeSearchQuery 経由で
	// distinct=1, total=1 の結果になる。
	req := httptest.NewRequest(http.MethodGet, "/api/messages/count?q=REPORT", nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 1 {
		t.Errorf("expected total=1, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_channels"].(float64); distinct != 1 {
		t.Errorf("expected distinct_channels=1, got %v", resp["distinct_channels"])
	}
	byChannel, _ := resp["by_channel"].(map[string]interface{})
	if v, _ := byChannel["metrics"].(float64); v != 1 {
		t.Errorf("expected metrics=1 under q filter, got %v", byChannel["metrics"])
	}
}

func TestMessagesCount_FiltersBySinceUntil(t *testing.T) {
	seedCountMessages(t)
	// 2030-01-02 〜 2030-01-03 の 2 件 (id-2 alerts / id-3 orders) を切り出す
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/messages/count?since=2030-01-02T00:00:00Z&until=2030-01-03T23:59:59Z",
		nil,
	)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 2 {
		t.Errorf("expected total=2, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_channels"].(float64); distinct != 2 {
		t.Errorf("expected distinct_channels=2, got %v", resp["distinct_channels"])
	}
	byChannel, _ := resp["by_channel"].(map[string]interface{})
	if v, _ := byChannel["alerts"].(float64); v != 1 {
		t.Errorf("expected alerts=1 in window, got %v", byChannel["alerts"])
	}
	if v, _ := byChannel["orders"].(float64); v != 1 {
		t.Errorf("expected orders=1 in window, got %v", byChannel["orders"])
	}
	if _, present := byChannel["metrics"]; present {
		t.Errorf("expected metrics absent (outside window), got %v", byChannel["metrics"])
	}
}

func TestMessagesCount_CombinedFiltersMatchNothing(t *testing.T) {
	seedCountMessages(t)
	// channel=orders と q=cpu は排他 (orders チャネルの payload に "cpu" は含まれない)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/count?channel=orders&q=cpu", nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 0 {
		t.Errorf("expected total=0, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_channels"].(float64); distinct != 0 {
		t.Errorf("expected distinct_channels=0, got %v", resp["distinct_channels"])
	}
	byChannel, ok := resp["by_channel"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected by_channel object, got %T", resp["by_channel"])
	}
	if len(byChannel) != 0 {
		t.Errorf("expected empty by_channel, got %v", byChannel)
	}
}

func TestMessagesCount_RejectsInvalidSince(t *testing.T) {
	seedCountMessages(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/count?since=not-a-date", nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if _, ok := resp["error"].(string); !ok {
		t.Errorf("expected error field in body, got %v", resp)
	}
}

func TestMessagesCount_RejectsSinceGreaterThanUntil(t *testing.T) {
	seedCountMessages(t)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/messages/count?since=2030-12-01T00:00:00Z&until=2030-01-01T00:00:00Z",
		nil,
	)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMessagesCount_RejectsQTooLong(t *testing.T) {
	seedCountMessages(t)
	// maxSearchLength は既定 100。101 文字を送って 400 になることを確認する。
	long := ""
	for i := 0; i < 101; i++ {
		long += "a"
	}
	req := httptest.NewRequest(http.MethodGet, "/api/messages/count?q="+long, nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for over-long q, got %d", w.Code)
	}
}

func TestMessagesCount_RejectsNonGet(t *testing.T) {
	seedCountMessages(t)
	req := httptest.NewRequest(http.MethodPost, "/api/messages/count", nil)
	w := httptest.NewRecorder()
	messagesCountHandler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["error"] != "method not allowed" {
		t.Errorf("expected error='method not allowed', got %v", resp["error"])
	}
}

// count エンドポイントは Go 1.22 の http.ServeMux 上で `GET /api/messages/count` を
// リテラルとして登録し、`GET /api/messages/{id}` ワイルドカードよりも優先させる必要がある
// (`channels` / `by_day` 等と同じルーティング契約)。middleware_test.go の buildTestMux 経由で
// ルータ全体を通し、`count` が {id} として 404 に解決されないことを回帰保証する。
func TestMessagesCount_DoesNotCollideWithIDRoute(t *testing.T) {
	silenceLogger(t)
	seedCountMessages(t)
	h := buildTestMux()

	req := httptest.NewRequest(http.MethodGet, "/api/messages/count", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 via mux, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 4 {
		t.Errorf("expected total=4 via mux, got %v", resp["total"])
	}
	// ID ルート側は本物の ID を投げれば 200 (=count 経由で 404 にならないこと) を確認
	req2 := httptest.NewRequest(http.MethodGet, "/api/messages/id-1", nil)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /api/messages/id-1 via mux, got %d", w2.Code)
	}
}
