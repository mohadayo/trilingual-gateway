package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// このファイルは /api/messages/by_week と /api/messages/by_month の回帰テストと、
// 補助関数 isoWeekKey の単体テストを集める。
// by_day / by_hour_of_day / by_day_of_week のテスト群 (main_test.go) と同じ観点を
// 週次・月次にも展開し、フィルタ・405/400 系・UTC 正規化・境界のカバレッジを保つ。

// ---- /api/messages/by_week ----

// seedByWeek はハンドラの週次ビニングを検証しやすい固定データを注入する。
// 2030-01-01 (火) は ISO 週 2030-W01、2030-01-04 (金) も 2030-W01、
// 2030-01-08 (火) は 2030-W02、2030-01-15 (火) は 2030-W03 に落ちるため、
// distinct な週境界を跨いだ集計を単一データセットで確認できる。
// channel/payload は by_day 系と同じ命名で `q`/`channel` フィルタ検証を共有する。
func seedByWeek(t *testing.T) {
	t.Helper()
	resetMessages()
	mu.Lock()
	messages = []Message{
		{ID: "id-1", Channel: "alerts", Payload: "p1", CreatedAt: time.Date(2030, 1, 1, 8, 0, 0, 0, time.UTC)},
		{ID: "id-2", Channel: "alerts", Payload: "p2", CreatedAt: time.Date(2030, 1, 4, 23, 30, 0, 0, time.UTC)},
		{ID: "id-3", Channel: "orders", Payload: "p3", CreatedAt: time.Date(2030, 1, 8, 0, 5, 0, 0, time.UTC)},
		{ID: "id-4", Channel: "orders", Payload: "p4", CreatedAt: time.Date(2030, 1, 15, 12, 0, 0, 0, time.UTC)},
	}
	mu.Unlock()
}

func TestMessagesByWeek_EmptyStoreReturnsEmpty(t *testing.T) {
	resetMessages()
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_week", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if total, _ := resp["total"].(float64); total != 0 {
		t.Errorf("expected total=0, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_weeks"].(float64); distinct != 0 {
		t.Errorf("expected distinct_weeks=0, got %v", resp["distinct_weeks"])
	}
	byWeek, ok := resp["by_week"].([]interface{})
	if !ok {
		t.Fatalf("expected by_week array, got %T", resp["by_week"])
	}
	if len(byWeek) != 0 {
		t.Errorf("expected empty by_week, got %v", byWeek)
	}
}

func TestMessagesByWeek_BasicChronologicalOrder(t *testing.T) {
	seedByWeek(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_week", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 4 {
		t.Errorf("expected total=4, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_weeks"].(float64); distinct != 3 {
		t.Errorf("expected distinct_weeks=3, got %v", resp["distinct_weeks"])
	}
	byWeek, _ := resp["by_week"].([]interface{})
	if len(byWeek) != 3 {
		t.Fatalf("expected 3 buckets, got %d: %v", len(byWeek), byWeek)
	}
	want := []struct {
		week  string
		count float64
	}{
		{"2030-W01", 2},
		{"2030-W02", 1},
		{"2030-W03", 1},
	}
	for i, e := range want {
		bucket, _ := byWeek[i].(map[string]interface{})
		if bucket["week"] != e.week {
			t.Errorf("position %d: expected week=%s, got %v", i, e.week, bucket["week"])
		}
		if got, _ := bucket["count"].(float64); got != e.count {
			t.Errorf("position %d: expected count=%v, got %v", i, e.count, got)
		}
	}
}

func TestMessagesByWeek_FiltersByChannel(t *testing.T) {
	seedByWeek(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_week?channel=orders", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 2 {
		t.Errorf("expected total=2, got %v", resp["total"])
	}
	byWeek, _ := resp["by_week"].([]interface{})
	if len(byWeek) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(byWeek))
	}
	b0, _ := byWeek[0].(map[string]interface{})
	b1, _ := byWeek[1].(map[string]interface{})
	if b0["week"] != "2030-W02" || b1["week"] != "2030-W03" {
		t.Errorf("expected orders weeks [2030-W02, 2030-W03], got [%v, %v]", b0["week"], b1["week"])
	}
}

func TestMessagesByWeek_FiltersBySinceUntil(t *testing.T) {
	seedByWeek(t)
	// 2030-W02 のメッセージ (2030-01-08) だけを since/until で切り出す
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/messages/by_week?since=2030-01-08T00:00:00Z&until=2030-01-08T23:59:59Z",
		nil,
	)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 1 {
		t.Errorf("expected total=1, got %v", resp["total"])
	}
	byWeek, _ := resp["by_week"].([]interface{})
	if len(byWeek) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(byWeek))
	}
	b0, _ := byWeek[0].(map[string]interface{})
	if b0["week"] != "2030-W02" {
		t.Errorf("expected week=2030-W02, got %v", b0["week"])
	}
}

func TestMessagesByWeek_FiltersByQ(t *testing.T) {
	seedByWeek(t)
	// payload "p1" は 2030-01-01 (2030-W01) の 1 件のみ一致
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_week?q=p1", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 1 {
		t.Errorf("expected total=1, got %v", resp["total"])
	}
	byWeek, _ := resp["by_week"].([]interface{})
	if len(byWeek) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(byWeek))
	}
	b0, _ := byWeek[0].(map[string]interface{})
	if b0["week"] != "2030-W01" {
		t.Errorf("expected week=2030-W01, got %v", b0["week"])
	}
}

func TestMessagesByWeek_RejectsInvalidSince(t *testing.T) {
	seedByWeek(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_week?since=not-a-date", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMessagesByWeek_RejectsSinceGreaterThanUntil(t *testing.T) {
	seedByWeek(t)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/messages/by_week?since=2030-12-01T00:00:00Z&until=2030-01-01T00:00:00Z",
		nil,
	)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMessagesByWeek_RejectsNonGet(t *testing.T) {
	seedByWeek(t)
	req := httptest.NewRequest(http.MethodPost, "/api/messages/by_week", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// ISO 8601 週の年跨ぎ検証: 2030-12-30 (月曜) は ISO 週年 2031、週 01 に属するため
// "2031-W01" にビニングされる。カレンダー年 2030 のままだと "2030-W01" と衝突するため、
// isoWeekKey が ISO 週年 (ISOWeek() の第一戻り値) を使っていることの回帰確認になる。
func TestMessagesByWeek_ISOYearBoundaryUsesWeekYear(t *testing.T) {
	resetMessages()
	mu.Lock()
	messages = []Message{
		// 2030-01-01 は ISO 2030-W01
		{ID: "start", Channel: "c", Payload: "p", CreatedAt: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)},
		// 2030-12-30 (月曜) は ISO 2031-W01
		{ID: "end", Channel: "c", Payload: "p", CreatedAt: time.Date(2030, 12, 30, 0, 0, 0, 0, time.UTC)},
	}
	mu.Unlock()
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_week", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	byWeek, _ := resp["by_week"].([]interface{})
	if len(byWeek) != 2 {
		t.Fatalf("expected 2 buckets, got %d: %v", len(byWeek), byWeek)
	}
	b0, _ := byWeek[0].(map[string]interface{})
	b1, _ := byWeek[1].(map[string]interface{})
	if b0["week"] != "2030-W01" {
		t.Errorf("expected first bucket 2030-W01, got %v", b0["week"])
	}
	if b1["week"] != "2031-W01" {
		t.Errorf("expected second bucket 2031-W01 (ISO week year), got %v", b1["week"])
	}
}

func TestMessagesByWeek_NormalizesToUTCAcrossLocations(t *testing.T) {
	resetMessages()
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Skipf("Asia/Tokyo not available: %v", err)
	}
	// 同じ UTC 週 (2030-W01) に属する 2 件を異なる location で表現する。
	// UTC 2030-01-04T23:00:00Z == JST 2030-01-05T08:00:00+09:00, どちらも 2030-W01。
	mu.Lock()
	messages = []Message{
		{ID: "u1", Channel: "c", Payload: "p", CreatedAt: time.Date(2030, 1, 4, 23, 0, 0, 0, time.UTC)},
		{ID: "u2", Channel: "c", Payload: "p", CreatedAt: time.Date(2030, 1, 5, 8, 0, 0, 0, jst)},
	}
	mu.Unlock()
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_week", nil)
	w := httptest.NewRecorder()
	messagesByWeekHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	byWeek, _ := resp["by_week"].([]interface{})
	if len(byWeek) != 1 {
		t.Fatalf("expected 1 bucket (same UTC ISO week), got %d: %v", len(byWeek), byWeek)
	}
	b0, _ := byWeek[0].(map[string]interface{})
	if b0["week"] != "2030-W01" {
		t.Errorf("expected week=2030-W01, got %v", b0["week"])
	}
	if c, _ := b0["count"].(float64); c != 2 {
		t.Errorf("expected count=2, got %v", c)
	}
}

// ---- /api/messages/by_month ----

// seedByMonth は月境界を跨いだ 4 件を注入する。
// 2030-01-01 と 2030-01-31 は "2030-01"、2030-02-15 は "2030-02"、
// 2030-04-10 は "2030-04" に落ちる (2030-03 は空月として集計に現れない)。
func seedByMonth(t *testing.T) {
	t.Helper()
	resetMessages()
	mu.Lock()
	messages = []Message{
		{ID: "id-1", Channel: "alerts", Payload: "p1", CreatedAt: time.Date(2030, 1, 1, 8, 0, 0, 0, time.UTC)},
		{ID: "id-2", Channel: "alerts", Payload: "p2", CreatedAt: time.Date(2030, 1, 31, 23, 30, 0, 0, time.UTC)},
		{ID: "id-3", Channel: "orders", Payload: "p3", CreatedAt: time.Date(2030, 2, 15, 0, 5, 0, 0, time.UTC)},
		{ID: "id-4", Channel: "orders", Payload: "p4", CreatedAt: time.Date(2030, 4, 10, 12, 0, 0, 0, time.UTC)},
	}
	mu.Unlock()
}

func TestMessagesByMonth_EmptyStoreReturnsEmpty(t *testing.T) {
	resetMessages()
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_month", nil)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if total, _ := resp["total"].(float64); total != 0 {
		t.Errorf("expected total=0, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_months"].(float64); distinct != 0 {
		t.Errorf("expected distinct_months=0, got %v", resp["distinct_months"])
	}
	byMonth, ok := resp["by_month"].([]interface{})
	if !ok {
		t.Fatalf("expected by_month array, got %T", resp["by_month"])
	}
	if len(byMonth) != 0 {
		t.Errorf("expected empty by_month, got %v", byMonth)
	}
}

func TestMessagesByMonth_BasicChronologicalOrder(t *testing.T) {
	seedByMonth(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_month", nil)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 4 {
		t.Errorf("expected total=4, got %v", resp["total"])
	}
	if distinct, _ := resp["distinct_months"].(float64); distinct != 3 {
		t.Errorf("expected distinct_months=3, got %v", resp["distinct_months"])
	}
	byMonth, _ := resp["by_month"].([]interface{})
	if len(byMonth) != 3 {
		t.Fatalf("expected 3 buckets (empty months skipped), got %d: %v", len(byMonth), byMonth)
	}
	want := []struct {
		month string
		count float64
	}{
		{"2030-01", 2},
		{"2030-02", 1},
		{"2030-04", 1},
	}
	for i, e := range want {
		bucket, _ := byMonth[i].(map[string]interface{})
		if bucket["month"] != e.month {
			t.Errorf("position %d: expected month=%s, got %v", i, e.month, bucket["month"])
		}
		if got, _ := bucket["count"].(float64); got != e.count {
			t.Errorf("position %d: expected count=%v, got %v", i, e.count, got)
		}
	}
}

func TestMessagesByMonth_FiltersByChannel(t *testing.T) {
	seedByMonth(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_month?channel=orders", nil)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 2 {
		t.Errorf("expected total=2, got %v", resp["total"])
	}
	byMonth, _ := resp["by_month"].([]interface{})
	if len(byMonth) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(byMonth))
	}
	b0, _ := byMonth[0].(map[string]interface{})
	b1, _ := byMonth[1].(map[string]interface{})
	if b0["month"] != "2030-02" || b1["month"] != "2030-04" {
		t.Errorf("expected orders months [2030-02, 2030-04], got [%v, %v]", b0["month"], b1["month"])
	}
}

func TestMessagesByMonth_FiltersBySinceUntil(t *testing.T) {
	seedByMonth(t)
	// 2030-02 だけを切り出す
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/messages/by_month?since=2030-02-01T00:00:00Z&until=2030-02-28T23:59:59Z",
		nil,
	)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 1 {
		t.Errorf("expected total=1, got %v", resp["total"])
	}
	byMonth, _ := resp["by_month"].([]interface{})
	if len(byMonth) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(byMonth))
	}
	b0, _ := byMonth[0].(map[string]interface{})
	if b0["month"] != "2030-02" {
		t.Errorf("expected month=2030-02, got %v", b0["month"])
	}
}

func TestMessagesByMonth_FiltersByQ(t *testing.T) {
	seedByMonth(t)
	// payload "p1" は 2030-01-01 の 1 件のみ一致
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_month?q=p1", nil)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if total, _ := resp["total"].(float64); total != 1 {
		t.Errorf("expected total=1, got %v", resp["total"])
	}
	byMonth, _ := resp["by_month"].([]interface{})
	if len(byMonth) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(byMonth))
	}
	b0, _ := byMonth[0].(map[string]interface{})
	if b0["month"] != "2030-01" {
		t.Errorf("expected month=2030-01, got %v", b0["month"])
	}
}

func TestMessagesByMonth_RejectsInvalidSince(t *testing.T) {
	seedByMonth(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_month?since=not-a-date", nil)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMessagesByMonth_RejectsSinceGreaterThanUntil(t *testing.T) {
	seedByMonth(t)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/messages/by_month?since=2030-12-01T00:00:00Z&until=2030-01-01T00:00:00Z",
		nil,
	)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMessagesByMonth_RejectsNonGet(t *testing.T) {
	seedByMonth(t)
	req := httptest.NewRequest(http.MethodPost, "/api/messages/by_month", nil)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// 月境界の UTC 正規化: 同じ UTC 月 (2030-01) に属する 2 件を異なる location で
// 表現しても 1 バケットにまとまることを確認する。
// UTC 2030-01-31T20:00:00Z == JST 2030-02-01T05:00:00+09:00, どちらも UTC 上は 2030-01。
func TestMessagesByMonth_NormalizesToUTCAcrossLocations(t *testing.T) {
	resetMessages()
	jst, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Skipf("Asia/Tokyo not available: %v", err)
	}
	mu.Lock()
	messages = []Message{
		{ID: "u1", Channel: "c", Payload: "p", CreatedAt: time.Date(2030, 1, 31, 20, 0, 0, 0, time.UTC)},
		{ID: "u2", Channel: "c", Payload: "p", CreatedAt: time.Date(2030, 2, 1, 5, 0, 0, 0, jst)},
	}
	mu.Unlock()
	req := httptest.NewRequest(http.MethodGet, "/api/messages/by_month", nil)
	w := httptest.NewRecorder()
	messagesByMonthHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	byMonth, _ := resp["by_month"].([]interface{})
	if len(byMonth) != 1 {
		t.Fatalf("expected 1 bucket (same UTC month), got %d: %v", len(byMonth), byMonth)
	}
	b0, _ := byMonth[0].(map[string]interface{})
	if b0["month"] != "2030-01" {
		t.Errorf("expected month=2030-01, got %v", b0["month"])
	}
	if c, _ := b0["count"].(float64); c != 2 {
		t.Errorf("expected count=2, got %v", c)
	}
}

// isoWeekKey が UTC 化とゼロ埋め (W01, W02...) を正しく行うことのユニット確認。
func TestIsoWeekKeyFormat(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		want string
	}{
		{"first week of 2030", time.Date(2030, 1, 3, 0, 0, 0, 0, time.UTC), "2030-W01"},
		{"iso year rollover Dec 30 2030", time.Date(2030, 12, 30, 0, 0, 0, 0, time.UTC), "2031-W01"},
		{"single digit week padded", time.Date(2030, 2, 4, 0, 0, 0, 0, time.UTC), "2030-W06"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isoWeekKey(tc.t); got != tc.want {
				t.Errorf("isoWeekKey(%v) = %q, want %q", tc.t, got, tc.want)
			}
		})
	}
}
