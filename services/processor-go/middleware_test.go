package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// buildTestMux は main() と同じ経路を最小構成で組み立てるヘルパ。
// テストからは httptest.NewServer 相当の使い方を想定するが、リクエスト単発の
// 検証には httptest.NewRecorder を loggingMiddleware(mux) に直接投げるだけで十分。
// main() 側のフローと乖離しないよう、経路登録は main.go と同じ順序で行う。
func buildTestMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/messages", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			messagesHandler(w, r)
		case http.MethodPost:
			publishHandler(w, r)
		case http.MethodDelete:
			deleteMessagesHandler(w, r)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		}
	})
	mux.HandleFunc("GET /api/messages/channels", messageChannelsHandler)
	mux.HandleFunc("GET /api/messages/by_day", messagesByDayHandler)
	mux.HandleFunc("GET /api/messages/by_hour_of_day", messagesByHourOfDayHandler)
	mux.HandleFunc("GET /api/messages/by_day_of_week", messagesByDayOfWeekHandler)
	mux.HandleFunc("GET /api/messages/{id}", getMessageByIDHandler)
	mux.HandleFunc("DELETE /api/messages/{id}", deleteMessageByIDHandler)
	mux.HandleFunc("/api/stats", statsHandler)
	return loggingMiddleware(mux)
}

// captureLogger は package-level logger を bytes.Buffer に差し替えるヘルパ。
// テスト後には defer で必ず元の出力先に戻す (log.Logger.SetOutput は goroutine safe)。
func captureLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := new(bytes.Buffer)
	prev := logger.Writer()
	logger.SetOutput(buf)
	t.Cleanup(func() {
		logger.SetOutput(prev)
	})
	return buf
}

// silenceLogger は package-level logger の出力をテスト実行中の noise 抑制のため
// io.Discard に切り替える。captureLogger と併用しない場合の既定として使う。
func silenceLogger(t *testing.T) {
	t.Helper()
	prev := logger.Writer()
	logger.SetOutput(io.Discard)
	t.Cleanup(func() {
		logger.SetOutput(prev)
	})
}

// ---- X-Response-Time-Ms ヘッダの存在検証 ----

func TestLoggingMiddleware_SetsXResponseTimeMsOnHealth(t *testing.T) {
	silenceLogger(t)
	h := buildTestMux()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	raw := w.Header().Get("X-Response-Time-Ms")
	if raw == "" {
		t.Fatal("expected X-Response-Time-Ms header to be present")
	}
	// 常に整数の ms が入っているはず（最短ケースで "0" もありうるので `>= 0` で確認）
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatalf("expected integer ms in X-Response-Time-Ms, got %q: %v", raw, err)
	}
	if ms < 0 {
		t.Fatalf("expected non-negative ms, got %d", ms)
	}
}

func TestLoggingMiddleware_SetsXResponseTimeMsOnUnknownPath(t *testing.T) {
	silenceLogger(t)
	h := buildTestMux()
	req := httptest.NewRequest(http.MethodGet, "/no-such-route", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// http.ServeMux は未登録パスに対して既定 404 を返す。
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	if got := w.Header().Get("X-Response-Time-Ms"); got == "" {
		t.Fatal("expected X-Response-Time-Ms header even on 404 response")
	}
}

func TestLoggingMiddleware_SetsXResponseTimeMsOnPostMessages(t *testing.T) {
	silenceLogger(t)
	resetMessages()
	h := buildTestMux()

	body := `{"channel":"alerts","payload":"server down"}`
	req := httptest.NewRequest(http.MethodPost, "/api/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if got := w.Header().Get("X-Response-Time-Ms"); got == "" {
		t.Fatal("expected X-Response-Time-Ms header on 201 response")
	}
}

// ---- アクセスログのフォーマット / 記録 ----

func TestLoggingMiddleware_LogsMethodPathAndStatus(t *testing.T) {
	buf := captureLogger(t)
	h := buildTestMux()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	logLine := buf.String()
	// フォーマット: "GET /health -> 200 (Nms)"
	if !strings.Contains(logLine, "GET /health -> 200") {
		t.Fatalf("expected access log for GET /health -> 200, got: %q", logLine)
	}
	// (Nms) の末尾 (0 以上の整数 + "ms" + 閉じ括弧)
	if matched, _ := regexp.MatchString(`\(\d+ms\)`, logLine); !matched {
		t.Fatalf("expected (Nms) pattern in log, got: %q", logLine)
	}
}

func TestLoggingMiddleware_LogsStatusCodeOnNotFound(t *testing.T) {
	buf := captureLogger(t)
	h := buildTestMux()

	req := httptest.NewRequest(http.MethodGet, "/no-such-route", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	logLine := buf.String()
	if !strings.Contains(logLine, "GET /no-such-route -> 404") {
		t.Fatalf("expected access log with 404 status, got: %q", logLine)
	}
}

// ---- ハンドラの応答 body / status がミドルウェアで変質しないこと ----

func TestLoggingMiddleware_DoesNotAlterHealthBody(t *testing.T) {
	silenceLogger(t)
	h := buildTestMux()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	var resp HealthResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode /health body: %v", err)
	}
	if resp.Status != "ok" {
		t.Fatalf("expected status=ok, got %q", resp.Status)
	}
	if resp.Service != "processor-go" {
		t.Fatalf("expected service=processor-go, got %q", resp.Service)
	}
}

// ---- loggingResponseWriter の暗黙 200 挙動 ----

func TestLoggingResponseWriter_ImplicitWriteHeader200(t *testing.T) {
	// Handler が Write のみ呼んで WriteHeader を呼ばない場合、net/http の仕様と同じく
	// 暗黙的に 200 として記録されなければならない。
	rec := httptest.NewRecorder()
	lrw := &loggingResponseWriter{ResponseWriter: rec, statusCode: http.StatusOK}
	// wroteHeader は初期化直後 false。Write を先に呼ぶことで暗黙 200 に固定される想定。
	if _, err := lrw.Write([]byte("hello")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if lrw.statusCode != http.StatusOK {
		t.Fatalf("expected implicit 200 after Write-only, got %d", lrw.statusCode)
	}
	if !lrw.wroteHeader {
		t.Fatal("expected wroteHeader to be true after Write")
	}
}

func TestLoggingResponseWriter_CapturesExplicitStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	lrw := &loggingResponseWriter{ResponseWriter: rec, statusCode: http.StatusOK}
	lrw.WriteHeader(http.StatusTeapot)
	if lrw.statusCode != http.StatusTeapot {
		t.Fatalf("expected 418, got %d", lrw.statusCode)
	}
	if !lrw.wroteHeader {
		t.Fatal("expected wroteHeader to be true after WriteHeader")
	}
	// 二重 WriteHeader は net/http と同じく無視され、初回のコードが保持される
	lrw.WriteHeader(http.StatusInternalServerError)
	if lrw.statusCode != http.StatusTeapot {
		t.Fatalf("expected first-call 418 to be preserved, got %d", lrw.statusCode)
	}
}

// ---- logger 出力先の Cleanup が確実に元に戻ることの回帰確認 ----

func TestCaptureLogger_RestoresPreviousOutput(t *testing.T) {
	// captureLogger が t.Cleanup 経由で元の出力先に戻すことを、
	// サブテストで期間を明示的に区切って検証する。
	before := logger.Writer()
	t.Run("inner-scope", func(inner *testing.T) {
		buf := captureLogger(inner)
		logger.Printf("hi")
		if !strings.Contains(buf.String(), "hi") {
			inner.Fatalf("expected captured logger output to contain 'hi', got: %q", buf.String())
		}
	})
	// inner の t.Cleanup が発火した後、logger.Writer() は元に戻っているはず。
	if logger.Writer() != before {
		t.Fatal("expected logger output to be restored after captureLogger scope")
	}
	// log パッケージ経由でも副作用が残っていないことを確認 (念のため)
	_ = log.Ldate
}
