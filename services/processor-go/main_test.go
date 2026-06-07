package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func resetMessages() {
	mu.Lock()
	messages = nil
	mu.Unlock()
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp HealthResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "ok" {
		t.Fatalf("expected status ok, got %s", resp.Status)
	}
	if resp.Service != "processor-go" {
		t.Fatalf("expected service processor-go, got %s", resp.Service)
	}
}

func TestPublishAndListMessages(t *testing.T) {
	resetMessages()

	body := `{"channel":"alerts","payload":"server down"}`
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}

	var msg Message
	json.NewDecoder(w.Body).Decode(&msg)
	if msg.Channel != "alerts" {
		t.Fatalf("expected channel alerts, got %s", msg.Channel)
	}
	if !msg.Processed {
		t.Fatal("expected processed to be true")
	}

	req = httptest.NewRequest(http.MethodGet, "/api/messages", nil)
	w = httptest.NewRecorder()
	messagesHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var listResp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&listResp)
	if int(listResp["count"].(float64)) != 1 {
		t.Fatalf("expected 1 message, got %v", listResp["count"])
	}
}

func TestPublishMissingFields(t *testing.T) {
	body := `{"channel":"","payload":""}`
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestPublishInvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString("not json"))
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestFilterByChannel(t *testing.T) {
	resetMessages()

	for _, ch := range []string{"ch1", "ch2", "ch1"} {
		body, _ := json.Marshal(map[string]string{"channel": ch, "payload": "test"})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/messages?channel=ch1", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["count"].(float64)) != 2 {
		t.Fatalf("expected 2 messages for ch1, got %v", resp["count"])
	}
}

func TestStatsHandler(t *testing.T) {
	resetMessages()

	for _, ch := range []string{"a", "a", "b"} {
		body, _ := json.Marshal(map[string]string{"channel": ch, "payload": "data"})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["total_messages"].(float64)) != 3 {
		t.Fatalf("expected 3 total, got %v", resp["total_messages"])
	}
}

func TestPublishMethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestNewUUIDFormat(t *testing.T) {
	id := newUUID()
	uuidRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	if !uuidRegex.MatchString(id) {
		t.Fatalf("expected UUID v4 format, got %s", id)
	}
}

func TestNewUUIDUniqueness(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := newUUID()
		if ids[id] {
			t.Fatalf("duplicate UUID generated: %s", id)
		}
		ids[id] = true
	}
}

func TestPublishBodyTooLarge(t *testing.T) {
	resetMessages()

	largePayload := strings.Repeat("a", 128*1024)
	body := fmt.Sprintf(`{"channel":"test","payload":"%s"}`, largePayload)
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}

	var resp ErrorResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != "request body too large" {
		t.Errorf("expected 'request body too large', got %s", resp.Error)
	}
}

func TestStatsHandler_EmptyStore(t *testing.T) {
	resetMessages()

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["total_messages"].(float64)) != 0 {
		t.Fatalf("expected 0 total_messages, got %v", resp["total_messages"])
	}
}

func TestNewUUIDConcurrency(t *testing.T) {
	const goroutines = 50
	const idsPerGoroutine = 100
	ch := make(chan string, goroutines*idsPerGoroutine)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < idsPerGoroutine; j++ {
				ch <- newUUID()
			}
		}()
	}
	wg.Wait()
	close(ch)

	seen := make(map[string]bool)
	for id := range ch {
		if seen[id] {
			t.Fatalf("concurrent duplicate UUID: %s", id)
		}
		seen[id] = true
	}

	if len(seen) != goroutines*idsPerGoroutine {
		t.Fatalf("expected %d unique IDs, got %d", goroutines*idsPerGoroutine, len(seen))
	}
}

func TestMessageStoreMaxCapacity(t *testing.T) {
	resetMessages()
	oldMax := maxMessages
	maxMessages = 3
	defer func() { maxMessages = oldMax }()

	for i := 0; i < 5; i++ {
		body, _ := json.Marshal(map[string]string{
			"channel": fmt.Sprintf("ch-%d", i),
			"payload": "data",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d", w.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	count := int(resp["count"].(float64))
	if count != 3 {
		t.Fatalf("expected 3 messages (capped), got %d", count)
	}

	msgs := resp["messages"].([]interface{})
	first := msgs[0].(map[string]interface{})
	last := msgs[2].(map[string]interface{})
	if first["channel"] != "ch-2" {
		t.Errorf("expected oldest remaining to be ch-2, got %s", first["channel"])
	}
	if last["channel"] != "ch-4" {
		t.Errorf("expected newest to be ch-4, got %s", last["channel"])
	}
}

func TestMessageStoreWithinCapacity(t *testing.T) {
	resetMessages()
	oldMax := maxMessages
	maxMessages = 10
	defer func() { maxMessages = oldMax }()

	for i := 0; i < 3; i++ {
		body, _ := json.Marshal(map[string]string{
			"channel": fmt.Sprintf("ch-%d", i),
			"payload": "data",
		})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	count := int(resp["count"].(float64))
	if count != 3 {
		t.Fatalf("expected 3 messages, got %d", count)
	}
}

func seedMessages(t *testing.T, n int, channel string) {
	t.Helper()
	for i := 0; i < n; i++ {
		body, _ := json.Marshal(map[string]string{
			"channel": channel,
			"payload": fmt.Sprintf("msg-%d", i),
		})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("seed failed at %d: %d", i, w.Code)
		}
	}
}

func TestMessagesPaginationDefaults(t *testing.T) {
	resetMessages()
	seedMessages(t, 75, "p")

	req := httptest.NewRequest(http.MethodGet, "/api/messages", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if int(resp["total"].(float64)) != 75 {
		t.Fatalf("expected total 75, got %v", resp["total"])
	}
	if int(resp["limit"].(float64)) != 50 {
		t.Fatalf("expected default limit 50, got %v", resp["limit"])
	}
	if int(resp["offset"].(float64)) != 0 {
		t.Fatalf("expected default offset 0, got %v", resp["offset"])
	}
	if int(resp["count"].(float64)) != 50 {
		t.Fatalf("expected count 50, got %v", resp["count"])
	}
}

func TestMessagesPaginationOffset(t *testing.T) {
	resetMessages()
	seedMessages(t, 30, "p")

	req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=10&offset=20", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if int(resp["count"].(float64)) != 10 {
		t.Fatalf("expected count 10, got %v", resp["count"])
	}
	if int(resp["total"].(float64)) != 30 {
		t.Fatalf("expected total 30, got %v", resp["total"])
	}
	msgs := resp["messages"].([]interface{})
	first := msgs[0].(map[string]interface{})
	if first["payload"] != "msg-20" {
		t.Errorf("expected first payload msg-20, got %v", first["payload"])
	}
}

func TestMessagesPaginationOffsetBeyondTotal(t *testing.T) {
	resetMessages()
	seedMessages(t, 5, "p")

	req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=10&offset=100", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if int(resp["count"].(float64)) != 0 {
		t.Fatalf("expected count 0, got %v", resp["count"])
	}
	if int(resp["total"].(float64)) != 5 {
		t.Fatalf("expected total 5, got %v", resp["total"])
	}
	if msgs, ok := resp["messages"].([]interface{}); !ok || len(msgs) != 0 {
		t.Errorf("expected empty messages array, got %v", resp["messages"])
	}
}

func TestMessagesPaginationLimitClamped(t *testing.T) {
	resetMessages()
	oldMax := maxPageLimit
	maxPageLimit = 5
	defer func() { maxPageLimit = oldMax }()

	seedMessages(t, 10, "p")

	req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=999", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if int(resp["limit"].(float64)) != 5 {
		t.Fatalf("expected limit clamped to 5, got %v", resp["limit"])
	}
	if int(resp["count"].(float64)) != 5 {
		t.Fatalf("expected count 5, got %v", resp["count"])
	}
}

func TestMessagesPaginationNegativeAndInvalid(t *testing.T) {
	resetMessages()
	seedMessages(t, 3, "p")

	req := httptest.NewRequest(http.MethodGet, "/api/messages?limit=-5&offset=-2", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if int(resp["limit"].(float64)) != defaultPageLimit {
		t.Fatalf("expected default limit, got %v", resp["limit"])
	}
	if int(resp["offset"].(float64)) != 0 {
		t.Fatalf("expected offset 0, got %v", resp["offset"])
	}
}

func TestMessagesPaginationWithChannelFilter(t *testing.T) {
	resetMessages()
	seedMessages(t, 10, "ch-a")
	seedMessages(t, 5, "ch-b")

	req := httptest.NewRequest(http.MethodGet, "/api/messages?channel=ch-a&limit=3&offset=2", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)

	if int(resp["total"].(float64)) != 10 {
		t.Fatalf("expected filtered total 10, got %v", resp["total"])
	}
	if int(resp["count"].(float64)) != 3 {
		t.Fatalf("expected count 3, got %v", resp["count"])
	}
}

func TestMessagesSinceUntilFilter(t *testing.T) {
	resetMessages()

	now := time.Now().UTC()
	mu.Lock()
	messages = []Message{
		{ID: "id-old", Channel: "p", Payload: "old", Processed: true, CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "id-mid", Channel: "p", Payload: "mid", Processed: true, CreatedAt: now.Add(-1 * time.Hour)},
		{ID: "id-new", Channel: "p", Payload: "new", Processed: true, CreatedAt: now},
	}
	mu.Unlock()

	since := now.Add(-90 * time.Minute).Format(time.RFC3339Nano)
	until := now.Add(-30 * time.Minute).Format(time.RFC3339Nano)
	url := fmt.Sprintf("/api/messages?since=%s&until=%s", since, until)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["total"].(float64)) != 1 {
		t.Fatalf("expected total 1, got %v", resp["total"])
	}
	msgs := resp["messages"].([]interface{})
	first := msgs[0].(map[string]interface{})
	if first["id"] != "id-mid" {
		t.Fatalf("expected id-mid, got %v", first["id"])
	}
}

func TestMessagesInvalidSinceUntil(t *testing.T) {
	resetMessages()
	cases := []struct {
		name string
		q    string
	}{
		{"non-iso since", "/api/messages?since=abc"},
		{"non-iso until", "/api/messages?until=xyz"},
		{"until less than since", "/api/messages?since=2026-01-02T00:00:00Z&until=2026-01-01T00:00:00Z"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, tc.q, nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d (%s)", tc.name, w.Code, w.Body.String())
		}
	}
}

func TestMessagesSortByCreatedAtDesc(t *testing.T) {
	resetMessages()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mu.Lock()
	messages = []Message{
		{ID: "a", Channel: "p", Payload: "1", Processed: true, CreatedAt: base},
		{ID: "b", Channel: "p", Payload: "2", Processed: true, CreatedAt: base.Add(2 * time.Hour)},
		{ID: "c", Channel: "p", Payload: "3", Processed: true, CreatedAt: base.Add(1 * time.Hour)},
	}
	mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?sort=created_at&order=desc", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if resp["sort"] != "created_at" || resp["order"] != "desc" {
		t.Errorf("expected sort=created_at order=desc, got sort=%v order=%v", resp["sort"], resp["order"])
	}
	msgs := resp["messages"].([]interface{})
	if msgs[0].(map[string]interface{})["id"] != "b" ||
		msgs[1].(map[string]interface{})["id"] != "c" ||
		msgs[2].(map[string]interface{})["id"] != "a" {
		t.Errorf("unexpected order: %v, %v, %v",
			msgs[0].(map[string]interface{})["id"],
			msgs[1].(map[string]interface{})["id"],
			msgs[2].(map[string]interface{})["id"])
	}
}

func TestMessagesSortByChannelAsc(t *testing.T) {
	resetMessages()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mu.Lock()
	messages = []Message{
		{ID: "1", Channel: "charlie", Payload: "x", Processed: true, CreatedAt: base},
		{ID: "2", Channel: "alpha", Payload: "y", Processed: true, CreatedAt: base},
		{ID: "3", Channel: "bravo", Payload: "z", Processed: true, CreatedAt: base},
	}
	mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/api/messages?sort=channel&order=asc", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)

	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	msgs := resp["messages"].([]interface{})
	if msgs[0].(map[string]interface{})["channel"] != "alpha" ||
		msgs[1].(map[string]interface{})["channel"] != "bravo" ||
		msgs[2].(map[string]interface{})["channel"] != "charlie" {
		t.Errorf("unexpected order")
	}
}

func TestMessagesInvalidSort(t *testing.T) {
	resetMessages()
	req := httptest.NewRequest(http.MethodGet, "/api/messages?sort=bogus", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMessagesInvalidOrder(t *testing.T) {
	resetMessages()
	req := httptest.NewRequest(http.MethodGet, "/api/messages?order=sideways", nil)
	w := httptest.NewRecorder()
	messagesHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestPublishWhitespaceOnlyRejected(t *testing.T) {
	resetMessages()
	cases := []struct {
		name string
		body string
	}{
		{"blank channel", `{"channel":"   ","payload":"hello"}`},
		{"blank payload", `{"channel":"alerts","payload":"   "}`},
		{"both blank", `{"channel":" ","payload":"\t"}`},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(tc.body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		publishHandler(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d (%s)", tc.name, w.Code, w.Body.String())
		}
	}
}

func TestPublishTrimsChannelAndPayload(t *testing.T) {
	resetMessages()

	body := `{"channel":"  alerts  ","payload":"  hi  "}`
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", w.Code, w.Body.String())
	}
	var msg Message
	json.NewDecoder(w.Body).Decode(&msg)
	if msg.Channel != "alerts" {
		t.Errorf("expected trimmed channel 'alerts', got %q", msg.Channel)
	}
	if msg.Payload != "hi" {
		t.Errorf("expected trimmed payload 'hi', got %q", msg.Payload)
	}
}

func TestPublishChannelTooLong(t *testing.T) {
	resetMessages()
	oldMax := maxChannelLength
	maxChannelLength = 8
	defer func() { maxChannelLength = oldMax }()

	body := fmt.Sprintf(`{"channel":"%s","payload":"ok"}`, strings.Repeat("c", 9))
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp ErrorResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != "channel must be at most 8 characters" {
		t.Errorf("unexpected error message: %q", resp.Error)
	}
}

func TestPublishPayloadTooLong(t *testing.T) {
	resetMessages()
	oldMax := maxPayloadLength
	maxPayloadLength = 10
	defer func() { maxPayloadLength = oldMax }()

	body := fmt.Sprintf(`{"channel":"ok","payload":"%s"}`, strings.Repeat("p", 11))
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp ErrorResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != "payload must be at most 10 characters" {
		t.Errorf("unexpected error message: %q", resp.Error)
	}
}

func TestPublishAtLengthBoundaryAccepted(t *testing.T) {
	resetMessages()
	oldCh := maxChannelLength
	oldPl := maxPayloadLength
	maxChannelLength = 5
	maxPayloadLength = 5
	defer func() {
		maxChannelLength = oldCh
		maxPayloadLength = oldPl
	}()

	body := `{"channel":"abcde","payload":"12345"}`
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 at length boundary, got %d (%s)", w.Code, w.Body.String())
	}
}

func TestEnvSeconds_OverrideAndFallback(t *testing.T) {
	const key = "TEST_TRIGW_PROCESSOR_ENV_SECONDS"

	if got := envSeconds(key, 7*time.Second); got != 7*time.Second {
		t.Fatalf("expected fallback 7s when unset, got %v", got)
	}

	t.Setenv(key, "42")
	if got := envSeconds(key, 7*time.Second); got != 42*time.Second {
		t.Fatalf("expected override 42s, got %v", got)
	}

	t.Setenv(key, "not-a-number")
	if got := envSeconds(key, 7*time.Second); got != 7*time.Second {
		t.Fatalf("expected fallback for invalid value, got %v", got)
	}

	t.Setenv(key, "0")
	if got := envSeconds(key, 7*time.Second); got != 7*time.Second {
		t.Fatalf("expected fallback for zero, got %v", got)
	}

	t.Setenv(key, "-5")
	if got := envSeconds(key, 7*time.Second); got != 7*time.Second {
		t.Fatalf("expected fallback for negative, got %v", got)
	}
}

func TestMessagesQSearch(t *testing.T) {
	resetMessages()

	seeds := []struct{ channel, payload string }{
		{"alerts", "server down at us-east-1"},
		{"alerts", "deploy succeeded"},
		{"metrics", "cpu high on api-server"},
		{"logs", "warning: disk usage"},
	}
	for _, s := range seeds {
		body, _ := json.Marshal(map[string]string{"channel": s.channel, "payload": s.payload})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		publishHandler(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("seed publish failed: code=%d body=%s", w.Code, w.Body.String())
		}
	}

	type result struct {
		Total    int `json:"total"`
		Count    int `json:"count"`
		Messages []struct {
			Channel string `json:"channel"`
			Payload string `json:"payload"`
		} `json:"messages"`
	}

	t.Run("matches payload substring", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/messages?q=server", nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var r result
		json.NewDecoder(w.Body).Decode(&r)
		// "server down at us-east-1" + "cpu high on api-server" の 2 件
		if r.Total != 2 {
			t.Fatalf("expected 2 matches for 'server', got %d (%+v)", r.Total, r.Messages)
		}
	})

	t.Run("matches channel substring", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/messages?q=ert", nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		var r result
		json.NewDecoder(w.Body).Decode(&r)
		// "alerts" * 2 件
		if r.Total != 2 {
			t.Fatalf("expected 2 matches for 'ert', got %d", r.Total)
		}
	})

	t.Run("case-insensitive", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/messages?q=ALERTS", nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		var r result
		json.NewDecoder(w.Body).Decode(&r)
		if r.Total != 2 {
			t.Fatalf("expected 2 case-insensitive matches, got %d", r.Total)
		}
	})

	t.Run("no match returns empty", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/messages?q=nothingmatcheshere", nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		var r result
		json.NewDecoder(w.Body).Decode(&r)
		if r.Total != 0 || r.Count != 0 {
			t.Fatalf("expected 0, got %d / %d", r.Total, r.Count)
		}
	})

	t.Run("blank q is ignored", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/messages?q=%20%20", nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		var r result
		json.NewDecoder(w.Body).Decode(&r)
		if r.Total != 4 {
			t.Fatalf("expected all 4 messages, got %d", r.Total)
		}
	})

	t.Run("combines with channel filter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/messages?channel=alerts&q=deploy", nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		var r result
		json.NewDecoder(w.Body).Decode(&r)
		if r.Total != 1 {
			t.Fatalf("expected 1 (alerts + 'deploy'), got %d", r.Total)
		}
	})

	t.Run("q too long rejected", func(t *testing.T) {
		long := strings.Repeat("a", maxSearchLength+1)
		req := httptest.NewRequest(http.MethodGet, "/api/messages?q="+long, nil)
		w := httptest.NewRecorder()
		messagesHandler(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for q too long, got %d", w.Code)
		}
	})
}

func TestNormalizeSearchQuery(t *testing.T) {
	original := maxSearchLength
	maxSearchLength = 5
	defer func() { maxSearchLength = original }()

	if v, err := normalizeSearchQuery(""); v != "" || err != nil {
		t.Fatalf("empty: got %q err=%v", v, err)
	}
	if v, err := normalizeSearchQuery("   "); v != "" || err != nil {
		t.Fatalf("whitespace: got %q err=%v", v, err)
	}
	if v, err := normalizeSearchQuery("  AbC  "); v != "abc" || err != nil {
		t.Fatalf("trim+lower: got %q err=%v", v, err)
	}
	if _, err := normalizeSearchQuery("toolong"); err == nil {
		t.Fatalf("expected error for too long")
	}
}

func TestStatsHandlerMethodNotAllowed(t *testing.T) {
	// POST 等非 GET は 405 で拒否される。
	req := httptest.NewRequest(http.MethodPost, "/api/stats", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestStatsHandlerFilteringByChannel(t *testing.T) {
	resetMessages()
	for _, ch := range []string{"a", "a", "b", "c"} {
		body, _ := json.Marshal(map[string]string{"channel": ch, "payload": "data"})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/stats?channel=a", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["total_messages"].(float64)) != 2 {
		t.Fatalf("expected 2 total for channel=a, got %v", resp["total_messages"])
	}
	channels := resp["channels"].(map[string]interface{})
	if len(channels) != 1 || channels["a"].(float64) != 2 {
		t.Fatalf("expected {a:2}, got %v", channels)
	}
}

func TestStatsHandlerFilteringByQ(t *testing.T) {
	resetMessages()
	cases := []struct {
		channel string
		payload string
	}{
		{"alerts", "disk full"},
		{"alerts", "cpu high"},
		{"info", "disk replaced"},
	}
	for _, c := range cases {
		body, _ := json.Marshal(map[string]string{"channel": c.channel, "payload": c.payload})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/stats?q=disk", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	// "disk" を含むのは 2 件（alerts/disk full と info/disk replaced）
	if int(resp["total_messages"].(float64)) != 2 {
		t.Fatalf("expected 2 total for q=disk, got %v", resp["total_messages"])
	}
}

func TestStatsHandlerFilteringBySinceUntil(t *testing.T) {
	resetMessages()
	// 直接 messages にタイムスタンプ込みで投入する（publishHandler は now を打つため）
	now := time.Now().UTC()
	mu.Lock()
	messages = []Message{
		{ID: "1", Channel: "a", Payload: "p1", CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "2", Channel: "a", Payload: "p2", CreatedAt: now.Add(-1 * time.Hour)},
		{ID: "3", Channel: "b", Payload: "p3", CreatedAt: now},
	}
	mu.Unlock()

	// 過去 90 分以内
	since := now.Add(-90 * time.Minute).Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodGet, "/api/stats?since="+since, nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	// since 以降は 2 件 (ID=2,3)
	if int(resp["total_messages"].(float64)) != 2 {
		t.Fatalf("expected 2 since=-90m, got %v", resp["total_messages"])
	}
}

func TestStatsHandlerSinceGreaterThanUntilIsRejected(t *testing.T) {
	resetMessages()
	req := httptest.NewRequest(http.MethodGet,
		"/api/stats?since=2030-01-02T00:00:00Z&until=2030-01-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestStatsHandlerInvalidSinceIsRejected(t *testing.T) {
	resetMessages()
	req := httptest.NewRequest(http.MethodGet, "/api/stats?since=not-a-date", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestStatsHandlerQTooLongIsRejected(t *testing.T) {
	resetMessages()
	long := strings.Repeat("a", maxSearchLength+1)
	req := httptest.NewRequest(http.MethodGet, "/api/stats?q="+long, nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestStatsHandlerNoFiltersReturnsAll(t *testing.T) {
	// 後方互換性の回帰テスト: フィルタ無しなら従来通り全件集計を返す。
	resetMessages()
	for _, ch := range []string{"x", "x", "y"} {
		body, _ := json.Marshal(map[string]string{"channel": ch, "payload": "d"})
		req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBuffer(body))
		w := httptest.NewRecorder()
		publishHandler(w, req)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	statsHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["total_messages"].(float64)) != 3 {
		t.Fatalf("expected 3 total, got %v", resp["total_messages"])
	}
}

// seedDeletableMessages は削除テスト用に固定の CreatedAt を持つメッセージ群を直接挿入する。
func seedDeletableMessages() {
	mu.Lock()
	messages = []Message{
		{ID: "m1", Channel: "alerts", Payload: "a", Processed: true, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "m2", Channel: "alerts", Payload: "b", Processed: true, CreatedAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "m3", Channel: "info", Payload: "c", Processed: true, CreatedAt: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "m4", Channel: "info", Payload: "d", Processed: true, CreatedAt: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)},
		{ID: "m5", Channel: "debug", Payload: "e", Processed: true, CreatedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
	}
	mu.Unlock()
}

func TestDeleteMessages_MissingFiltersReturns400(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete, "/api/messages", nil)
	w := httptest.NewRecorder()
	// 経由は main の mux と同じく method switch（テストでは直接ハンドラを呼ぶ）
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	mu.RLock()
	got := len(messages)
	mu.RUnlock()
	if got != 5 {
		t.Fatalf("expected 5 still present, got %d", got)
	}
}

func TestDeleteMessages_ByChannel(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete, "/api/messages?channel=alerts", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["deleted"].(float64)) != 2 {
		t.Fatalf("expected deleted=2, got %v", resp["deleted"])
	}
	if resp["channel"] != "alerts" {
		t.Fatalf("expected channel=alerts, got %v", resp["channel"])
	}
	if resp["before"] != nil {
		t.Fatalf("expected before=null, got %v", resp["before"])
	}
	mu.RLock()
	defer mu.RUnlock()
	if len(messages) != 3 {
		t.Fatalf("expected 3 remaining, got %d", len(messages))
	}
	for _, m := range messages {
		if m.Channel == "alerts" {
			t.Fatalf("alerts channel still present: %s", m.ID)
		}
	}
}

func TestDeleteMessages_BeforeOnly(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?before=2026-03-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	// 2026-03-01 “未満” → 1月 / 2月 の 2 件のみ削除
	if int(resp["deleted"].(float64)) != 2 {
		t.Fatalf("expected deleted=2, got %v", resp["deleted"])
	}
	mu.RLock()
	defer mu.RUnlock()
	if len(messages) != 3 {
		t.Fatalf("expected 3 remaining, got %d", len(messages))
	}
}

func TestDeleteMessages_CombinedFilters(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	// channel=info かつ before=2026-04-01 → m3 のみ
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?channel=info&before=2026-04-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["deleted"].(float64)) != 1 {
		t.Fatalf("expected deleted=1, got %v", resp["deleted"])
	}
	mu.RLock()
	defer mu.RUnlock()
	if len(messages) != 4 {
		t.Fatalf("expected 4 remaining, got %d", len(messages))
	}
	for _, m := range messages {
		if m.ID == "m3" {
			t.Fatalf("m3 should have been deleted")
		}
	}
}

func TestDeleteMessages_NoMatchReturnsZero(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?channel=nonexistent", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["deleted"].(float64)) != 0 {
		t.Fatalf("expected deleted=0, got %v", resp["deleted"])
	}
	mu.RLock()
	defer mu.RUnlock()
	if len(messages) != 5 {
		t.Fatalf("expected all 5 still present, got %d", len(messages))
	}
}

func TestDeleteMessages_InvalidBeforeReturns400(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?before=not-a-date", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if !strings.Contains(resp["error"], "before") {
		t.Fatalf("expected error to mention 'before', got %q", resp["error"])
	}
}

func TestDeleteMessages_BlankChannelTreatedAsUnspecified(t *testing.T) {
	// channel=" " のように空白だけは指定なし扱いとし、since/before も無ければ 400 を返す。
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?channel=%20%20", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestDeleteMessages_SinceOnly(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	// since=2026-03-01 (包含) → 3月/4月/5月 の 3 件
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?since=2026-03-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["deleted"].(float64)) != 3 {
		t.Fatalf("expected deleted=3, got %v", resp["deleted"])
	}
	if resp["since"] != "2026-03-01T00:00:00Z" {
		t.Fatalf("expected since=2026-03-01T00:00:00Z, got %v", resp["since"])
	}
	if resp["before"] != nil {
		t.Fatalf("expected before=null, got %v", resp["before"])
	}
	mu.RLock()
	defer mu.RUnlock()
	if len(messages) != 2 {
		t.Fatalf("expected 2 remaining (m1/m2), got %d", len(messages))
	}
}

func TestDeleteMessages_SinceAndBeforeRange(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	// [since=2026-02-01, before=2026-04-01) → m2(2月) と m3(3月) の 2 件
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?since=2026-02-01T00:00:00Z&before=2026-04-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["deleted"].(float64)) != 2 {
		t.Fatalf("expected deleted=2, got %v", resp["deleted"])
	}
	mu.RLock()
	defer mu.RUnlock()
	remainingIDs := []string{}
	for _, m := range messages {
		remainingIDs = append(remainingIDs, m.ID)
	}
	if len(remainingIDs) != 3 {
		t.Fatalf("expected 3 remaining, got %d: %v", len(remainingIDs), remainingIDs)
	}
}

func TestDeleteMessages_SinceChannelAndBefore(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	// channel=info && since=2026-03-01 && before=2026-05-01 → m3 のみ
	// (m4 は info かつ 4月だが before 範囲内、since 以降 → 削除)
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?channel=info&since=2026-03-01T00:00:00Z&before=2026-05-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["deleted"].(float64)) != 2 {
		t.Fatalf("expected deleted=2 (m3+m4), got %v", resp["deleted"])
	}
	mu.RLock()
	defer mu.RUnlock()
	for _, m := range messages {
		if m.ID == "m3" || m.ID == "m4" {
			t.Fatalf("%s should have been deleted", m.ID)
		}
	}
}

func TestDeleteMessages_BeforeLessThanSinceReturns400(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?since=2026-06-01T00:00:00Z&before=2026-03-01T00:00:00Z", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if !strings.Contains(resp["error"], "since") || !strings.Contains(resp["error"], "before") {
		t.Fatalf("expected error to mention 'since' and 'before', got %q", resp["error"])
	}
	// ストアは温存されている
	mu.RLock()
	defer mu.RUnlock()
	if len(messages) != 5 {
		t.Fatalf("expected 5 remaining, got %d", len(messages))
	}
}

func TestDeleteMessages_InvalidSinceReturns400(t *testing.T) {
	resetMessages()
	seedDeletableMessages()
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?since=not-a-date", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var resp map[string]string
	json.NewDecoder(w.Body).Decode(&resp)
	if !strings.Contains(resp["error"], "since") {
		t.Fatalf("expected error to mention 'since', got %q", resp["error"])
	}
}

func TestDeleteMessages_SinceBoundaryInclusive(t *testing.T) {
	// since=CreatedAt のレコードは「削除対象に含まれる」（包含境界の回帰）
	resetMessages()
	seedDeletableMessages()
	// m3 の CreatedAt はちょうど 2026-03-01 00:00:00 UTC
	req := httptest.NewRequest(http.MethodDelete,
		"/api/messages?since=2026-03-01T00:00:00Z&before=2026-03-01T00:00:01Z", nil)
	w := httptest.NewRecorder()
	deleteMessagesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp map[string]interface{}
	json.NewDecoder(w.Body).Decode(&resp)
	if int(resp["deleted"].(float64)) != 1 {
		t.Fatalf("expected deleted=1 (m3 only), got %v", resp["deleted"])
	}
}

// --- GET /api/messages/{id} ---

func seedThreeMessages(t *testing.T) []Message {
	t.Helper()
	resetMessages()
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	m1 := Message{ID: "id-1", Channel: "alpha", Payload: "p1", Processed: true, CreatedAt: now}
	m2 := Message{ID: "id-2", Channel: "alpha", Payload: "p2", Processed: true, CreatedAt: now.Add(time.Second)}
	m3 := Message{ID: "id-3", Channel: "beta", Payload: "p3", Processed: true, CreatedAt: now.Add(2 * time.Second)}
	mu.Lock()
	messages = []Message{m1, m2, m3}
	mu.Unlock()
	return []Message{m1, m2, m3}
}

func TestGetMessageByID_ReturnsMessageWhenFound(t *testing.T) {
	seeded := seedThreeMessages(t)
	target := seeded[1] // id-2
	req := httptest.NewRequest(http.MethodGet, "/api/messages/id-2", nil)
	req.SetPathValue("id", target.ID)
	w := httptest.NewRecorder()
	getMessageByIDHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	var got Message
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v body=%s", err, w.Body.String())
	}
	if got.ID != target.ID || got.Channel != target.Channel || got.Payload != target.Payload {
		t.Errorf("unexpected message: %+v want %+v", got, target)
	}
}

func TestGetMessageByID_ReturnsNotFoundForUnknownID(t *testing.T) {
	seedThreeMessages(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/nope", nil)
	req.SetPathValue("id", "nope")
	w := httptest.NewRecorder()
	getMessageByIDHandler(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	var resp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp.Error == "" {
		t.Errorf("expected non-empty error field")
	}
}

func TestGetMessageByID_ReturnsNotFoundForBlankID(t *testing.T) {
	seedThreeMessages(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/", nil)
	req.SetPathValue("id", "   ")
	w := httptest.NewRecorder()
	getMessageByIDHandler(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for blank id, got %d", w.Code)
	}
}

func TestGetMessageByID_RejectsNonGETMethods(t *testing.T) {
	seedThreeMessages(t)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/api/messages/id-1", nil)
		req.SetPathValue("id", "id-1")
		w := httptest.NewRecorder()
		getMessageByIDHandler(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("method %s: expected 405, got %d", method, w.Code)
		}
	}
}

func TestGetMessageByID_DoesNotMisMatchWithinSameChannel(t *testing.T) {
	// 同じ channel に複数メッセージがあっても、ID 完全一致のもののみ返ること。
	seeded := seedThreeMessages(t)
	req := httptest.NewRequest(http.MethodGet, "/api/messages/id-1", nil)
	req.SetPathValue("id", "id-1")
	w := httptest.NewRecorder()
	getMessageByIDHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var got Message
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != seeded[0].ID {
		t.Errorf("expected id-1, got %s", got.ID)
	}
	if got.Payload != "p1" {
		t.Errorf("expected payload p1, got %s", got.Payload)
	}
}
