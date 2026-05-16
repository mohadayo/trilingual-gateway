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
