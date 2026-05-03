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
