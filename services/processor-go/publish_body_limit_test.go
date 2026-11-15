package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDefaultPublishBodyLimitCoversMaxLengths(t *testing.T) {
	got := defaultPublishBodyLimit(256, 65536)
	want := 256 + 65536 + publishBodyOverhead
	if got != want {
		t.Fatalf("defaultPublishBodyLimit = %d, want %d", got, want)
	}
}

// MAX_PAYLOAD_LENGTH ちょうどの payload は受理されなければならない。
// 以前はボディ上限が 64 KiB 固定だったため、既定値 65536 文字の payload が
// JSON エンベロープ分だけ超過して 413 になっていた。
func TestPublishAcceptsPayloadAtMaxLength(t *testing.T) {
	resetMessages()
	oldBody := maxPublishBody
	maxPublishBody = int64(defaultPublishBodyLimit(maxChannelLength, maxPayloadLength))
	defer func() { maxPublishBody = oldBody }()

	payload := strings.Repeat("a", maxPayloadLength)
	body := fmt.Sprintf(`{"channel":"test","payload":"%s"}`, payload)
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for payload of exactly MAX_PAYLOAD_LENGTH (%d), got %d: %s",
			maxPayloadLength, w.Code, w.Body.String())
	}
}

// 引き上げた MAX_PAYLOAD_LENGTH に追従してボディ上限も広がることを確認する。
func TestPublishBodyLimitFollowsRaisedPayloadLength(t *testing.T) {
	resetMessages()
	oldPayload, oldBody := maxPayloadLength, maxPublishBody
	maxPayloadLength = 200 * 1024
	maxPublishBody = int64(defaultPublishBodyLimit(maxChannelLength, maxPayloadLength))
	defer func() { maxPayloadLength, maxPublishBody = oldPayload, oldBody }()

	payload := strings.Repeat("b", 150*1024)
	body := fmt.Sprintf(`{"channel":"test","payload":"%s"}`, payload)
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPublishBodyLimitOverrideReturns413(t *testing.T) {
	resetMessages()
	oldBody := maxPublishBody
	maxPublishBody = 32
	defer func() { maxPublishBody = oldBody }()

	body := `{"channel":"test","payload":"this body is longer than 32 bytes"}`
	req := httptest.NewRequest(http.MethodPost, "/api/messages", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	publishHandler(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
	var resp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error != "request body too large" {
		t.Fatalf("unexpected error: %q", resp.Error)
	}
}

func TestMethodNotAllowedSetsAllowHeader(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		method  string
		path    string
		allow   string
	}{
		{"messages router PATCH", messagesRouter, http.MethodPatch, "/api/messages", "GET, POST, DELETE"},
		{"messages router PUT", messagesRouter, http.MethodPut, "/api/messages", "GET, POST, DELETE"},
		{"publish GET", publishHandler, http.MethodGet, "/api/messages", "POST"},
		{"stats POST", statsHandler, http.MethodPost, "/api/stats", "GET"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			tc.handler(w, req)
			if w.Code != http.StatusMethodNotAllowed {
				t.Fatalf("expected 405, got %d", w.Code)
			}
			if got := w.Header().Get("Allow"); got != tc.allow {
				t.Fatalf("Allow = %q, want %q", got, tc.allow)
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q", ct)
			}
		})
	}
}
