package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Message struct {
	ID        string    `json:"id"`
	Channel   string    `json:"channel"`
	Payload   string    `json:"payload"`
	Processed bool      `json:"processed"`
	CreatedAt time.Time `json:"created_at"`
}

type HealthResponse struct {
	Status    string `json:"status"`
	Service   string `json:"service"`
	Timestamp string `json:"timestamp"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

var (
	messages []Message
	mu       sync.RWMutex
	logger   *log.Logger
)

func init() {
	logger = log.New(os.Stdout, "[processor-go] ", log.LstdFlags)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(HealthResponse{
		Status:    "ok",
		Service:   "processor-go",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		return
	}

	var input struct {
		Channel string `json:"channel"`
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "invalid JSON body"})
		return
	}
	if input.Channel == "" || input.Payload == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "channel and payload are required"})
		return
	}

	msg := Message{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Channel:   input.Channel,
		Payload:   input.Payload,
		Processed: true,
		CreatedAt: time.Now().UTC(),
	}

	mu.Lock()
	messages = append(messages, msg)
	mu.Unlock()

	logger.Printf("Processed message on channel %s", msg.Channel)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(msg)
}

func messagesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		return
	}

	channel := r.URL.Query().Get("channel")

	mu.RLock()
	defer mu.RUnlock()

	var result []Message
	if channel != "" {
		for _, m := range messages {
			if m.Channel == channel {
				result = append(result, m)
			}
		}
	} else {
		result = messages
	}

	if result == nil {
		result = []Message{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"messages": result,
		"count":    len(result),
	})
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	mu.RLock()
	defer mu.RUnlock()

	channels := make(map[string]int)
	for _, m := range messages {
		channels[m.Channel]++
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_messages": len(messages),
		"channels":       channels,
	})
}

func main() {
	port := os.Getenv("PROCESSOR_PORT")
	if port == "" {
		port = "8002"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/messages", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			messagesHandler(w, r)
		case http.MethodPost:
			publishHandler(w, r)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		}
	})
	mux.HandleFunc("/api/stats", statsHandler)

	logger.Printf("Starting processor service on port %s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		logger.Fatalf("Server failed: %v", err)
	}
}
