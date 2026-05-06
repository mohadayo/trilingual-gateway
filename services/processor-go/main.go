package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
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
	messages         []Message
	mu               sync.RWMutex
	logger           *log.Logger
	maxMessages      int
	defaultPageLimit int
	maxPageLimit     int
)

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func init() {
	logger = log.New(os.Stdout, "[processor-go] ", log.LstdFlags)
	maxMessages = envInt("MAX_MESSAGES", 10000)
	defaultPageLimit = envInt("DEFAULT_PAGE_LIMIT", 50)
	maxPageLimit = envInt("MAX_PAGE_LIMIT", 1000)
}

func parsePagination(q map[string][]string) (limit, offset int) {
	limit = defaultPageLimit
	if vs, ok := q["limit"]; ok && len(vs) > 0 {
		if n, err := strconv.Atoi(vs[0]); err == nil && n >= 0 {
			limit = n
		}
	}
	if limit <= 0 {
		limit = defaultPageLimit
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}

	offset = 0
	if vs, ok := q["offset"]; ok && len(vs) > 0 {
		if n, err := strconv.Atoi(vs[0]); err == nil && n >= 0 {
			offset = n
		}
	}
	return limit, offset
}

func newUUID() string {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
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

	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)

	var input struct {
		Channel string `json:"channel"`
		Payload string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.Header().Set("Content-Type", "application/json")
		if err.Error() == "http: request body too large" {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "request body too large"})
			return
		}
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
		ID:        newUUID(),
		Channel:   input.Channel,
		Payload:   input.Payload,
		Processed: true,
		CreatedAt: time.Now().UTC(),
	}

	mu.Lock()
	messages = append(messages, msg)
	if len(messages) > maxMessages {
		removed := len(messages) - maxMessages
		messages = messages[removed:]
		logger.Printf("Evicted %d old messages (store capped at %d)", removed, maxMessages)
	}
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

	query := r.URL.Query()
	channel := query.Get("channel")
	limit, offset := parsePagination(query)

	mu.RLock()
	defer mu.RUnlock()

	var filtered []Message
	if channel != "" {
		for _, m := range messages {
			if m.Channel == channel {
				filtered = append(filtered, m)
			}
		}
	} else {
		filtered = messages
	}

	total := len(filtered)
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}
	page := filtered[start:end]
	if page == nil {
		page = []Message{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"messages": page,
		"count":    len(page),
		"total":    total,
		"limit":    limit,
		"offset":   offset,
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

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	shutdownTimeout := 30 * time.Second
	if v := os.Getenv("SHUTDOWN_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			shutdownTimeout = time.Duration(n) * time.Second
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Printf("Starting processor service on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Server failed: %v", err)
		}
	}()

	<-ctx.Done()
	stop()
	logger.Println("Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Fatalf("Forced shutdown: %v", err)
	}
	logger.Println("Server stopped")
}
