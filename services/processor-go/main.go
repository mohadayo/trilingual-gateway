package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var allowedMessageSortFields = map[string]bool{
	"created_at": true,
	"channel":    true,
	"id":         true,
}

var allowedMessageSortOrders = map[string]bool{"asc": true, "desc": true}

func parseTimeQuery(value string) (time.Time, error) {
	v := strings.TrimSpace(value)
	if v == "" {
		return time.Time{}, fmt.Errorf("must not be blank")
	}
	if strings.HasSuffix(v, "Z") {
		v = v[:len(v)-1] + "+00:00"
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err == nil {
		return t.UTC(), nil
	}
	if t2, err2 := time.Parse(time.RFC3339, v); err2 == nil {
		return t2.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("must be an ISO 8601 / RFC 3339 datetime: %s", err.Error())
}

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
	messages          []Message
	mu                sync.RWMutex
	logger            *log.Logger
	maxMessages       int
	defaultPageLimit  int
	maxPageLimit      int
	maxChannelLength  int
	maxPayloadLength  int
	maxSearchLength   int
	readHeaderTimeout time.Duration
	readTimeout       time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
)

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envSeconds(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return fallback
}

func init() {
	logger = log.New(os.Stdout, "[processor-go] ", log.LstdFlags)
	maxMessages = envInt("MAX_MESSAGES", 10000)
	defaultPageLimit = envInt("DEFAULT_PAGE_LIMIT", 50)
	maxPageLimit = envInt("MAX_PAGE_LIMIT", 1000)
	maxChannelLength = envInt("MAX_CHANNEL_LENGTH", 256)
	maxPayloadLength = envInt("MAX_PAYLOAD_LENGTH", 65536)
	maxSearchLength = envInt("MAX_SEARCH_LENGTH", 100)
	readHeaderTimeout = envSeconds("PROCESSOR_READ_HEADER_TIMEOUT", 5*time.Second)
	readTimeout = envSeconds("PROCESSOR_READ_TIMEOUT", 15*time.Second)
	writeTimeout = envSeconds("PROCESSOR_WRITE_TIMEOUT", 15*time.Second)
	idleTimeout = envSeconds("PROCESSOR_IDLE_TIMEOUT", 60*time.Second)
}

// normalizeSearchQuery は `q` クエリを正規化する。
//   - 未指定 / trim 後が空 → ("", nil, nil)：フィルタしない
//   - 上限超過 → ("", nil, error)：呼び出し側で 400 を返す
//   - 正常 → (lowercased, nil, nil)
//
// 第二戻り値 errResp は将来エラー詳細を返す拡張用で、現状は常に nil。
func normalizeSearchQuery(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if len(trimmed) > maxSearchLength {
		return "", fmt.Errorf("q must be at most %d characters", maxSearchLength)
	}
	return strings.ToLower(trimmed), nil
}

// messageFilters は GET /api/messages と GET /api/stats が共有する
// フィルタクエリの解析結果。
type messageFilters struct {
	channel string
	q       string
	since   *time.Time
	until   *time.Time
}

// parseMessageFilters は `channel` / `q` / `since` / `until` を解析する。
// 解析失敗時は errMsg を返す（呼び出し側で 400 を返す）。
func parseMessageFilters(query url.Values) (messageFilters, string) {
	var f messageFilters
	f.channel = query.Get("channel")

	q, qErr := normalizeSearchQuery(query.Get("q"))
	if qErr != nil {
		return messageFilters{}, qErr.Error()
	}
	f.q = q

	if raw := query.Get("since"); raw != "" {
		t, err := parseTimeQuery(raw)
		if err != nil {
			return messageFilters{}, fmt.Sprintf("query parameter 'since' %s", err.Error())
		}
		f.since = &t
	}
	if raw := query.Get("until"); raw != "" {
		t, err := parseTimeQuery(raw)
		if err != nil {
			return messageFilters{}, fmt.Sprintf("query parameter 'until' %s", err.Error())
		}
		f.until = &t
	}
	if f.since != nil && f.until != nil && f.until.Before(*f.since) {
		return messageFilters{}, "query parameter 'until' must be greater than or equal to 'since'"
	}
	return f, ""
}

// matchesFilters は単一メッセージが messageFilters に合致するかを返す。
// `messages` ストア全体のスキャンで使うため、ホットパス上にある（インライン化を期待）。
func (f messageFilters) matches(m Message) bool {
	if f.channel != "" && m.Channel != f.channel {
		return false
	}
	if f.since != nil && m.CreatedAt.Before(*f.since) {
		return false
	}
	if f.until != nil && m.CreatedAt.After(*f.until) {
		return false
	}
	if f.q != "" {
		if !strings.Contains(strings.ToLower(m.Channel), f.q) &&
			!strings.Contains(strings.ToLower(m.Payload), f.q) {
			return false
		}
	}
	return true
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
	// channel / payload をトリムし、空白のみの値や長さ超過を拒否する。
	channel := strings.TrimSpace(input.Channel)
	payload := strings.TrimSpace(input.Payload)
	if channel == "" || payload == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "channel and payload are required"})
		return
	}
	if len(channel) > maxChannelLength {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error: fmt.Sprintf("channel must be at most %d characters", maxChannelLength),
		})
		return
	}
	if len(payload) > maxPayloadLength {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error: fmt.Sprintf("payload must be at most %d characters", maxPayloadLength),
		})
		return
	}

	msg := Message{
		ID:        newUUID(),
		Channel:   channel,
		Payload:   payload,
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
	limit, offset := parsePagination(query)

	filters, ferr := parseMessageFilters(query)
	if ferr != "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: ferr})
		return
	}

	sortField := query.Get("sort")
	if sortField == "" {
		sortField = "created_at"
	}
	if !allowedMessageSortFields[sortField] {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "sort must be one of: channel, created_at, id"})
		return
	}

	sortOrder := query.Get("order")
	if sortOrder == "" {
		sortOrder = "asc"
	}
	if !allowedMessageSortOrders[sortOrder] {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "order must be one of: asc, desc"})
		return
	}

	mu.RLock()
	defer mu.RUnlock()

	filtered := make([]Message, 0, len(messages))
	for _, m := range messages {
		if !filters.matches(m) {
			continue
		}
		filtered = append(filtered, m)
	}

	reverse := sortOrder == "desc"
	sort.SliceStable(filtered, func(i, j int) bool {
		a, b := filtered[i], filtered[j]
		switch sortField {
		case "created_at":
			if reverse {
				return a.CreatedAt.After(b.CreatedAt)
			}
			return a.CreatedAt.Before(b.CreatedAt)
		case "channel":
			if reverse {
				return a.Channel > b.Channel
			}
			return a.Channel < b.Channel
		case "id":
			if reverse {
				return a.ID > b.ID
			}
			return a.ID < b.ID
		}
		return false
	})

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
		"sort":     sortField,
		"order":    sortOrder,
	})
}

// deleteMessagesHandler は channel / since / before のフィルタ AND で
// 一致するメッセージを削除する。誤って全件削除する事故を避けるため、
// すべて未指定の場合は 400 を返す。
//
// since は `CreatedAt >= since`（包含）、before は `CreatedAt < before`（排他）。
// 半開区間 [since, before) として組み合わせれば、「ある月だけ削除」のような
// 時間範囲指定の削除が 1 リクエストで実現できる。
func deleteMessagesHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	channel := strings.TrimSpace(query.Get("channel"))

	var before *time.Time
	if raw := query.Get("before"); raw != "" {
		t, err := parseTimeQuery(raw)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: fmt.Sprintf("query parameter 'before' %s", err.Error()),
			})
			return
		}
		before = &t
	}

	var since *time.Time
	if raw := query.Get("since"); raw != "" {
		t, err := parseTimeQuery(raw)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{
				Error: fmt.Sprintf("query parameter 'since' %s", err.Error()),
			})
			return
		}
		since = &t
	}

	if channel == "" && before == nil && since == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error: "at least one of 'channel', 'since' or 'before' must be provided",
		})
		return
	}

	// before は排他、since は包含なので since == before の場合に削除対象は空になる。
	// before < since は意味的に不整合のため 400 を返す（誤指定の早期検知）。
	if since != nil && before != nil && before.Before(*since) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error: "query parameter 'before' must be greater than or equal to 'since'",
		})
		return
	}

	mu.Lock()
	kept := make([]Message, 0, len(messages))
	deleted := 0
	for _, m := range messages {
		// 全フィルタに合致するものを削除（保持しない）
		matchChannel := channel == "" || m.Channel == channel
		matchBefore := before == nil || m.CreatedAt.Before(*before)
		// `!m.CreatedAt.Before(*since)` で `m.CreatedAt >= *since`（包含）
		matchSince := since == nil || !m.CreatedAt.Before(*since)
		if matchChannel && matchBefore && matchSince {
			deleted++
			continue
		}
		kept = append(kept, m)
	}
	messages = kept
	mu.Unlock()

	beforeOut := ""
	if before != nil {
		beforeOut = before.Format(time.RFC3339)
	}
	sinceOut := ""
	if since != nil {
		sinceOut = since.Format(time.RFC3339)
	}
	logger.Printf(
		"Messages deleted: count=%d channel=%q since=%q before=%q",
		deleted, channel, sinceOut, beforeOut,
	)

	resp := map[string]interface{}{
		"deleted": deleted,
		"channel": nullableString(channel),
		"since":   nullableString(sinceOut),
		"before":  nullableString(beforeOut),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// messageChannelsHandler は保持中のメッセージから distinct な channel 一覧のみを返す。
// `/api/stats` は per-channel カウントとペイロード集計を一緒に返すため、UI の
// チャネル選択ドロップダウン populate などの「名前だけ欲しい」用途には過剰になる。
// 本ハンドラは集計を行わず、distinct した channel 名を `q` / `since` / `until` で
// 絞り込み、`order` で並べ替え、`limit` / `offset` でページングして返す。
//
// `q` セマンティクスは `/api/messages` と同じで、channel もしくは payload に
// `q` を部分一致するメッセージを対象に distinct を取る。これにより
// 「`q=error` を含むメッセージが流れたチャネル」のような調査クエリが書ける。
//
// 経路は Go 1.22+ の ServeMux で `GET /api/messages/channels` リテラルとして登録され、
// `GET /api/messages/{id}` ワイルドカードよりも優先される（パターン仕様準拠）。
func messageChannelsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		return
	}

	query := r.URL.Query()
	filters, ferr := parseMessageFilters(query)
	if ferr != "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: ferr})
		return
	}

	sortOrder := query.Get("order")
	if sortOrder == "" {
		sortOrder = "asc"
	}
	if !allowedMessageSortOrders[sortOrder] {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "order must be one of: asc, desc"})
		return
	}

	limit, offset := parsePagination(query)

	mu.RLock()
	seen := make(map[string]struct{})
	for _, m := range messages {
		if !filters.matches(m) {
			continue
		}
		seen[m.Channel] = struct{}{}
	}
	mu.RUnlock()

	distinct := make([]string, 0, len(seen))
	for c := range seen {
		distinct = append(distinct, c)
	}
	if sortOrder == "desc" {
		sort.Sort(sort.Reverse(sort.StringSlice(distinct)))
	} else {
		sort.Strings(distinct)
	}

	total := len(distinct)
	start := offset
	if start > total {
		start = total
	}
	end := start + limit
	if end > total {
		end = total
	}
	page := distinct[start:end]
	if page == nil {
		page = []string{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"channels": page,
		"count":    len(page),
		"total":    total,
		"limit":    limit,
		"offset":   offset,
		"order":    sortOrder,
	})
}

// nullableString は空文字列を JSON null として表現するためのヘルパ。
// フィルタ未指定を明示するため、`""` ではなく `null` を返す。
func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// getMessageByIDHandler は path パラメータ `id` に一致するメッセージを 1 件返す。
// 経路は Go 1.22 の http.ServeMux パスパラメータ機能で
// `GET /api/messages/{id}` として登録される。
//
// - GET 以外: 405
// - id が空 or 一致なし: 404
// - 一致あり: 200 + Message
func getMessageByIDHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		return
	}

	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "message not found"})
		return
	}

	mu.RLock()
	defer mu.RUnlock()
	for _, m := range messages {
		if m.ID == id {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(m)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(ErrorResponse{Error: "message not found"})
}

// deleteMessageByIDHandler は DELETE /api/messages/{id} を処理する。
// id 完全一致の 1 件を削除し、削除前のメッセージを返す（クライアントは
// 別 GET なしで監査ログに残せる）。残ったメッセージの順序は保持する。
func deleteMessageByIDHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "message not found"})
		return
	}

	mu.Lock()
	var removed *Message
	for i := range messages {
		if messages[i].ID == id {
			m := messages[i]
			removed = &m
			messages = append(messages[:i], messages[i+1:]...)
			break
		}
	}
	mu.Unlock()

	if removed == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "message not found"})
		return
	}

	logger.Printf("Message deleted: id=%s channel=%s", removed.ID, removed.Channel)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"deleted": 1,
		"id":      removed.ID,
		"message": removed,
	})
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		return
	}

	filters, ferr := parseMessageFilters(r.URL.Query())
	if ferr != "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: ferr})
		return
	}

	mu.RLock()
	defer mu.RUnlock()

	channels := make(map[string]int)
	total := 0
	for _, m := range messages {
		if !filters.matches(m) {
			continue
		}
		channels[m.Channel]++
		total++
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total_messages": total,
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
		case http.MethodDelete:
			deleteMessagesHandler(w, r)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "method not allowed"})
		}
	})
	// distinct な channel 一覧。`GET /api/messages/{id}` ワイルドカードよりも
	// リテラルパターンが優先されるため、`channels` を ID として誤解されない。
	mux.HandleFunc("GET /api/messages/channels", messageChannelsHandler)
	// Go 1.22 の http.ServeMux パスパラメータ機能。`/api/messages` (集合) と
	// `/api/messages/{id}` (単一) は別経路として共存する。
	// 単一 path で GET と DELETE は別ハンドラに振り分け、URL 設計をシンメトリックに保つ。
	mux.HandleFunc("GET /api/messages/{id}", getMessageByIDHandler)
	mux.HandleFunc("DELETE /api/messages/{id}", deleteMessageByIDHandler)
	mux.HandleFunc("/api/stats", statsHandler)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
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
