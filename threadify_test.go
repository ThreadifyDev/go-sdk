package threadify

import (
	"context"
	"testing"
	"time"
)

func TestConnect_Success(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	if !conn.IsConnected() {
		t.Error("expected IsConnected to be true")
	}

	// Verify the connect message was sent.
	sent := mt.getSent()
	if len(sent) == 0 {
		t.Fatal("expected at least one sent message")
	}
	if sent[0]["action"] != "connect" {
		t.Errorf("expected action 'connect', got %v", sent[0]["action"])
	}
	if sent[0]["apiKey"] != "test-api-key" {
		t.Errorf("expected apiKey 'test-api-key', got %v", sent[0]["apiKey"])
	}
	if sent[0]["serviceName"] != "test-service" {
		t.Errorf("expected serviceName 'test-service', got %v", sent[0]["serviceName"])
	}
}

func TestConnect_EmptyAPIKey(t *testing.T) {
	ctx := context.Background()
	_, err := Connect(ctx, "",
		WithServiceName("service"),
		WithWSURL("wss://example.com"),
	)
	if err == nil {
		t.Error("expected error for empty API key")
	}
}

func TestConnect_InvalidMaxInFlight(t *testing.T) {
	mt := newMockTransport()
	ctx := context.Background()

	_, err := Connect(ctx, "key",
		WithServiceName("service"),
		WithWSURL("wss://example.com"),
		WithMaxInFlight(200),
		WithDialer(&mockDialer{transport: mt}),
	)
	if err == nil {
		t.Error("expected error for maxInFlight > 100")
	}
}

func TestConnect_ServerRejectsConnection(t *testing.T) {
	mt := newMockTransport()
	mt.enqueueResponse(map[string]any{
		"action":  "connect",
		"status":  "error",
		"message": "invalid API key",
	})

	ctx := context.Background()
	_, err := Connect(ctx, "bad-key",
		WithServiceName("service"),
		WithWSURL("wss://example.com"),
		WithDialer(&mockDialer{transport: mt}),
	)
	if err == nil {
		t.Error("expected error for rejected connection")
	}
	if err.Error() != "invalid API key" {
		t.Errorf("expected 'invalid API key', got %q", err.Error())
	}
}

func TestConnect_Timeout(t *testing.T) {
	mt := newMockTransport()
	// Don't enqueue any response — should timeout.

	ctx := context.Background()
	_, err := Connect(ctx, "key",
		WithServiceName("service"),
		WithWSURL("wss://example.com"),
		WithConnectTimeout(100*time.Millisecond),
		WithDialer(&mockDialer{transport: mt}),
	)
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestCreate(t *testing.T) {
	factory := Create(Config{
		APIKey:      "test-key",
		ServiceName: "my-service",
		WSURL:       "wss://example.com/threads",
	})

	if factory == nil {
		t.Fatal("expected factory to be non-nil")
	}
}

func TestConnection_Start_NonContract(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	// Enqueue startThread response.
	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-123",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx)
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if thread.ThreadID != "thread-123" {
		t.Errorf("expected threadId 'thread-123', got %q", thread.ThreadID)
	}
}

func TestConnection_Start_WithContract(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-456",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, WithContract("order_flow"), WithService("merchant-service"))
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if thread.ThreadID != "thread-456" {
		t.Errorf("expected threadId 'thread-456', got %q", thread.ThreadID)
	}

	// Verify the sent message includes contract and role.
	sent := mt.getSent()
	// The last sent message should be the startThread.
	startMsg := sent[len(sent)-1]
	if startMsg["contractName"] != "order_flow" {
		t.Errorf("expected contractName 'order_flow', got %v", startMsg["contractName"])
	}
	// Role should be derived from serviceName: "merchant-service" → "merchant"
	if startMsg["role"] != "merchant" {
		t.Errorf("expected role 'merchant', got %v", startMsg["role"])
	}
}

func TestConnection_Start_NotConnected(t *testing.T) {
	conn, _ := newTestConnection(t)
	_ = conn.Close()

	ctx := context.Background()
	_, err := conn.Start(ctx)
	if err == nil {
		t.Error("expected error when not connected")
	}
}

func TestConnection_Join_DirectJoin(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":     "joinThread",
		"status":     "success",
		"threadId":   "thread-789",
		"role":       "logistics",
		"contractId": "contract-001",
	})

	ctx := context.Background()
	thread, err := conn.Join(ctx, WithJoinThreadID("thread-789"), WithJoinRole("logistics"))
	if err != nil {
		t.Fatalf("Join() error: %v", err)
	}

	if thread.ThreadID != "thread-789" {
		t.Errorf("expected threadId 'thread-789', got %q", thread.ThreadID)
	}
	if thread.Role != "logistics" {
		t.Errorf("expected role 'logistics', got %q", thread.Role)
	}
}

func TestConnection_Join_TokenJoin(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	// Token must be >50 chars to trigger token-based join.
	// dummyJWTToken is used to trigger token-based join.
	dummyJWTToken := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0aHJlYWRJZCI6InRocmVhZC0xMjMiLCJyb2xlIjoibG9naXN0aWNzIn0.abcdef" // #nosec G101
	mt.enqueueResponse(map[string]any{
		"action":   "joinThread",
		"status":   "success",
		"threadId": "thread-token-123",
		"role":     "logistics",
	})

	ctx := context.Background()
	thread, err := conn.Join(ctx, WithJoinToken(dummyJWTToken))
	if err != nil {
		t.Fatalf("Join() error: %v", err)
	}

	if thread.ThreadID != "thread-token-123" {
		t.Errorf("expected threadId 'thread-token-123', got %q", thread.ThreadID)
	}

	// Verify threadToken was sent.
	sent := mt.getSent()
	joinMsg := sent[len(sent)-1]
	if joinMsg["threadToken"] != dummyJWTToken {
		t.Error("expected threadToken in sent message")
	}
}

func TestConnection_Join_EmptyTokenOrID(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	_, err := conn.Join(ctx)
	if err == nil {
		t.Error("expected error for empty tokenOrThreadId")
	}
}

func TestConnection_Join_InvalidParams(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	// Short string without role — ambiguous.
	_, err := conn.Join(ctx, WithJoinThreadID("short-id"))
	if err == nil {
		t.Error("expected error for short ID without role")
	}
}

func TestConnection_EventParsing(t *testing.T) {
	tests := []struct {
		event      string
		wantSource string
		wantType   string
	}{
		{"step.success", "execution", "success"},
		{"step.failed", "execution", "failed"},
		{"rule.violated", "validation", "violated"},
		{"rule.passed", "validation", "passed"},
		{"step.*", "execution", "*"},
		{"rule.*", "validation", "*"},
		{"*", "*", "*"},
	}

	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			source, eventType := parseEvent(tt.event)
			if source != tt.wantSource {
				t.Errorf("parseEvent(%q) source = %q, want %q", tt.event, source, tt.wantSource)
			}
			if eventType != tt.wantType {
				t.Errorf("parseEvent(%q) type = %q, want %q", tt.event, eventType, tt.wantType)
			}
		})
	}
}

func TestBuildEventTypes(t *testing.T) {
	tests := []struct {
		source   string
		eType    string
		expected []string
	}{
		{"*", "*", []string{"step.success", "step.failed", "rule.passed", "rule.violated"}},
		{"step", "*", []string{"step.success", "step.failed"}},
		{"rule", "*", []string{"rule.passed", "rule.violated"}},
		{"step", "success", []string{"step.success"}},
		{"rule", "violated", []string{"rule.violated"}},
	}

	for _, tt := range tests {
		t.Run(tt.source+"."+tt.eType, func(t *testing.T) {
			got := buildEventTypes(tt.source, tt.eType)
			if len(got) != len(tt.expected) {
				t.Errorf("buildEventTypes(%q, %q) = %v, want %v", tt.source, tt.eType, got, tt.expected)
				return
			}
			// Check all expected are present.
			gotMap := make(map[string]bool)
			for _, g := range got {
				gotMap[g] = true
			}
			for _, e := range tt.expected {
				if !gotMap[e] {
					t.Errorf("buildEventTypes missing %q", e)
				}
			}
		})
	}
}

func TestMergeUnique(t *testing.T) {
	tests := []struct {
		name    string
		a, b    []string
		wantLen int
	}{
		{"no overlap", []string{"a", "b"}, []string{"c", "d"}, 4},
		{"full overlap", []string{"a", "b"}, []string{"a", "b"}, 2},
		{"partial overlap", []string{"a", "b"}, []string{"b", "c"}, 3},
		{"empty a", []string{}, []string{"a"}, 1},
		{"both empty", []string{}, []string{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeUnique(tt.a, tt.b)
			if len(result) != tt.wantLen {
				t.Errorf("mergeUnique(%v, %v) len = %d, want %d", tt.a, tt.b, len(result), tt.wantLen)
			}
		})
	}
}

func TestSameElements(t *testing.T) {
	tests := []struct {
		name   string
		a, b   []string
		expect bool
	}{
		{"same order", []string{"a", "b"}, []string{"a", "b"}, true},
		{"different order", []string{"b", "a"}, []string{"a", "b"}, true},
		{"different length", []string{"a"}, []string{"a", "b"}, false},
		{"different elements", []string{"a", "c"}, []string{"a", "b"}, false},
		{"both empty", []string{}, []string{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sameElements(tt.a, tt.b)
			if result != tt.expect {
				t.Errorf("sameElements(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expect)
			}
		})
	}
}

func TestConnection_Subscribe_Unsubscribe(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	ctx := context.Background()
	err := conn.Subscribe(ctx, "step.success", "order_placed", func(n *Notification) {
		_ = n // handler registered
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	// Verify handler is registered.
	val, ok := conn.notificationHandlers.Load("step.success:order_placed")
	if !ok {
		t.Fatal("expected handler to be registered")
	}
	hl := val.(*handlerList)
	hl.mu.RLock()
	if len(hl.handlers) != 1 {
		t.Errorf("expected 1 handler, got %d", len(hl.handlers))
	}
	hl.mu.RUnlock()

	// Unsubscribe should remove the handler.
	err = conn.Unsubscribe(ctx, "step.success", "order_placed")
	if err != nil {
		t.Fatalf("Unsubscribe() error: %v", err)
	}

	_, ok = conn.notificationHandlers.Load("step.success:order_placed")
	if ok {
		t.Error("expected handler to be removed after Unsubscribe()")
	}
}

func TestConnection_HandleNotification_Deduplication(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	callCount := 0

	ctx := context.Background()
	err := conn.Subscribe(ctx, "step.success", "order_placed", func(_ *Notification) {
		callCount++
	})
	if err != nil {
		t.Fatalf("Subscribe() error: %v", err)
	}

	notifData := map[string]any{
		"notificationId":   "notif-001",
		"threadId":         "thread-123",
		"stepName":         "order_placed",
		"source":           "step",
		"notificationType": "step.success",
		"status":           "passed",
		"stepStatus":       "success",
		"severity":         "info",
		"message":          "Step completed",
	}

	// Handle the same notification twice.
	conn.handleNotification(notifData, "ack-token-1")
	conn.handleNotification(notifData, "ack-token-1")

	// Handler should only be called once due to deduplication.
	if callCount != 1 {
		t.Errorf("expected handler called 1 time, got %d", callCount)
	}
}
