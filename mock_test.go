package threadify

import (
	"context"
	"fmt"
	"sync"
)

// mockTransport is a test helper that simulates WebSocket communication.
type mockTransport struct {
	mu        sync.Mutex
	sent      []map[string]any
	recvQueue chan map[string]any
	closed    bool
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		recvQueue: make(chan map[string]any, 100),
	}
}

func (m *mockTransport) Send(msg map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("connection closed")
	}
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockTransport) Recv() (map[string]any, error) {
	msg, ok := <-m.recvQueue
	if !ok {
		return nil, fmt.Errorf("connection closed")
	}
	return msg, nil
}

func (m *mockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.recvQueue)
	}
	return nil
}

// getSent returns a copy of the sent messages.
func (m *mockTransport) getSent() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]map[string]any, len(m.sent))
	copy(out, m.sent)
	return out
}

// enqueueResponse pushes a message for Recv() to return.
func (m *mockTransport) enqueueResponse(msg map[string]any) {
	m.recvQueue <- msg
}

// mockDialer returns a pre-configured mockTransport.
type mockDialer struct {
	transport *mockTransport
}

func (d *mockDialer) Dial(_ context.Context, _ string) (Transport, error) {
	return d.transport, nil
}

// newTestConnection creates a Connection backed by a mock transport.
// It simulates a successful connect handshake.
func newTestConnection(t interface {
	Helper()
	Fatal(...any)
}) (*Connection, *mockTransport) {
	t.Helper()
	mt := newMockTransport()

	// Enqueue the connect success response.
	mt.enqueueResponse(map[string]any{
		"action": "connect",
		"status": "success",
	})

	ctx := context.Background()

	conn, err := Connect(ctx, "test-api-key",
		WithServiceName("test-service"),
		WithWSURL("wss://eng.threadify.dev/threads"),
		WithDialer(&mockDialer{transport: mt}),
	)
	if err != nil {
		t.Fatal("failed to create test connection:", err)
	}

	return conn, mt
}
