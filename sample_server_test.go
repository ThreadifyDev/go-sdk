package threadify

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type sampleThreadRecord struct {
	refs   map[string]any
	events []map[string]any
	status string
}

type sampleThreadServer struct {
	server *httptest.Server

	mu         sync.Mutex
	actions    []string
	threads    map[string]*sampleThreadRecord
	nextID     int
	startDelay time.Duration
}

func newSampleThreadServer(t *testing.T, startDelay time.Duration) *sampleThreadServer {
	t.Helper()

	s := &sampleThreadServer{
		threads:    make(map[string]*sampleThreadRecord),
		startDelay: startDelay,
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		for {
			var request map[string]any
			if err := conn.ReadJSON(&request); err != nil {
				return
			}

			response := s.handle(request)
			if response == nil {
				continue
			}
			if err := conn.WriteJSON(response); err != nil {
				return
			}
		}
	}))

	t.Cleanup(s.server.Close)
	return s
}

func (s *sampleThreadServer) wsURL() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

func (s *sampleThreadServer) handle(request map[string]any) map[string]any {
	action := asString(request[FieldAction])

	s.mu.Lock()
	s.actions = append(s.actions, action)
	s.mu.Unlock()

	switch action {
	case ActionConnect:
		return map[string]any{
			FieldAction: ActionConnect,
			FieldStatus: StatusSuccess,
		}

	case ActionStartThread:
		if s.startDelay > 0 {
			time.Sleep(s.startDelay)
		}

		s.mu.Lock()
		s.nextID++
		threadID := fmt.Sprintf("sample-thread-%d", s.nextID)
		s.threads[threadID] = &sampleThreadRecord{
			refs:   cloneAnyMap(asMap(request[FieldRefs])),
			status: StatusInProgress,
		}
		s.mu.Unlock()

		return map[string]any{
			FieldAction:      ActionStartThread,
			FieldStatus:      StatusSuccess,
			FieldThreadID:    threadID,
			FieldAccessLevel: string(ForParticipant),
		}

	case ActionAddRefs:
		threadID := asString(request[FieldThreadID])
		s.mu.Lock()
		thread := s.threads[threadID]
		if thread != nil {
			for key, value := range asMap(request[FieldRefs]) {
				thread.refs[key] = value
			}
		}
		s.mu.Unlock()

		return map[string]any{
			FieldAction: ActionAddRefs,
			FieldStatus: StatusSuccess,
		}

	case ActionRecordThreadEvent:
		threadID := asString(request[FieldThreadID])
		s.mu.Lock()
		thread := s.threads[threadID]
		if thread != nil {
			thread.events = append(thread.events, cloneAnyMap(request))
		}
		s.mu.Unlock()

		return map[string]any{
			FieldAction: ActionRecordThreadEvent,
			FieldStatus: StatusSuccess,
		}

	case ActionThreadEnd:
		threadID := asString(request[FieldThreadID])
		status := asString(request[FieldStatus])
		s.mu.Lock()
		thread := s.threads[threadID]
		if thread != nil {
			thread.status = status
		}
		s.mu.Unlock()

		return map[string]any{
			FieldAction:       ActionThreadEnd,
			FieldStatus:       StatusSuccess,
			FieldThreadStatus: status,
			FieldCompletedAt:  time.Now().UTC().Format(time.RFC3339),
		}
	}

	return map[string]any{
		FieldAction:  action,
		FieldStatus:  StatusError,
		FieldMessage: "unsupported sample action",
	}
}

func (s *sampleThreadServer) snapshot(threadID string) ([]string, *sampleThreadRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	actions := append([]string(nil), s.actions...)
	thread := s.threads[threadID]
	if thread == nil {
		return actions, nil
	}

	copyRecord := &sampleThreadRecord{
		refs:   cloneAnyMap(thread.refs),
		events: make([]map[string]any, len(thread.events)),
		status: thread.status,
	}
	for i, event := range thread.events {
		copyRecord.events[i] = cloneAnyMap(event)
	}
	return actions, copyRecord
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return make(map[string]any)
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func connectToSampleServer(t *testing.T, server *sampleThreadServer) *Connection {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := Connect(ctx, "sample-api-key",
		WithServiceName("sample-service"),
		WithWSURL(server.wsURL()),
	)
	if err != nil {
		t.Fatalf("Connect() error: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestSDKAgainstSampleWebSocketServer(t *testing.T) {
	server := newSampleThreadServer(t, 0)
	conn := connectToSampleServer(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	thread, err := conn.Start(ctx, "Sample order", WithContract("order_processing"))
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	if err := thread.AddRefs(ctx, map[string]string{"orderId": "ORD-123"}); err != nil {
		t.Fatalf("AddRefs() error: %v", err)
	}

	result, err := thread.Step("order_received").
		AddContext(map[string]any{"amount": 99.95, "currency": "EUR"}).
		Success(ctx, "Order accepted")
	if err != nil {
		t.Fatalf("Success() error: %v", err)
	}
	if result.ThreadID != thread.ThreadID || result.Status != StatusSuccess {
		t.Fatalf("unexpected step result: %#v", result)
	}

	failedResult, err := thread.Step("payment_attempted").
		AddContext(map[string]any{"provider": "sample-payments"}).
		Failed(ctx, "Payment declined")
	if err != nil {
		t.Fatalf("Failed() error: %v", err)
	}
	if failedResult.ThreadID != thread.ThreadID || failedResult.Status != StatusFailed {
		t.Fatalf("unexpected failed step result: %#v", failedResult)
	}

	if _, err := thread.Complete(ctx, "Order flow finished"); err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	actions, recorded := server.snapshot(thread.ThreadID)
	wantActions := []string{
		ActionConnect,
		ActionStartThread,
		ActionAddRefs,
		ActionRecordThreadEvent,
		ActionRecordThreadEvent,
		ActionThreadEnd,
	}
	if !slices.Equal(actions, wantActions) {
		t.Fatalf("server actions = %v, want %v", actions, wantActions)
	}
	if recorded == nil {
		t.Fatal("sample server did not create the thread")
	}
	if recorded.refs["orderId"] != "ORD-123" {
		t.Fatalf("orderId ref = %v, want ORD-123", recorded.refs["orderId"])
	}
	if len(recorded.events) != 2 {
		t.Fatalf("recorded events = %d, want 2", len(recorded.events))
	}
	eventContext := asMap(recorded.events[0][FieldContext])
	if eventContext["currency"] != "EUR" {
		t.Fatalf("step currency context = %v, want EUR", eventContext["currency"])
	}
	if status := asString(recorded.events[1][FieldStatus]); status != StatusFailed {
		t.Fatalf("second step status = %q, want %q", status, StatusFailed)
	}
	if recorded.status != StatusCompleted {
		t.Fatalf("thread status = %q, want %q", recorded.status, StatusCompleted)
	}
}

func TestSampleServer_ExpiredBeforeSendDoesNotCreateThread(t *testing.T) {
	server := newSampleThreadServer(t, 0)
	conn := connectToSampleServer(t, server)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	thread, err := conn.Start(ctx, "Must not be sent")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want context.DeadlineExceeded", err)
	}
	if thread != nil {
		t.Fatalf("Start() thread = %#v, want nil", thread)
	}

	actions, _ := server.snapshot("")
	if len(actions) != 1 || actions[0] != ActionConnect {
		t.Fatalf("server actions = %v, want only %q", actions, ActionConnect)
	}
}

func TestSampleServer_TimeoutAfterSendHasUnknownOutcome(t *testing.T) {
	server := newSampleThreadServer(t, 100*time.Millisecond)
	conn := connectToSampleServer(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	thread, err := conn.Start(ctx, "May still be created")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want context.DeadlineExceeded", err)
	}
	if thread != nil {
		t.Fatalf("Start() thread = %#v, want nil", thread)
	}

	deadline := time.Now().Add(time.Second)
	for {
		_, recorded := server.snapshot("sample-thread-1")
		if recorded != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not create the thread after the client timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
