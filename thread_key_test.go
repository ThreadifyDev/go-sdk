package threadify

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type threadResult struct {
	thread *ThreadInstance
	err    error
}

func resolveTestThread(t *testing.T, conn *Connection, mt *mockTransport, key string, response map[string]any, options ...ThreadOptions) *ThreadInstance {
	t.Helper()
	after := len(mt.getSent())
	results := make(chan threadResult, 1)
	go func() {
		thread, err := conn.Thread(context.Background(), key, options...)
		results <- threadResult{thread, err}
	}()
	request := nextParityRequest(t, mt, "thread", after)
	parityReply(mt, request, response)
	result := <-results
	if result.err != nil {
		t.Fatal(result.err)
	}
	return result.thread
}

func TestThreadCreateAndResumeStoredMetadata(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	response := map[string]any{"threadId": "thread-1", "threadKey": "session:42", "label": "Agent session", "contractId": "contract-7", "contractName": "agent", "contractVersion": 3, "refs": map[string]string{"customerId": "customer-42"}, "tags": []string{"agent"}, "role": "runner", "accessLevel": "internal"}
	first := resolveTestThread(t, conn, mt, " session:42 ", response, ThreadOptions{Label: "Agent session", Contract: "agent", Refs: map[string]string{"customerId": "customer-42"}, Tags: []string{"agent"}, Role: "runner"})
	requests := mt.getSent()
	create := requests[len(requests)-1]
	if create["threadKey"] != "session:42" || create["contractName"] != "agent" || create["label"] != "Agent session" || create["serviceName"] != "test-service" || create["requestId"] == "" {
		t.Fatalf("wrong creation envelope: %+v", create)
	}
	resumed := resolveTestThread(t, conn, mt, "session:42", response)
	requests = mt.getSent()
	resume := requests[len(requests)-1]
	for _, field := range []string{"contractName", "label", "refs", "tags", "role"} {
		if _, ok := resume[field]; ok {
			t.Fatalf("key-only resume sends creation field %s", field)
		}
	}
	if resumed.ContractID != "contract-7" || resumed.ContractName != "agent" || resumed.ContractVersion != 3 || resumed.Label != "Agent session" || resumed.Refs["customerId"] != "customer-42" || len(resumed.Tags) != 1 {
		t.Fatalf("lost stored metadata: %+v", resumed)
	}
	if resumed == first || resumed.runtime() != first.runtime() {
		t.Fatal("resumed metadata must have a separate handle with shared runtime")
	}
	// A response snapshot is independent from older handles and returned maps.
	resumed.Refs["customerId"] = "local-change"
	if first.Refs["customerId"] != "customer-42" {
		t.Fatal("metadata snapshots share mutable refs")
	}
}

func TestThreadConcurrentRequestsAreCorrelated(t *testing.T) {
	for _, sameKey := range []bool{false, true} {
		t.Run(map[bool]string{false: "different keys", true: "same key"}[sameKey], func(t *testing.T) {
			conn, mt := newTestConnection(t)
			defer conn.Close()
			keys := []string{"session:1", "session:2"}
			if sameKey {
				keys[1] = keys[0]
			}
			resultCh := []chan threadResult{make(chan threadResult, 1), make(chan threadResult, 1)}
			after := len(mt.getSent())
			go func() { th, err := conn.Thread(context.Background(), keys[0]); resultCh[0] <- threadResult{th, err} }()
			first := nextParityRequest(t, mt, "thread", after)
			after = len(mt.getSent())
			go func() { th, err := conn.Thread(context.Background(), keys[1]); resultCh[1] <- threadResult{th, err} }()
			second := nextParityRequest(t, mt, "thread", after)
			if first["requestId"] == second["requestId"] {
				t.Fatal("request IDs collide")
			}
			// Wrong and uncorrelated replies must not satisfy either caller.
			mt.enqueueResponse(map[string]any{"action": "thread", "status": "success", "threadId": "wrong", "threadKey": keys[0]})
			parityReply(mt, map[string]any{"action": "thread", "requestId": "unknown"}, map[string]any{"threadId": "wrong", "threadKey": keys[0]})
			secondID := "thread-2"
			if sameKey {
				secondID = "thread-1"
			}
			parityReply(mt, second, map[string]any{"threadId": secondID, "threadKey": keys[1], "label": "second"})
			secondResult := <-resultCh[1]
			if secondResult.err != nil || secondResult.thread.Label != "second" {
				t.Fatalf("wrong second response: %+v", secondResult)
			}
			select {
			case r := <-resultCh[0]:
				t.Fatalf("first caller accepted unrelated reply: %+v", r)
			default:
			}
			parityReply(mt, first, map[string]any{"threadId": "thread-1", "threadKey": keys[0], "label": "first"})
			firstResult := <-resultCh[0]
			if firstResult.err != nil || firstResult.thread.Label != "first" {
				t.Fatalf("wrong first response: %+v", firstResult)
			}
			if sameKey {
				if firstResult.thread.runtime() != secondResult.thread.runtime() {
					t.Fatal("same thread has separate runtime")
				}
				wait := &pendingWait{ch: make(chan *Notification, 1), cancel: func() {}}
				firstResult.thread.runtime().pendingWaits.Store("turn", wait)
				registered, _ := conn.threads.Load("thread-1")
				registered.(*ThreadInstance).handleNotification(&Notification{StepName: "turn", StepStatus: "success"})
				select {
				case <-wait.ch:
				case <-time.After(time.Second):
					t.Fatal("notification did not reach resumed handle")
				}
			}
		})
	}
}

func TestThreadRejectsInvalidKeysBeforeSending(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	for _, key := range []string{"", "  ", strings.Repeat("é", 513), string([]byte{0xff})} {
		before := len(mt.getSent())
		if _, err := conn.Thread(context.Background(), key); err == nil {
			t.Fatalf("accepted invalid key %q", key)
		}
		if len(mt.getSent()) != before {
			t.Fatal("invalid key sent")
		}
	}
	if _, err := conn.Thread(context.Background(), "valid", ThreadOptions{}, ThreadOptions{}); err == nil {
		t.Fatal("accepted two options")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := conn.Thread(ctx, "valid"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context: %v", err)
	}
}

func TestThreadRejectsClosedConflictingAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response map[string]any
		want     string
	}{
		{"closed", map[string]any{"status": "error", "message": "thread is closed"}, "closed"},
		{"contract conflict", map[string]any{"status": "error", "message": "contract conflicts with stored contract"}, "contract conflicts"},
		{"wrong key", map[string]any{"threadId": "thread-1", "threadKey": "other"}, "invalid thread identity"},
		{"missing ID", map[string]any{"threadKey": "session"}, "invalid thread identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, mt := newTestConnection(t)
			defer conn.Close()
			errs := make(chan error, 1)
			go func() {
				_, err := conn.Thread(context.Background(), "session", ThreadOptions{Contract: "agent"})
				errs <- err
			}()
			req := nextParityRequest(t, mt, "thread", 0)
			parityReply(mt, req, tc.response)
			if err := <-errs; err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v; want %s", err, tc.want)
			}
			conn.threads.Range(func(_, _ any) bool { t.Error("failed resolution registered a handle"); return true })
		})
	}
}

func TestResumedThreadCancelClearsSharedGrant(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	response := map[string]any{"threadId": "thread", "threadKey": "session"}
	first := resolveTestThread(t, conn, mt, "session", response)
	resumed := resolveTestThread(t, conn, mt, "session", response)
	grants := make(chan *PermissionGrant, 1)
	errs := make(chan error, 1)
	before := len(mt.getSent())
	go func() { g, err := resumed.WaitFor(context.Background(), "charge", nil); grants <- g; errs <- err }()
	req := nextParityRequest(t, mt, "waitFor", before)
	parityReply(mt, req, map[string]any{"decision": "allowed", "invocationId": req["invocationId"]})
	grant := <-grants
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	before = len(mt.getSent())
	go func() { _, err := grant.Cancel(context.Background()); errs <- err }()
	req = nextParityRequest(t, mt, "waitFor", before)
	parityReply(mt, req, map[string]any{"decision": "cancelled", "invocationId": grant.InvocationID})
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if _, ok := first.runtime().invocationGrants.Load("charge"); ok {
		t.Fatal("cancelled grant retained by original handle")
	}
	before = len(mt.getSent())
	go func() { _, err := resumed.Step("charge").Success(context.Background()); errs <- err }()
	req = nextParityRequest(t, mt, "recordThreadEvent", before)
	if req["invocationId"] != nil && req["invocationId"] != "" {
		t.Fatalf("reused cancelled invocation: %v", req)
	}
	parityReply(mt, req, map[string]any{"stepId": "step"})
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}
