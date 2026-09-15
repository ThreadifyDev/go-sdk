package threadify

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func nextParityRequest(t *testing.T, mt *mockTransport, action string, after int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		messages := mt.getSent()
		for i := after; i < len(messages); i++ {
			if asString(messages[i][FieldAction]) == action {
				return messages[i]
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("missing %s request", action)
	return nil
}
func parityReply(mt *mockTransport, request map[string]any, fields map[string]any) {
	response := map[string]any{"action": request[FieldAction], "requestId": request["requestId"], "status": "success", "threadId": request[FieldThreadID], "stepName": request[FieldStepName]}
	for k, v := range fields {
		response[k] = v
	}
	mt.enqueueResponse(response)
}
func TestParityEngineURL(t *testing.T) {
	ws, gql, err := engineEndpoints("https://example.com/proxy/threads/customer/")
	if err != nil || ws != "wss://example.com/proxy/threads/customer/threads" || gql != "https://example.com/proxy/threads/customer/graphql" {
		t.Fatalf("%s %s %v", ws, gql, err)
	}
	for _, base := range []string{"relative", "ftp://host", "http://user:pass@host", "https://host?", "https://host#", "http://bad host"} {
		if _, _, err := engineEndpoints(base); err == nil {
			t.Fatalf("accepted %s", base)
		}
	}
	mt := newMockTransport()
	mt.enqueueResponse(map[string]any{"action": "connect", "status": "success"})
	conn, err := Connect(context.Background(), "test", WithEngineURL("http://engine/proxy"), WithDialer(&mockDialer{transport: mt}))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.graphqlURL != "http://engine/proxy/graphql" {
		t.Fatal(conn.graphqlURL)
	}
	if _, err = Connect(context.Background(), "test", WithEngineURL("http://engine"), WithWSURL("ws://other")); err == nil {
		t.Fatal("accepted mixed URLs")
	}
}
func TestParityReferenceMap(t *testing.T) {
	q, err := referenceQuery(map[string]string{"order_id": "ORD-1001"}, RefQuery{Limit: 2, Status: "active"})
	if err != nil || q.RefKey != "order_id" || q.RefValue != "ORD-1001" || q.Limit != 2 || q.Status != "active" {
		t.Fatalf("%+v %v", q, err)
	}
	for _, refs := range []any{nil, map[string]string{}, map[string]string{"a": "b", "c": "d"}, map[string]string{"a": ""}} {
		if _, err := referenceQuery(refs); err == nil {
			t.Fatalf("accepted %+v", refs)
		}
	}
}
func TestParityPermissionAndValidation(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	thread := newThreadInstance(conn, "thread-1", "", "", "", nil)
	grants := make(chan *PermissionGrant, 1)
	errs := make(chan error, 1)
	go func() { g, e := thread.WaitFor(context.Background(), "charge", nil); grants <- g; errs <- e }()
	req := nextParityRequest(t, mt, "waitFor", 0)
	parityReply(mt, map[string]any{"action": "waitFor", "requestId": "wrong"}, map[string]any{"decision": "allowed"})
	mt.enqueueResponse(map[string]any{"action": "waitFor", "status": "success", "decision": "allowed"})
	select {
	case <-grants:
		t.Fatal("accepted uncorrelated permission")
	case <-time.After(10 * time.Millisecond):
	}
	parityReply(mt, req, map[string]any{"decision": "allowed", "invocationId": req["invocationId"]})
	grant := <-grants
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Second)
	step := thread.Step("charge").RecordedTimes(start, end)
	results := make(chan *StepResult, 1)
	go func() {
		r, e := step.Success(context.Background(), "done", ReportOptions{WaitFor: true})
		results <- r
		errs <- e
	}()
	req = nextParityRequest(t, mt, "recordThreadEvent", 0)
	if req["invocationId"] != grant.InvocationID || req[FieldIdempotencyKey] != grant.InvocationID || req[FieldFinishedAt] != end.Format(time.RFC3339Nano) || !asBool(req["waitFor"]) {
		t.Fatalf("bad report %+v", req)
	}
	parityReply(mt, req, map[string]any{"stepId": "event-1", "validation": map[string]any{"stepId": "event-1", "decision": "passed"}})
	result := <-results
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if result.StepID != "event-1" || result.Validation.Decision != "passed" {
		t.Fatalf("%+v", result)
	}
}
func TestParityConcurrentWaits(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	thread := newThreadInstance(conn, "thread", "", "", "", nil)
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { _, e := thread.WaitFor(context.Background(), "first", nil); first <- e }()
	one := nextParityRequest(t, mt, "waitFor", 0)
	before := len(mt.getSent())
	go func() { _, e := thread.WaitFor(context.Background(), "second", nil); second <- e }()
	two := nextParityRequest(t, mt, "waitFor", before)
	parityReply(mt, two, map[string]any{"decision": "allowed", "invocationId": two["invocationId"]})
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	select {
	case <-first:
		t.Fatal("first completed early")
	default:
	}
	parityReply(mt, one, map[string]any{"decision": "allowed", "invocationId": one["invocationId"]})
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}
func TestParityWaitCancellationAndDisconnect(t *testing.T) {
	for _, mode := range []string{"timeout", "cancel", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			conn, mt := newTestConnection(t)
			defer conn.Close()
			thread := newThreadInstance(conn, "thread", "", "", "", nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := &WaitOptions{Timeout: time.Second}
			if mode == "timeout" {
				opts.Timeout = 20 * time.Millisecond
			}
			errs := make(chan error, 1)
			go func() { _, err := thread.WaitFor(ctx, "blocked", opts); errs <- err }()
			req := nextParityRequest(t, mt, "waitFor", 0)
			if mode == "cancel" {
				cancel()
			}
			if mode == "disconnect" {
				_ = mt.Close()
			}
			select {
			case err := <-errs:
				var detail *RequestError
				if !errors.As(err, &detail) {
					t.Fatalf("missing structured error: %v", err)
				}
				if detail.InvocationID != req["invocationId"] {
					t.Fatal("missing invocation")
				}
				if mode != "disconnect" {
					cancelled := nextParityRequest(t, mt, "cancelWait", 0)
					if cancelled["targetRequestId"] != req["requestId"] {
						t.Fatal("wrong cancellation")
					}
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("wait did not reject promptly")
			}
			count := 0
			conn.pendingRequests.Range(func(_, _ any) bool { count++; return true })
			if count != 0 {
				t.Fatal("leaked request")
			}
		})
	}
}
func TestParityValidationRejectsWrongOrNonfinal(t *testing.T) {
	for _, decision := range []string{"pending", "violated", "unvalidated", "passed"} {
		response := map[string]any{"decision": decision, "stepId": "event"}
		if decision == "passed" {
			response["stepId"] = "wrong"
		}
		_, err := checkedValidation(response, "event")
		var detail *RequestError
		if !errors.As(err, &detail) || detail.StepID != "event" {
			t.Fatalf("%s: %v", decision, err)
		}
	}
}
func TestParityDisconnectRejectsOrdinaryRequest(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	errs := make(chan error, 1)
	go func() { _, err := conn.Start(context.Background(), "order"); errs <- err }()
	nextParityRequest(t, mt, "startThread", 0)
	_ = mt.Close()
	select {
	case err := <-errs:
		if err == nil {
			t.Fatal("missing error")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("pending request did not reject")
	}
}

func TestParityWrongInvocationAndDuplicateValidation(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	thread := newThreadInstance(conn, "thread", "", "", "", nil)
	errs := make(chan error, 1)
	go func() { _, err := thread.WaitFor(context.Background(), "charge", nil); errs <- err }()
	req := nextParityRequest(t, mt, "waitFor", 0)
	parityReply(mt, req, map[string]any{"decision": "allowed", "invocationId": "different"})
	var detail *RequestError
	if err := <-errs; !errors.As(err, &detail) || detail.Code != "THREADIFY_INVALID_WAIT_RESPONSE" {
		t.Fatalf("%v", err)
	}
	step := thread.Step("charge")
	go func() { _, err := step.Success(context.Background(), ReportOptions{WaitFor: true}); errs <- err }()
	req = nextParityRequest(t, mt, "recordThreadEvent", 0)
	parityReply(mt, req, map[string]any{"status": "error", "isDuplicate": true, "message": "duplicate"})
	if err := <-errs; !errors.As(err, &detail) || detail.Code != "THREADIFY_REQUEST_FAILED" || detail.IdempotencyKey != req[FieldIdempotencyKey] {
		t.Fatalf("%v", err)
	}
}

func TestParityGrantCancel(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer conn.Close()
	thread := newThreadInstance(conn, "thread", "", "", "", nil)
	grants := make(chan *PermissionGrant, 1)
	errs := make(chan error, 1)
	go func() { g, e := thread.WaitFor(context.Background(), "charge", nil); grants <- g; errs <- e }()
	req := nextParityRequest(t, mt, "waitFor", 0)
	parityReply(mt, req, map[string]any{"decision": "allowed", "invocationId": req["invocationId"]})
	grant := <-grants
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	before := len(mt.getSent())
	go func() { _, err := grant.Cancel(context.Background()); errs <- err }()
	req = nextParityRequest(t, mt, "waitFor", before)
	if !asBool(req["cancel"]) || req["invocationId"] != grant.InvocationID {
		t.Fatal(req)
	}
	parityReply(mt, req, map[string]any{"decision": "cancelled", "invocationId": grant.InvocationID})
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if _, ok := thread.invocationGrants.Load("charge"); ok {
		t.Fatal("cancelled grant retained")
	}
}

func TestParityHTTPPolicyErrors(t *testing.T) {
	for status, code := range map[int]string{429: "THREADIFY_ALLOWANCE_EXCEEDED", 503: "THREADIFY_LICENSE_UNAVAILABLE", 401: "THREADIFY_HTTP_ERROR"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
		_, err := Connect(context.Background(), "fixture-key", WithEngineURL(server.URL))
		var detail *RequestError
		if !errors.As(err, &detail) || detail.Status != status || detail.Code != code {
			t.Fatalf("%d: %v", status, err)
		}
		client := newGraphQLClient(server.URL, "fixture-key", time.Second)
		_, err = client.query(context.Background(), "query { threads { id } }", nil)
		if !errors.As(err, &detail) || detail.Status != status || detail.Code != code {
			t.Fatalf("GraphQL %d: %v", status, err)
		}
		server.Close()
	}
}
