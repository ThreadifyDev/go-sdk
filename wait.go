package threadify

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	CodeWaitTimeout         = "THREADIFY_WAIT_TIMEOUT"
	CodeInvalidWaitResponse = "THREADIFY_INVALID_WAIT_RESPONSE"
	ActionWaitFor           = "waitFor"
)

// RequestError retains identity needed to inspect or retry an uncertain operation.
type RequestError struct {
	Status         int
	Code           string
	Message        string
	RequestID      string
	StepID         string
	InvocationID   string
	IdempotencyKey string
	Response       map[string]any
	Validation     *WaitResult
	Cause          error
}

func (e *RequestError) Error() string { return e.Message }
func (e *RequestError) Unwrap() error { return e.Cause }

type WaitResult struct {
	Decision     string           `json:"decision"`
	ThreadID     string           `json:"threadId"`
	StepName     string           `json:"stepName"`
	StepID       string           `json:"stepId,omitempty"`
	InvocationID string           `json:"invocationId,omitempty"`
	Message      string           `json:"message,omitempty"`
	Violations   []map[string]any `json:"violations,omitempty"`
}
type ReportOptions struct {
	WaitFor bool
	Timeout time.Duration
}
type PermissionGrant struct {
	WaitResult
	thread *ThreadInstance
}

func uniqueRequestID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func waitTimeout(timeout time.Duration) (time.Duration, error) {
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	if timeout < time.Millisecond || timeout > 5*time.Minute {
		return 0, fmt.Errorf("wait timeout must be between 1ms and 5m")
	}
	return timeout, nil
}
func (c *Connection) correlatedRequest(ctx context.Context, msg map[string]any, timeout time.Duration) (map[string]any, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	timeout, err := waitTimeout(timeout)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, err := uniqueRequestID()
	if err != nil {
		return nil, err
	}
	ch := make(chan map[string]any, 1)
	c.pendingRequests.Store(id, ch)
	defer c.pendingRequests.Delete(id)
	envelope := make(map[string]any, len(msg)+1)
	for k, v := range msg {
		envelope[k] = v
	}
	envelope["requestId"] = id
	if err = c.sendWithContext(ctx, envelope); err != nil {
		return nil, &RequestError{Code: "THREADIFY_CONNECTION_ERROR", Message: err.Error(), RequestID: id, Cause: err}
	}
	cancelWait := func() {
		if !asBool(msg["await"]) && !asBool(msg[ActionWaitFor]) {
			return
		}
		cancelCtx, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer stop()
		_ = c.sendWithContext(cancelCtx, map[string]any{FieldAction: "cancelWait", "targetRequestId": id})
	}
	deadlineError := func() error {
		cancelWait()
		code := CodeWaitTimeout
		if errors.Is(ctx.Err(), context.Canceled) {
			code = "THREADIFY_WAIT_CANCELLED"
		}
		return &RequestError{Code: code, Message: "Wait ended: " + ctx.Err().Error(), RequestID: id, Cause: ctx.Err()}
	}
	select {
	case <-ctx.Done():
		return nil, deadlineError()
	case <-c.stopCh:
		return nil, &RequestError{Code: "THREADIFY_CONNECTION_CLOSED", Message: "Threadify connection closed; operation outcome may be unknown", RequestID: id}
	case response := <-ch:
		// Do not accept an overdue permission when I/O and the timer are both ready.
		deadline, _ := ctx.Deadline()
		if ctx.Err() != nil {
			return nil, deadlineError()
		}
		if !time.Now().Before(deadline) {
			cancelWait()
			return nil, &RequestError{Code: CodeWaitTimeout, Message: "Timed out waiting for Engine response", RequestID: id, Cause: context.DeadlineExceeded}
		}
		if asString(response[FieldStatus]) != StatusSuccess {
			return nil, &RequestError{Code: "THREADIFY_REQUEST_FAILED", Message: firstNonEmpty(asString(response[FieldMessage]), "Request failed"), RequestID: id, Response: response}
		}
		return response, nil
	}
}
func decodeWait(response map[string]any) *WaitResult {
	raw, _ := json.Marshal(response)
	result := &WaitResult{}
	_ = json.Unmarshal(raw, result)
	return result
}
func checkedWait(response map[string]any) (*WaitResult, error) {
	result := decodeWait(response)
	if result.Decision == "" || result.Decision == "pending" {
		return nil, &RequestError{Code: "THREADIFY_SYNC_WAIT_UNSUPPORTED", Message: "Engine did not return a final synchronous wait result"}
	}
	if result.Decision == "timed_out" || result.Decision == StatusCancelled {
		code := CodeWaitTimeout
		cause := context.DeadlineExceeded
		if result.Decision == StatusCancelled {
			code = "THREADIFY_WAIT_CANCELLED"
			cause = context.Canceled
		}
		return nil, &RequestError{Code: code, Message: firstNonEmpty(result.Message, "Wait ended"), StepID: result.StepID, InvocationID: result.InvocationID, Cause: cause}
	}
	return result, nil
}
func checkedValidation(response map[string]any, id string) (*WaitResult, error) {
	result, err := checkedWait(response)
	if err != nil {
		var detail *RequestError
		if errors.As(err, &detail) {
			detail.StepID = id
		}
		return nil, err
	}
	if result.StepID != id {
		return nil, &RequestError{Code: CodeInvalidWaitResponse, Message: "Engine returned a different event", StepID: id}
	}
	if result.Decision != "passed" {
		code := "THREADIFY_VALIDATION_UNAVAILABLE"
		if result.Decision == "violated" {
			code = "THREADIFY_VALIDATION_VIOLATED"
		}
		return nil, &RequestError{Code: code, Message: firstNonEmpty(result.Message, "Validation unavailable"), StepID: id, Validation: result}
	}
	return result, nil
}

// WaitFor waits for permission to execute a single contract invocation.
func (t *ThreadInstance) WaitFor(ctx context.Context, stepName string, opts *WaitOptions) (*PermissionGrant, error) {
	if t == nil {
		return nil, fmt.Errorf("ThreadInstance is nil")
	}
	if err := requireNonEmpty("stepName", stepName); err != nil {
		return nil, err
	}
	var timeout time.Duration
	var id string
	if opts != nil {
		timeout, id = opts.Timeout, opts.InvocationID
	}
	timeout, err := waitTimeout(timeout)
	if err != nil {
		return nil, err
	}
	if id == "" {
		id, err = uniqueRequestID()
		if err != nil {
			return nil, err
		}
	}
	response, err := t.conn.correlatedRequest(ctx, map[string]any{FieldAction: ActionWaitFor, FieldThreadID: t.ThreadID, FieldStepName: stepName, "invocationId": id, "await": true, "timeoutMs": int((timeout + time.Millisecond - 1) / time.Millisecond)}, timeout)
	if err != nil {
		var detail *RequestError
		if errors.As(err, &detail) {
			detail.InvocationID = id
		}
		return nil, err
	}
	result, err := checkedWait(response)
	if err != nil {
		return nil, err
	}
	if result.Decision != "allowed" {
		return nil, &RequestError{Code: "THREADIFY_PERMISSION_DENIED", Message: firstNonEmpty(result.Message, "Permission denied"), InvocationID: id, Response: response}
	}
	if result.InvocationID != id {
		return nil, &RequestError{Code: CodeInvalidWaitResponse, Message: "Engine returned a different invocation", InvocationID: id}
	}
	grant := &PermissionGrant{WaitResult: *result, thread: t}
	t.runtime().invocationGrants.Store(stepName, grant)
	return grant, nil
}
func (t *ThreadInstance) WaitForValidation(ctx context.Context, stepName, stepID string, opts *WaitOptions) (*WaitResult, error) {
	if t == nil {
		return nil, fmt.Errorf("ThreadInstance is nil")
	}
	if err := requireNonEmpty("stepName", stepName); err != nil {
		return nil, err
	}
	if err := requireNonEmpty("stepID", stepID); err != nil {
		return nil, err
	}
	var timeout time.Duration
	if opts != nil {
		timeout = opts.Timeout
	}
	timeout, err := waitTimeout(timeout)
	if err != nil {
		return nil, err
	}
	response, err := t.conn.correlatedRequest(ctx, map[string]any{FieldAction: ActionWaitFor, FieldThreadID: t.ThreadID, FieldStepName: stepName, FieldStepID: stepID, "await": true, "timeoutMs": int((timeout + time.Millisecond - 1) / time.Millisecond)}, timeout)
	if err != nil {
		var detail *RequestError
		if errors.As(err, &detail) {
			detail.StepID = stepID
		}
		return nil, err
	}
	return checkedValidation(response, stepID)
}
func (g *PermissionGrant) Cancel(ctx context.Context) (*WaitResult, error) {
	if g == nil || g.thread == nil {
		return nil, fmt.Errorf("PermissionGrant is nil")
	}
	response, err := g.thread.conn.correlatedRequest(ctx, map[string]any{FieldAction: ActionWaitFor, FieldThreadID: g.ThreadID, FieldStepName: g.StepName, "invocationId": g.InvocationID, "cancel": true}, 10*time.Second)
	if err != nil {
		return nil, err
	}
	result := decodeWait(response)
	if result.Decision != StatusCancelled || result.InvocationID != g.InvocationID {
		return nil, &RequestError{Code: "THREADIFY_PERMISSION_DENIED", Message: "Engine did not cancel this invocation", Response: response}
	}
	g.thread.runtime().invocationGrants.CompareAndDelete(g.StepName, g)
	return result, nil
}

func httpRequestError(status int, message string) *RequestError {
	code := "THREADIFY_HTTP_ERROR"
	switch status {
	case 429:
		code = "THREADIFY_ALLOWANCE_EXCEEDED"
	case 503:
		code = "THREADIFY_LICENSE_UNAVAILABLE"
	}
	return &RequestError{Code: code, Message: message, Status: status}
}
