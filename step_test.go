package threadify

import (
	"context"
	"testing"
	"time"
)

const testOrderID = "ORD-123"

func TestThreadStep_FluentChaining(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	// Start a thread.
	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-step-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("order_placed")
	if step.err != nil {
		t.Fatalf("Step() error: %v", step.err)
	}

	// Chain methods.
	stepBuilt := step.
		AddContext(map[string]any{"orderId": "ORD-123", "amount": 99.99}).
		AddRefs(map[string]string{"stripe_id": "pi_abc"}).
		SubStep("validate_inventory", map[string]any{"items": 5})

	// Verify chaining returned the same step.
	if stepBuilt != step {
		t.Error("expected chaining to return same step instance")
	}

	// Verify context.
	stepCtx := step.GetContext()
	if stepCtx["orderId"] != testOrderID {
		t.Errorf("expected orderId %q, got %q", testOrderID, stepCtx["orderId"])
	}
	if stepCtx["amount"] != "99.99" {
		t.Errorf("expected amount '99.99', got %q", stepCtx["amount"])
	}

	// Verify refs.
	if step.refs["stripe_id"] != "pi_abc" {
		t.Errorf("expected ref stripe_id 'pi_abc', got %q", step.refs["stripe_id"])
	}

	// Verify sub-steps.
	if len(step.subSteps) != 1 {
		t.Fatalf("expected 1 sub-step, got %d", len(step.subSteps))
	}
	if step.subSteps[0].Name != "validate_inventory" {
		t.Errorf("expected sub-step name 'validate_inventory', got %q", step.subSteps[0].Name)
	}
	if step.subSteps[0].Status != StatusSuccess {
		t.Errorf("expected sub-step status 'success', got %q", step.subSteps[0].Status)
	}
}

func TestThreadStep_ManualIdempotencyKey(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-idemp-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("payment_processed")
	step.IdempotencyKey("custom-key-123")

	key := step.generateIdempotencyKey()
	if key != "custom-key-123" {
		t.Errorf("expected manual key 'custom-key-123', got %q", key)
	}
}

func TestThreadStep_AutoIdempotencyKey(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-auto-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	// Same step name + same context should produce same key.
	step1 := thread.Step("order_placed")
	step1.AddContext(map[string]any{"orderId": testOrderID})
	key1 := step1.generateIdempotencyKey()

	step2 := thread.Step("order_placed")
	step2.AddContext(map[string]any{"orderId": testOrderID})
	key2 := step2.generateIdempotencyKey()

	if key1 != key2 {
		t.Errorf("same step+context should produce same key: %q vs %q", key1, key2)
	}

	// Different context should produce different key.
	step3 := thread.Step("order_placed")
	step3.AddContext(map[string]any{"orderId": "ORD-456"})
	key3 := step3.generateIdempotencyKey()

	if key1 == key3 {
		t.Errorf("different context should produce different key: %q vs %q", key1, key3)
	}

	// Key should be 8 hex characters.
	if len(key1) != 8 {
		t.Errorf("expected 8-char hex key, got %d chars: %q", len(key1), key1)
	}
}

func TestThreadStep_Success(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-success-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("order_placed")
	step.AddContext(map[string]any{"orderId": testOrderID})

	// Enqueue recordThreadEvent response.
	mt.enqueueResponse(map[string]any{
		"action": "recordThreadEvent",
		"status": "success",
	})

	result, err := step.Success(ctx, "Order placed successfully")
	if err != nil {
		t.Fatalf("Success() error: %v", err)
	}

	if result.StepName != "order_placed" {
		t.Errorf("expected stepName 'order_placed', got %q", result.StepName)
	}
	if result.ThreadID != "thread-success-001" {
		t.Errorf("expected threadId 'thread-success-001', got %q", result.ThreadID)
	}
	if result.Status != StatusSuccess {
		t.Errorf("expected status 'success', got %q", result.Status)
	}
	if result.Duplicate {
		t.Error("expected non-duplicate result")
	}
}

func TestThreadStep_Failed(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-failed-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("payment_processed")

	mt.enqueueResponse(map[string]any{
		"action": "recordThreadEvent",
		"status": "success",
	})

	result, err := step.Failed(ctx, "Payment gateway timeout")
	if err != nil {
		t.Fatalf("Failed() error: %v", err)
	}

	if result.Status != StatusFailed {
		t.Errorf("expected status 'failed', got %q", result.Status)
	}
}

func TestThreadStep_Error(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-error-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("service_call")

	mt.enqueueResponse(map[string]any{
		"action": "recordThreadEvent",
		"status": "success",
	})

	result, err := step.Error(ctx, "Service unavailable")
	if err != nil {
		t.Fatalf("Error() error: %v", err)
	}

	if result.Status != StatusError {
		t.Errorf("expected status 'error', got %q", result.Status)
	}
}

func TestThreadStep_DuplicateDetection(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-dup-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("order_placed")

	mt.enqueueResponse(map[string]any{
		"action":      "recordThreadEvent",
		"status":      "error",
		"isDuplicate": true,
		"message":     "Duplicate step detected",
	})

	result, err := step.Success(ctx)
	if err == nil {
		t.Fatal("expected duplicate error")
	}

	if !IsDuplicateError(err) {
		t.Errorf("expected IsDuplicateError to be true, got false")
	}

	if result == nil {
		t.Fatal("expected result even for duplicate")
	}

	if !result.Duplicate {
		t.Error("expected duplicate=true on result")
	}
}

func TestThreadStep_EmptyStepName(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-empty-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("")
	if step.err == nil {
		t.Error("expected error for empty step name")
	}
}

func TestThreadStep_SubStepStatuses(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-substep-001",
	})

	ctx := context.Background()
	thread, err := conn.Start(ctx, "", "")
	if err != nil {
		t.Fatalf("Start() error: %v", err)
	}

	step := thread.Step("process_order")
	step.SubStep("validate", nil) // default success
	step.SubStep("calculate_tax", map[string]any{"amount": 12.50}, "success")
	step.SubStep("apply_discount", map[string]any{"error": "Invalid coupon"}, "failed")

	if len(step.subSteps) != 3 {
		t.Fatalf("expected 3 sub-steps, got %d", len(step.subSteps))
	}

	if step.subSteps[0].Status != StatusSuccess {
		t.Errorf("expected default sub-step status 'success', got %q", step.subSteps[0].Status)
	}
	if step.subSteps[2].Status != StatusFailed {
		t.Errorf("expected sub-step status 'failed', got %q", step.subSteps[2].Status)
	}
}

func TestThreadStep_SubStepInvalidStatus(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-substep-inv-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")
	step := thread.Step("process")

	// Should not panic, but store error.
	step.SubStep("bad", nil, "invalid_status")

	// Trigger send to check error.
	_, err := step.Success(ctx)
	if err == nil {
		t.Error("expected error for invalid sub-step status")
	}
	if err.Error() != `sub-step status must be either "success" or "failed"` {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestThreadStep_IdempotencyKeyErrorOnEmpty(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-panic-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")
	step := thread.Step("test")

	// Should not panic, but store error.
	step.IdempotencyKey("")

	_, err := step.Success(ctx)
	if err == nil {
		t.Error("expected error for empty idempotency key")
	}
	if err.Error() != "idempotency key must be a non-empty string" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestThreadStep_PrivateContext(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-private-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")
	step := thread.Step("payment")

	step.AddPrivateContext(map[string]any{"cardNumber": "4111111111111111"})

	stepCtx := step.GetContext()
	if stepCtx["cardNumber"] != "4111111111111111" {
		t.Errorf("expected cardNumber in context")
	}
	if stepCtx["private_cardNumber"] != "4111111111111111" {
		t.Errorf("expected private_cardNumber in context")
	}
}

func TestThreadStep_MessageAsMapData(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-mapdata-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")
	step := thread.Step("order_placed")

	mt.enqueueResponse(map[string]any{
		"action": "recordThreadEvent",
		"status": "success",
	})

	result, err := step.Success(ctx, map[string]any{
		"message": "Order placed",
		"orderId": "ORD-123",
	})
	if err != nil {
		t.Fatalf("Success() error: %v", err)
	}

	if result.Status != StatusSuccess {
		t.Errorf("expected status 'success', got %q", result.Status)
	}
}

func TestThreadInstance_InviteParty(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-invite-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	mt.enqueueResponse(map[string]any{
		"action":      "inviteParty",
		"status":      "success",
		"threadToken": "jwt-token-here",
		"role":        "supplier",
		"accessLevel": "external",
		"expiresAt":   "2026-03-01T00:00:00Z",
	})

	resp, err := thread.InviteParty(ctx, InviteOptions{Role: "supplier"})
	if err != nil {
		t.Fatalf("InviteParty() error: %v", err)
	}

	if resp.Token != "jwt-token-here" {
		t.Errorf("expected token 'jwt-token-here', got %q", resp.Token)
	}
	if resp.Role != "supplier" {
		t.Errorf("expected role 'supplier', got %q", resp.Role)
	}
}

func TestThreadInstance_InviteParty_MissingRole(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-invite-002",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	_, err := thread.InviteParty(ctx, InviteOptions{})
	if err == nil {
		t.Error("expected error for missing role")
	}
}

func TestThreadInstance_AddRefs(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-refs-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	mt.enqueueResponse(map[string]any{
		"action": "addRefs",
		"status": "success",
	})

	err := thread.AddRefs(ctx, map[string]string{
		"orderId":    "ORD-123",
		"customerId": "CUST-456",
	})
	if err != nil {
		t.Fatalf("AddRefs() error: %v", err)
	}

	if thread.Refs["orderId"] != "ORD-123" {
		t.Errorf("expected ref orderId 'ORD-123', got %q", thread.Refs["orderId"])
	}
}

func TestThreadInstance_AddRefs_Empty(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-refs-002",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	err := thread.AddRefs(ctx, map[string]string{})
	if err == nil {
		t.Error("expected error for empty refs")
	}
}

func TestThreadInstance_LinkThread(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-link-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	mt.enqueueResponse(map[string]any{
		"action": "addRefs",
		"status": "success",
	})

	validUUID := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	err := thread.LinkThread(ctx, validUUID, "parent")
	if err != nil {
		t.Fatalf("LinkThread() error: %v", err)
	}

	if thread.Refs["linkedThread:parent"] != validUUID {
		t.Errorf("expected linked thread ref, got %v", thread.Refs)
	}
}

func TestThreadInstance_LinkThread_InvalidUUID(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-link-002",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	err := thread.LinkThread(ctx, "not-a-uuid", "parent")
	if err == nil {
		t.Error("expected error for invalid UUID format")
	}
}

func TestThreadInstance_Cancel(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-end-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	mt.enqueueResponse(map[string]any{
		"action":       "threadEnd",
		"status":       "success",
		"threadStatus": "cancelled",
		"cancelledAt":  "2026-02-18T12:00:00Z",
		"message":      "Thread cancelled",
	})

	resp, err := thread.Cancel(ctx, "User requested cancellation")
	if err != nil {
		t.Fatalf("Cancel() error: %v", err)
	}

	if resp.Status != "cancelled" {
		t.Errorf("expected status 'cancelled', got %q", resp.Status)
	}
	if resp.EndedAt != "2026-02-18T12:00:00Z" {
		t.Errorf("expected endedAt, got %q", resp.EndedAt)
	}
}

func TestThreadInstance_Complete(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-complete-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	mt.enqueueResponse(map[string]any{
		"action":       "threadEnd",
		"status":       "success",
		"threadStatus": "completed",
		"completedAt":  "2026-02-18T12:00:00Z",
		"message":      "Thread completed",
	})

	resp, err := thread.Complete(ctx, "All steps done")
	if err != nil {
		t.Fatalf("Complete() error: %v", err)
	}

	if resp.Status != "completed" {
		t.Errorf("expected status 'completed', got %q", resp.Status)
	}
}

func TestThreadInstance_WaitFor_Timeout(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-wait-001",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	_, err := thread.WaitFor(ctx, "some_step", &WaitOptions{
		Timeout: 100 * time.Millisecond,
	})
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestThreadInstance_WaitFor_EmptyStepName(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	mt.enqueueResponse(map[string]any{
		"action":   "startThread",
		"status":   "success",
		"threadId": "thread-wait-002",
	})

	ctx := context.Background()
	thread, _ := conn.Start(ctx, "", "")

	_, err := thread.WaitFor(ctx, "", nil)
	if err == nil {
		t.Error("expected error for empty step name")
	}
}
