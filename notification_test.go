package threadify

import (
	"testing"
	"time"
)

const (
	testOrderPlaced = "order_placed"
	testOrderFlow   = "order_flow"
)

func TestNotification_NewNotification(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	data := map[string]any{
		"notificationId":   "notif-001",
		"threadId":         "thread-123",
		"stepId":           "step-456",
		"stepName":         testOrderPlaced,
		"contractName":     testOrderFlow,
		"status":           "violated",
		"stepStatus":       "success",
		"severity":         "critical",
		"message":          "Missing required field",
		"details":          map[string]any{"field": "amount"},
		"timestamp":        "2026-02-18T12:00:00Z",
		"violationType":    "required_field",
		"ownerId":          "owner-789",
		"source":           "rule",
		"notificationType": "rule.violated",
	}

	notif := NewNotification(data, conn, "ack-token-1")

	if notif.NotificationID != "notif-001" {
		t.Errorf("expected NotificationID 'notif-001', got %q", notif.NotificationID)
	}
	if notif.ThreadID != "thread-123" {
		t.Errorf("expected ThreadID 'thread-123', got %q", notif.ThreadID)
	}
	if notif.StepName != testOrderPlaced {
		t.Errorf("expected StepName %q, got %q", testOrderPlaced, notif.StepName)
	}
	if notif.ContractName != testOrderFlow {
		t.Errorf("expected ContractName %q, got %q", testOrderFlow, notif.ContractName)
	}
	if notif.Severity != "critical" {
		t.Errorf("expected severity 'critical', got %q", notif.Severity)
	}
	if notif.Source != "rule" {
		t.Errorf("expected source 'rule', got %q", notif.Source)
	}
}

func TestNotification_TimestampParsing(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	t.Run("valid timestamp", func(t *testing.T) {
		notif := NewNotification(map[string]any{
			"notificationId": "n1",
			"timestamp":      "2026-02-18T12:00:00Z",
		}, conn, "")

		expected, _ := time.Parse(time.RFC3339, "2026-02-18T12:00:00Z")
		if !notif.Timestamp.Equal(expected) {
			t.Errorf("expected timestamp %v, got %v", expected, notif.Timestamp)
		}
	})

	t.Run("invalid timestamp falls back to now", func(t *testing.T) {
		before := time.Now().Add(-time.Second)
		notif := NewNotification(map[string]any{
			"notificationId": "n2",
			"timestamp":      "not-a-date",
		}, conn, "")
		after := time.Now().Add(time.Second)

		if notif.Timestamp.Before(before) || notif.Timestamp.After(after) {
			t.Errorf("expected timestamp near now, got %v", notif.Timestamp)
		}
	})

	t.Run("missing timestamp falls back to now", func(t *testing.T) {
		before := time.Now().Add(-time.Second)
		notif := NewNotification(map[string]any{
			"notificationId": "n3",
		}, conn, "")
		after := time.Now().Add(time.Second)

		if notif.Timestamp.Before(before) || notif.Timestamp.After(after) {
			t.Errorf("expected timestamp near now, got %v", notif.Timestamp)
		}
	})
}

func TestNotification_Ack(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	notif := NewNotification(map[string]any{
		"notificationId": "notif-ack-001",
		"threadId":       "thread-123",
	}, conn, "ack-token-abc")

	// First ACK should succeed.
	err := notif.Ack()
	if err != nil {
		t.Fatalf("Ack() error: %v", err)
	}

	if !notif.IsAcknowledged() {
		t.Error("expected IsAcknowledged to be true after Ack()")
	}

	// Verify the ACK message was sent.
	sent := mt.getSent()
	var ackMsg map[string]any
	for _, msg := range sent {
		if asString(msg["action"]) == "ack_notification" {
			ackMsg = msg
			break
		}
	}

	if ackMsg == nil {
		t.Fatal("expected ack_notification message to be sent")
	}
	if ackMsg["notification_id"] != "notif-ack-001" {
		t.Errorf("expected notification_id 'notif-ack-001', got %v", ackMsg["notification_id"])
	}
	if ackMsg["ackToken"] != "ack-token-abc" {
		t.Errorf("expected ackToken 'ack-token-abc', got %v", ackMsg["ackToken"])
	}
}

func TestNotification_Ack_Idempotent(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	notif := NewNotification(map[string]any{
		"notificationId": "notif-ack-002",
		"threadId":       "thread-123",
	}, conn, "ack-token-xyz")

	// ACK twice — both should succeed.
	_ = notif.Ack()
	_ = notif.Ack()

	// Only one ACK message should be sent.
	sent := mt.getSent()
	ackCount := 0
	for _, msg := range sent {
		if asString(msg["action"]) == "ack_notification" {
			ackCount++
		}
	}

	if ackCount != 1 {
		t.Errorf("expected 1 ACK sent (idempotent), got %d", ackCount)
	}
}

func TestNotification_Ack_MissingToken(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	notif := NewNotification(map[string]any{
		"notificationId": "notif-no-token",
		"threadId":       "thread-123",
	}, conn, "") // empty ack token

	err := notif.Ack()
	if err == nil {
		t.Error("expected error for missing ackToken")
	}
}

func TestNotification_StatusHelpers(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	tests := []struct {
		name     string
		data     map[string]any
		violated bool
		passed   bool
	}{
		{"violated", map[string]any{"notificationId": "n1", "status": "violated"}, true, false},
		{"passed", map[string]any{"notificationId": "n2", "status": "passed"}, false, true},
		{"none", map[string]any{"notificationId": "n3", "status": "none"}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notif := NewNotification(tt.data, conn, "")
			if notif.IsViolated() != tt.violated {
				t.Errorf("IsViolated() = %v, want %v", notif.IsViolated(), tt.violated)
			}
			if notif.IsPassed() != tt.passed {
				t.Errorf("IsPassed() = %v, want %v", notif.IsPassed(), tt.passed)
			}
		})
	}
}

func TestNotification_SeverityHelpers(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	tests := []struct {
		name     string
		severity string
		critical bool
		warning  bool
		info     bool
	}{
		{"critical", "critical", true, false, false},
		{"warning", "warning", false, true, false},
		{"info", "info", false, false, true},
		{"unknown", "unknown", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notif := NewNotification(map[string]any{
				"notificationId": "n-" + tt.name,
				"severity":       tt.severity,
			}, conn, "")

			if notif.IsCritical() != tt.critical {
				t.Errorf("IsCritical() = %v, want %v", notif.IsCritical(), tt.critical)
			}
			if notif.IsWarning() != tt.warning {
				t.Errorf("IsWarning() = %v, want %v", notif.IsWarning(), tt.warning)
			}
			if notif.IsInfo() != tt.info {
				t.Errorf("IsInfo() = %v, want %v", notif.IsInfo(), tt.info)
			}
		})
	}
}

func TestNotification_StepStatusHelpers(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	tests := []struct {
		name    string
		status  string
		success bool
		failed  bool
		isError bool
	}{
		{"success", "success", true, false, false},
		{"failed", "failed", false, true, false},
		{"error", "error", false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notif := NewNotification(map[string]any{
				"notificationId": "n-step-" + tt.name,
				"stepStatus":     tt.status,
			}, conn, "")

			if notif.IsSuccess() != tt.success {
				t.Errorf("IsSuccess() = %v, want %v", notif.IsSuccess(), tt.success)
			}
			if notif.IsFailed() != tt.failed {
				t.Errorf("IsFailed() = %v, want %v", notif.IsFailed(), tt.failed)
			}
			if notif.IsError() != tt.isError {
				t.Errorf("IsError() = %v, want %v", notif.IsError(), tt.isError)
			}
		})
	}
}

func TestNotification_String(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	notif := NewNotification(map[string]any{
		"notificationId": "n-str",
		"stepName":       "order_placed",
		"severity":       "critical",
		"message":        "Missing amount",
	}, conn, "")

	str := notif.String()
	if str == "" {
		t.Error("String() returned empty")
	}
}

func TestNotification_ToMap(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	notif := NewNotification(map[string]any{
		"notificationId": "n-map",
		"threadId":       "t-1",
		"stepName":       "order_placed",
		"severity":       "warning",
		"message":        "Test",
	}, conn, "ack-123")

	m := notif.ToMap()
	if m["notificationId"] != "n-map" {
		t.Errorf("expected notificationId 'n-map', got %v", m["notificationId"])
	}
	if m["threadId"] != "t-1" {
		t.Errorf("expected threadId 't-1', got %v", m["threadId"])
	}
	if m["acknowledged"] != false {
		t.Errorf("expected acknowledged false, got %v", m["acknowledged"])
	}

	_ = notif.Ack()
	m = notif.ToMap()
	if m["acknowledged"] != true {
		t.Errorf("expected acknowledged true after Ack(), got %v", m["acknowledged"])
	}
}
