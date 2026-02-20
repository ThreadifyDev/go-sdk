package threadify

import (
	"context"
	"testing"
	"time"
)

func TestThreadInstance_WaitFor_SuccessNotification(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	thread := &ThreadInstance{
		ThreadID: "thread-wait-123",
		conn:     conn,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go func() {
		time.Sleep(50 * time.Millisecond)
		notif := &Notification{
			ThreadID:         "thread-wait-123",
			StepName:         "order_placed",
			StepStatus:       StatusSuccess,
			Source:           "execution",
			NotificationType: "execution.success",
		}
		thread.handleNotification(notif)
	}()

	notif, err := thread.WaitFor(ctx, "order_placed", nil)
	if err != nil {
		t.Fatalf("WaitFor() error: %v", err)
	}

	if notif.StepName != "order_placed" {
		t.Errorf("expected step 'order_placed', got %q", notif.StepName)
	}
}

func TestThreadInstance_End_Statuses(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	tests := []struct {
		name     string
		action   func(ctx context.Context, tr *ThreadInstance) error
		wantStat string
	}{
		{
			"End",
			func(ctx context.Context, tr *ThreadInstance) error {
				_, err := tr.End(ctx, StatusCompleted, "done")
				return err
			},
			StatusCompleted,
		},
		{
			"Complete",
			func(ctx context.Context, tr *ThreadInstance) error {
				_, err := tr.Complete(ctx, "finished")
				return err
			},
			StatusCompleted,
		},
		{
			"Close",
			func(ctx context.Context, tr *ThreadInstance) error {
				_, err := tr.Close(ctx, "aborted")
				return err
			},
			StatusCancelled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt.enqueueResponse(map[string]any{
				"action":       ActionThreadEnd,
				"status":       StatusSuccess,
				"threadStatus": tt.wantStat,
			})

			thread := &ThreadInstance{
				ThreadID: "thread-end-test",
				conn:     conn,
			}

			ctx := context.Background()
			err := tt.action(ctx, thread)
			if err != nil {
				t.Fatalf("%s() error: %v", tt.name, err)
			}

			sent := mt.getSent()
			msg := sent[len(sent)-1]
			if msg["action"] != ActionThreadEnd {
				t.Errorf("expected action 'endThread', got %v", msg["action"])
			}
			if msg["status"] != tt.wantStat {
				t.Errorf("expected status %v, got %v", tt.wantStat, msg["status"])
			}
		})
	}
}

func TestThreadInstance_Cleanup(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	thread := &ThreadInstance{
		ThreadID: "thread-cleanup-123",
		conn:     conn,
	}

	// Add a pending wait to verify it gets cancelled.
	ch := make(chan *Notification, 1)
	pw := &pendingWait{
		ch:     ch,
		cancel: func() {},
	}
	thread.pendingWaits.Store("some-step", pw)
	conn.threads.Store(thread.ThreadID, thread)

	thread.cleanup()

	// Verify pendingWaits is empty.
	count := 0
	thread.pendingWaits.Range(func(_, _ any) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 pending waits, got %d", count)
	}

	// Verify thread is removed from connection.
	_, ok := conn.threads.Load(thread.ThreadID)
	if ok {
		t.Error("expected thread to be removed from connection")
	}
}
