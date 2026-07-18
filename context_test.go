package threadify

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestValidateContext(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if err := validateContext(nil); err == nil || err.Error() != "context cannot be nil" {
			t.Fatalf("validateContext(nil) error = %v, want context cannot be nil", err)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := validateContext(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("validateContext(cancelled) error = %v, want context.Canceled", err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		if err := validateContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("validateContext(expired) error = %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("active", func(t *testing.T) {
		if err := validateContext(context.Background()); err != nil {
			t.Fatalf("validateContext(active) error = %v, want nil", err)
		}
	})
}

func TestConnectionStart_ExpiredContextDoesNotSend(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	sentBefore := len(mt.getSent())
	thread, err := conn.Start(ctx, "expired")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want context.DeadlineExceeded", err)
	}
	if thread != nil {
		t.Fatalf("Start() thread = %#v, want nil", thread)
	}
	if sentAfter := len(mt.getSent()); sentAfter != sentBefore {
		t.Fatalf("sent message count = %d, want %d", sentAfter, sentBefore)
	}
}

func TestThreadStep_ExpiredContextDoesNotSend(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()

	thread := newThreadInstance(conn, "thread-expired", "", "", "", nil)
	step := thread.Step("expired-step").AddContext(map[string]any{"key": "value"})

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	sentBefore := len(mt.getSent())
	result, err := step.Success(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Success() error = %v, want context.DeadlineExceeded", err)
	}
	if result != nil {
		t.Fatalf("Success() result = %#v, want nil", result)
	}
	if sentAfter := len(mt.getSent()); sentAfter != sentBefore {
		t.Fatalf("sent message count = %d, want %d", sentAfter, sentBefore)
	}
}

func TestConnectionStart_UsesSDKRequestTimeout(t *testing.T) {
	conn, mt := newTestConnection(t)
	defer func() { _ = conn.Close() }()
	conn.requestTimeout = 20 * time.Millisecond

	sentBefore := len(mt.getSent())
	startedAt := time.Now()
	thread, err := conn.Start(context.Background(), "sdk-timeout")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want context.DeadlineExceeded", err)
	}
	if thread != nil {
		t.Fatalf("Start() thread = %#v, want nil", thread)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("Start() elapsed = %v, SDK request timeout was not applied", elapsed)
	}
	if sentAfter := len(mt.getSent()); sentAfter != sentBefore+1 {
		t.Fatalf("sent message count = %d, want %d", sentAfter, sentBefore+1)
	}
}

func TestConnectionStart_RespectsShorterCallerDeadline(t *testing.T) {
	conn, _ := newTestConnection(t)
	defer func() { _ = conn.Close() }()
	conn.requestTimeout = time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	_, err := conn.Start(ctx, "caller-timeout")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("Start() elapsed = %v, caller deadline did not take precedence", elapsed)
	}
}
