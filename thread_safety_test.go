package threadify

import (
	"context"
	"testing"
)

func TestThreadInstance_NilSafety(t *testing.T) {
	var thread *ThreadInstance // explicitly nil

	ctx := context.Background()

	t.Run("Step on nil thread", func(t *testing.T) {
		step := thread.Step("some_step")
		if step == nil {
			t.Error("expected non-nil step wrapper even on nil thread")
		}
		// Try to use the step, should result in an error in terminal methods
		_, err := step.Success(ctx)
		if err == nil {
			t.Error("expected error when calling Success on step from nil thread, got nil")
		}
	})

	t.Run("InviteParty on nil thread", func(t *testing.T) {
		_, err := thread.InviteParty(ctx, InviteOptions{Role: "external"})
		if err == nil {
			t.Error("expected error when calling InviteParty on nil thread, got nil")
		}
	})

	t.Run("WaitFor on nil thread", func(t *testing.T) {
		_, err := thread.WaitFor(ctx, "some_step", nil)
		if err == nil {
			t.Error("expected error when calling WaitFor on nil thread, got nil")
		}
	})

	t.Run("AddRefs on nil thread", func(t *testing.T) {
		err := thread.AddRefs(ctx, map[string]string{"foo": "bar"})
		if err == nil {
			t.Error("expected error when calling AddRefs on nil thread, got nil")
		}
	})

	t.Run("LinkThread on nil thread", func(t *testing.T) {
		err := thread.LinkThread(ctx, "00000000-0000-0000-0000-000000000000", "parent")
		if err == nil {
			t.Error("expected error when calling LinkThread on nil thread, got nil")
		}
	})

	t.Run("End on nil thread", func(t *testing.T) {
		_, err := thread.End(ctx, StatusCompleted)
		if err == nil {
			t.Error("expected error when calling End on nil thread, got nil")
		}
	})
}
