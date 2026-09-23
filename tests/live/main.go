// Cross-language contract participant, invoked by the Engine E2E harness.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	threadify "github.com/ThreadifyDev/go-sdk"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func connectAndResumeThread(ctx context.Context) (*threadify.Connection, *threadify.ThreadInstance, error) {
	conn, err := threadify.Connect(ctx, os.Getenv("THREADIFY_API_KEY"), threadify.WithEngineURL(os.Getenv("THREADIFY_ENGINE_URL")), threadify.WithServiceName("go-fulfilment"), threadify.WithRequestTimeout(10*time.Second))
	if err != nil {
		return nil, nil, err
	}
	thread, err := conn.Thread(ctx, "parity:"+os.Getenv("THREADIFY_PARITY_ID"))
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if thread.ThreadID != os.Getenv("THREADIFY_THREAD_ID") {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("thread key resolved a different thread")
	}
	if thread.ContractName == "" || thread.ContractVersion < 1 {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("resume lost pinned contract metadata")
	}
	return conn, thread, nil
}

func recordCharge(ctx context.Context, thread *threadify.ThreadInstance) (*threadify.StepResult, error) {
	if _, err := thread.WaitFor(ctx, "charge", nil); err != nil {
		return nil, err
	}
	result, err := thread.Step("charge").AddContext(map[string]any{"amount": 42}).Success(ctx, "charged", threadify.ReportOptions{WaitFor: true})
	if err != nil {
		return nil, err
	}
	if result.StepID == "" || result.Validation == nil || result.Validation.Decision != "passed" {
		return nil, fmt.Errorf("missing validation result")
	}
	if _, err := thread.WaitForValidation(ctx, "charge", result.StepID, nil); err != nil {
		return nil, err
	}
	return result, nil
}

func waitForSharedRef(ctx context.Context, conn *threadify.Connection, threadID string) error {
	for deadline := time.Now().Add(10 * time.Second); ; {
		found, err := conn.GetThreadsByRef(ctx, map[string]string{"parity_id": os.Getenv("THREADIFY_PARITY_ID")})
		if err != nil {
			return err
		}
		if len(found) == 1 && found[0].ID == threadID {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ref lookup did not find shared thread")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	conn, thread, err := connectAndResumeThread(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	result, err := recordCharge(ctx, thread)
	if err != nil {
		return err
	}
	if err := thread.AddRefs(ctx, map[string]string{"go_parity": "contributed"}); err != nil {
		return err
	}
	if err := waitForSharedRef(ctx, conn, thread.ThreadID); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{"thread_id": thread.ThreadID, "step_id": result.StepID})
}
