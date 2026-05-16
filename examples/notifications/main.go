package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	threadify "github.com/ThreadifyDev/go-sdk"
)

func main() {
	apiKey := os.Getenv("THREADIFY_API_KEY")
	if apiKey == "" {
		apiKey = "your-api-key"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Connect
	conn, err := threadify.Connect(ctx, apiKey)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 2. Join an existing thread (e.g. from a token or ID)
	threadID := "your-thread-id"
	if len(os.Args) > 1 {
		threadID = os.Args[1]
	} else {
		fmt.Println("Usage: go run main.go <thread-id>")
		// For demonstration, we'll just stop here if no ID provided
		return
	}

	thread, err := conn.Join(ctx,
		threadify.WithJoinThreadID(threadID),
		threadify.WithJoinRole("observer"),
	)
	if err != nil {
		log.Fatalf("Failed to join thread: %v", err)
	}

	fmt.Printf("Joined thread %s. Waiting for 'payment_confirmed' step...\n", thread.ThreadID)

	// 3. Wait for a specific step to be completed (by another party)
	// This blocks until the notification arrives or context times out.
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	notif, err := thread.WaitFor(waitCtx, "payment_confirmed", &threadify.WaitOptions{
		Statuses: []string{"success"},
	})

	if err != nil {
		log.Fatalf("Error waiting for step: %v", err)
	}

	// 4. Acknowledge the notification

	err = notif.Ack()
	if err != nil {
		log.Fatalf("Error acknowledging notification: %v", err)
	}

	fmt.Printf("Step confirmed! Message: %s\n", notif.Message)
	fmt.Printf("Notification Details: %v\n", notif.Details)
}
