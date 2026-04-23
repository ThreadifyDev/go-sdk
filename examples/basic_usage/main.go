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

	// 1. Connect to Threadify
	conn, err := threadify.Connect(ctx, apiKey)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to Threadify!")

	// 2. Start a new thread
	thread, err := conn.Start(ctx, "", "order_processing")
	if err != nil {
		log.Fatalf("Failed to start thread: %v", err)
	}
	fmt.Printf("Thread started: %s\n", thread.ThreadID)

	// 3. Record steps using the fluent API
	_, err = thread.Step("order_received").
		AddContext(map[string]any{
			"orderId": "ORD-12345",
			"amount":  99.99,
		}).
		Success(ctx, "Order recorded successfully")

	if err != nil {
		log.Fatalf("Failed to record step: %v", err)
	}
	fmt.Println("Step 'order_received' recorded.")

	// 4. Complete the thread
	_, err = thread.Complete(ctx, "Order flow finished")
	if err != nil {
		log.Fatalf("Failed to complete thread: %v", err)
	}
	fmt.Println("Thread completed successfully!")
}
