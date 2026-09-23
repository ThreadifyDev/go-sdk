package main

import (
	"context"
	"fmt"
	"log"
	"os"

	threadify "github.com/ThreadifyDev/go-sdk"
)

func main() {
	apiKey := os.Getenv("THREADIFY_API_KEY")
	if apiKey == "" {
		apiKey = "your-api-key"
	}

	ctx := context.Background()

	// 1. Connect to Threadify
	conn, err := threadify.Connect(ctx, apiKey)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to Threadify!")

	// 2. Create or resume this business process using its existing order ID.
	orderID := os.Getenv("ORDER_ID")
	if orderID == "" {
		log.Fatal("ORDER_ID must identify the order being processed")
	}
	thread, err := conn.Thread(ctx, "order:"+orderID, threadify.ThreadOptions{
		Label: "Order processing", Refs: map[string]string{"orderId": orderID},
	})
	if err != nil {
		log.Fatalf("Failed to resolve thread: %v", err)
	}
	fmt.Printf("Thread resolved: %s\n", thread.ThreadID)

	err = thread.AddRefs(ctx, map[string]string{
		"orderId": orderID,
	})
	if err != nil {
		log.Fatalf("Failed to add thread refs: %v", err)
	}

	// 3. Record steps using the fluent API
	_, err = thread.Step("order_received").
		AddContext(map[string]any{
			"orderId": orderID,
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
