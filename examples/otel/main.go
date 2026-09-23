package main

import (
	"context"
	"log"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	threadify "github.com/ThreadifyDev/go-sdk"
	threadifyotel "github.com/ThreadifyDev/go-sdk/otel"
)

func main() {
	ctx := context.Background()

	apiKey := os.Getenv("THREADIFY_API_KEY")
	if apiKey == "" {
		apiKey = "your-api-key"
	}

	conn, err := threadify.Connect(ctx, apiKey,
		threadify.WithServiceName("delivery-service"),
		threadify.WithDebug(true),
	)
	if err != nil {
		log.Fatal("connect:", err)
	}
	defer conn.Close()

	exporter := threadifyotel.NewSpanExporter(conn, threadifyotel.SpanExporterOptions{
		Refs:    []string{"rider.id"},
		Filters: []string{"invoke_llm", "adk.before*", "llm.*"},
	})

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(provider)
	defer func() { _ = provider.Shutdown(ctx) }()

	orderID := os.Getenv("ORDER_ID")
	if orderID == "" {
		log.Fatal("ORDER_ID must identify the delivery being processed")
	}
	threadKey := "delivery:" + orderID
	// Initialize the contract once before collecting spans. Later requests only need the key.
	if _, err := conn.Thread(ctx, threadKey, threadify.ThreadOptions{Label: "Deliver order", Contract: "delivery_contract"}); err != nil {
		log.Fatal("initialize thread:", err)
	}
	tracer := otel.Tracer("delivery-service")

	ctx, span := tracer.Start(ctx, "deliver_order",
		trace.WithAttributes(
			attribute.String("rider.id", "RIDER-456"),
			attribute.String("threadify.thread_key", threadKey),
			attribute.String("threadify.label", "Deliver order "+orderID),
			attribute.Int("random.data", 42),
		),
	)
	time.Sleep(100 * time.Millisecond)
	span.End()

	time.Sleep(500 * time.Millisecond)
	log.Println("Done")
}
