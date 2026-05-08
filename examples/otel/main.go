package main

import (
	"context"
	"log"
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

	conn, err := threadify.Connect(ctx, "your-api-key",
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

	tracer := otel.Tracer("delivery-service")

	ctx, span := tracer.Start(ctx, "deliver_order",
		trace.WithAttributes(
			attribute.String("rider.id", "RIDER-456"),
			attribute.String("threadify.contract", "delivery_contract"),
			attribute.String("threadify.label", "Deliver Order #12345"),
			attribute.Int("random.data", 42),
		),
	)
	time.Sleep(100 * time.Millisecond)
	span.End()

	time.Sleep(500 * time.Millisecond)
	log.Println("Done")
}
