package otel

import (
	"context"
	"encoding/json"
	threadify "github.com/ThreadifyDev/go-sdk"
	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecordedSpanTimestampsAndRefs(t *testing.T) {
	events := make(chan map[string]any, 8)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer socket.Close()
		for {
			var msg map[string]any
			if socket.ReadJSON(&msg) != nil {
				return
			}
			action, _ := msg["action"].(string)
			response := map[string]any{"action": action, "status": "success", "threadId": "thread-otel", "role": "processor"}
			if action == "addRefs" || action == "recordThreadEvent" {
				events <- msg
			}
			if socket.WriteJSON(response) != nil {
				return
			}
		}
	}))
	defer server.Close()
	conn, err := threadify.Connect(context.Background(), "fixture-key", threadify.WithEngineURL(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	parent := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{1}})
	span := tracetest.SpanStub{Name: "db.select", StartTime: start, EndTime: start.Add(3 * time.Second), Parent: parent, SpanContext: trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}}), Attributes: []attribute.KeyValue{attribute.String("threadify.thread_id", "thread-otel"), attribute.String("order.id", "ORD-1001")}, Events: []sdktrace.Event{{Name: "row", Time: start.Add(time.Second)}}}.Snapshot()
	exporter := NewSpanExporter(conn, SpanExporterOptions{RefsMap: map[string]string{"order.id": "order_id"}})
	if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{span}); err != nil {
		t.Fatal(err)
	}
	refs := <-events
	event := <-events
	raw, _ := json.Marshal(refs["refs"])
	if !strings.Contains(string(raw), "ORD-1001") || !strings.Contains(string(raw), "otel_trace_id") {
		t.Fatal(string(raw))
	}
	if event["startedAt"] != start.Format(time.RFC3339Nano) || event["finishedAt"] != start.Add(3*time.Second).Format(time.RFC3339Nano) {
		t.Fatal(event)
	}
	subs := event["subSteps"].([]any)
	if subs[0].(map[string]any)["recordedAt"] != start.Add(time.Second).Format(time.RFC3339Nano) {
		t.Fatal(subs)
	}
}
