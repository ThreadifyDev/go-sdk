package otel

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	threadify "github.com/ThreadifyDev/go-sdk"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type exporterTransport struct {
	mu          sync.Mutex
	sent        []map[string]any
	replies     chan map[string]any
	closed      bool
	contract    string
	failAction  string
	resolvedKey string
}

func (m *exporterTransport) Send(msg map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("closed")
	}
	m.sent = append(m.sent, msg)
	response := map[string]any{"action": msg["action"], "status": "success", "threadId": "thread-1", "threadKey": msg["threadKey"], "requestId": msg["requestId"], "contractName": m.contract, "contractVersion": 2, "label": "Stored session"}
	if m.resolvedKey != "" {
		response["threadKey"] = m.resolvedKey
	}
	if m.contract != "" {
		response["contractId"] = "contract-1"
	}
	if msg["action"] == m.failAction {
		response["status"] = "error"
		response["message"] = "engine refused " + m.failAction
	}
	m.replies <- response
	return nil
}
func (m *exporterTransport) Recv() (map[string]any, error) {
	msg, ok := <-m.replies
	if !ok {
		return nil, fmt.Errorf("closed")
	}
	return msg, nil
}
func (m *exporterTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.replies)
	}
	return nil
}
func (m *exporterTransport) Dial(context.Context, string) (threadify.Transport, error) { return m, nil }
func exporterTestConnection(t *testing.T, contract, fail string) (*threadify.Connection, *exporterTransport) {
	t.Helper()
	mt := &exporterTransport{replies: make(chan map[string]any, 100), contract: contract, failAction: fail}
	conn, err := threadify.Connect(context.Background(), "test-key", threadify.WithDialer(mt))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, mt
}
func exporterSpan(traceByte, spanByte byte, key string, complete bool) sdktrace.ReadOnlySpan {
	attrs := []attribute.KeyValue{}
	if key != "" {
		attrs = append(attrs, attribute.String("threadify.thread_key", key))
	}
	if complete {
		attrs = append(attrs, attribute.Bool("threadify.run.complete", true))
	}
	return tracetest.SpanStub{Name: fmt.Sprintf("turn-%d", spanByte), StartTime: time.Now(), EndTime: time.Now(), SpanContext: trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{traceByte}, SpanID: trace.SpanID{spanByte}}), Attributes: attrs}.Snapshot()
}
func TestKeyedExporterUsesThreadAcrossTracesAndDefersCompletion(t *testing.T) {
	conn, mt := exporterTestConnection(t, "", "")
	exporter := NewSpanExporter(conn, SpanExporterOptions{})
	if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{exporterSpan(1, 1, "session:42", false)}); err != nil {
		t.Fatal(err)
	}
	mt.mu.Lock()
	for _, msg := range mt.sent {
		if msg["action"] == "threadEnd" {
			t.Fatal("keyed root completed the session")
		}
	}
	mt.mu.Unlock()
	// Completion appears before a second span in the same batch: all steps must arrive first.
	if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{exporterSpan(2, 2, "session:42", true), exporterSpan(2, 3, "session:42", false)}); err != nil {
		t.Fatal(err)
	}
	mt.mu.Lock()
	defer mt.mu.Unlock()
	resolves, steps, ends := 0, 0, 0
	for _, msg := range mt.sent {
		switch msg["action"] {
		case "startThread":
			t.Fatal("keyed exporter used legacy creation")
		case "thread":
			resolves++
			if msg["threadKey"] != "session:42" {
				t.Fatal(msg)
			}
			if _, exists := msg["contractName"]; exists {
				t.Fatal("resume redefines contract")
			}
		case "recordThreadEvent":
			steps++
			if ends != 0 {
				t.Fatal("step sent after completion")
			}
		case "threadEnd":
			ends++
		}
	}
	if resolves != 3 || steps != 3 || ends != 1 {
		t.Fatalf("resolves=%d steps=%d ends=%d", resolves, steps, ends)
	}
}
func TestExporterStoredContractPreventsManualCompletion(t *testing.T) {
	for _, key := range []string{"session:42", ""} {
		t.Run(map[bool]string{true: "keyed", false: "trace only"}[key != ""], func(t *testing.T) {
			conn, mt := exporterTestConnection(t, "agent_contract", "")
			exporter := NewSpanExporter(conn, SpanExporterOptions{})
			if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{exporterSpan(1, 1, key, true)}); err != nil {
				t.Fatal(err)
			}
			mt.mu.Lock()
			defer mt.mu.Unlock()
			for _, msg := range mt.sent {
				if msg["action"] == "threadEnd" {
					t.Fatal("stored contract was ignored")
				}
			}
		})
	}
}
func TestExporterReturnsResolutionReferenceStepAndCompletionErrors(t *testing.T) {
	for _, action := range []string{"thread", "addRefs", "recordThreadEvent", "threadEnd"} {
		t.Run(action, func(t *testing.T) {
			conn, _ := exporterTestConnection(t, "", action)
			exporter := NewSpanExporter(conn, SpanExporterOptions{})
			if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{exporterSpan(1, 1, "session:42", true)}); err == nil {
				t.Fatalf("ignored %s error", action)
			}
		})
	}
}

func TestTraceOnlyResumeRecoversStoredSessionKey(t *testing.T) {
	conn, mt := exporterTestConnection(t, "", "")
	mt.mu.Lock()
	mt.resolvedKey = "session:42"
	mt.mu.Unlock()
	exporter := NewSpanExporter(conn, SpanExporterOptions{})
	if err := exporter.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{exporterSpan(1, 1, "", false), exporterSpan(1, 2, "", false)}); err != nil {
		t.Fatal(err)
	}
	mt.mu.Lock()
	defer mt.mu.Unlock()
	resolves := 0
	for _, msg := range mt.sent {
		if msg["action"] == "threadEnd" {
			t.Fatal("trace-only resume closed shared session")
		}
		if msg["action"] == "thread" {
			resolves++
			if msg["threadKey"] != "session:42" {
				t.Fatal(msg)
			}
		}
	}
	if resolves != 1 {
		t.Fatalf("later span failed to use recovered key: %d", resolves)
	}
}
