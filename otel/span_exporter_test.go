package otel

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"strings"
	"testing"

	threadify "github.com/ThreadifyDev/go-sdk"
)

func TestShouldDrop(t *testing.T) {
	conn := &threadify.Connection{}
	exporter := NewSpanExporter(conn, SpanExporterOptions{
		Filters: []string{
			"invoke_llm",
			"adk.before*",
			"llm.*",
		},
	})

	tests := []struct {
		name     string
		spanName string
		wantDrop bool
	}{
		{"exact match", "invoke_llm", true},
		{"prefix match wildcard", "adk.before_tool_call", true},
		{"prefix match exact", "adk.before", true},
		{"prefix match llm", "llm.chat_completion", true},
		{"prefix match llm exact", "llm.", true},
		{"no match", "some_other_span", false},
		{"partial prefix no match", "adk.after", false},
		{"empty filter skipped", "anything", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exporter.shouldDrop(tt.spanName)
			if got != tt.wantDrop {
				t.Errorf("shouldDrop(%q) = %v, want %v", tt.spanName, got, tt.wantDrop)
			}
		})
	}
}

func TestShouldDrop_EmptyFilters(t *testing.T) {
	conn := &threadify.Connection{}
	exporter := NewSpanExporter(conn, SpanExporterOptions{})

	if exporter.shouldDrop("anything") {
		t.Error("expected no drop when filters are empty")
	}
}

func TestThreadKeySelection(t *testing.T) {
	disabled := false
	for _, tc := range []struct {
		name     string
		attrs    []attribute.KeyValue
		resource []attribute.KeyValue
		opt      *bool
		want     string
		invalid  bool
	}{
		{name: "default workflow", attrs: []attribute.KeyValue{attribute.String("workflow.run_id", "run")}, want: "run"},
		{name: "resource fallback", resource: []attribute.KeyValue{attribute.String("workflow.run_id", "resource")}, want: "resource"},
		{name: "explicit precedence", attrs: []attribute.KeyValue{attribute.String("workflow.run_id", "run"), attribute.String("threadify.thread_key", " explicit ")}, want: "explicit"},
		{name: "disabled", attrs: []attribute.KeyValue{attribute.String("workflow.run_id", "run")}, opt: &disabled},
		{name: "explicit with disabled", attrs: []attribute.KeyValue{attribute.String("threadify.thread_key", "explicit")}, opt: &disabled, want: "explicit"},
		{name: "internal target", attrs: []attribute.KeyValue{attribute.String("threadify.thread_id", "internal"), attribute.Int("workflow.run_id", 4)}},
		{name: "blank key", attrs: []attribute.KeyValue{attribute.String("threadify.thread_key", " ")}, invalid: true},
		{name: "invalid type", attrs: []attribute.KeyValue{attribute.Int("workflow.run_id", 4)}, invalid: true},
		{name: "byte limit", attrs: []attribute.KeyValue{attribute.String("workflow.run_id", strings.Repeat("é", 513))}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := NewSpanExporter(&threadify.Connection{}, SpanExporterOptions{UseWorkflowRunID: tc.opt})
			span := tracetest.SpanStub{Attributes: tc.attrs, Resource: resource.NewSchemaless(tc.resource...)}.Snapshot()
			got, err := exporter.threadKey(span)
			if (err != nil) != tc.invalid || got != tc.want {
				t.Fatalf("got %q, %v; want %q invalid=%v", got, err, tc.want, tc.invalid)
			}
		})
	}
	if !attrBool([]attribute.KeyValue{attribute.Bool("threadify.run.complete", true)}, "threadify.run.complete") {
		t.Fatal("boolean completion marker ignored")
	}
}
