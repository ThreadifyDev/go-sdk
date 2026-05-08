package otel

import (
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
