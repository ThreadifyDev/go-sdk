package threadify

import (
	"testing"
)

func TestDeriveGraphQLURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "wss to https",
			input:    "wss://eng.threadify.dev/threads",
			expected: "https://eng.threadify.dev/graphql",
		},
		{
			name:     "ws to http",
			input:    "ws://localhost:8081/threads",
			expected: "http://localhost:8081/graphql",
		},
		{
			name:     "no replacement needed",
			input:    "http://example.com/api",
			expected: "http://example.com/api",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deriveGraphQLURL(tt.input)
			if got != tt.expected {
				t.Errorf("deriveGraphQLURL(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestRequireNonEmpty(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		value   string
		wantErr bool
	}{
		{"valid value", "apiKey", "abc123", false},
		{"empty string", "apiKey", "", true},
		{"whitespace only", "apiKey", "   ", true},
		{"tab whitespace", "apiKey", "\t\n", true},
		{"with spaces around", "apiKey", " valid ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := requireNonEmpty(tt.field, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("requireNonEmpty(%q, %q) error = %v, wantErr %v", tt.field, tt.value, err, tt.wantErr)
			}
		})
	}
}

func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{"first is valid", []string{"a", "b"}, "a"},
		{"first empty", []string{"", "b"}, "b"},
		{"all empty", []string{"", "", ""}, ""},
		{"whitespace skipped", []string{"   ", "b"}, "b"},
		{"no values", []string{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := firstNonEmpty(tt.values...)
			if got != tt.expected {
				t.Errorf("firstNonEmpty(%v) = %q, want %q", tt.values, got, tt.expected)
			}
		})
	}
}

func TestMapStringValues(t *testing.T) {
	input := map[string]any{
		"string": "hello",
		"int":    42,
		"float":  3.14,
		"bool":   true,
	}

	result := mapStringValues(input)

	if result["string"] != "hello" {
		t.Errorf("expected 'hello', got %q", result["string"])
	}
	if result["int"] != "42" {
		t.Errorf("expected '42', got %q", result["int"])
	}
	if result["bool"] != "true" {
		t.Errorf("expected 'true', got %q", result["bool"])
	}
}

func TestConnectOptions_withDefaults(t *testing.T) {
	// Test default values
	opts := ConnectOptions{}
	opts = opts.withDefaults()

	if opts.WSURL != "" {
		t.Errorf("expected empty WSURL by default, got %q", opts.WSURL)
	}
	if opts.GraphQLURL != "" {
		t.Errorf("expected empty GraphQLURL by default, got %q", opts.GraphQLURL)
	}
}

func TestConnectOptions_Validate(t *testing.T) {
	opts := ConnectOptions{
		WSURL:       "wss://example.com",
		MaxInFlight: 10,
	}
	if err := opts.validate(); err != nil {
		t.Errorf("validate() error: %v", err)
	}

	opts.MaxInFlight = 0
	if err := opts.validate(); err == nil {
		t.Error("expected error for MaxInFlight < 1")
	}

	opts.MaxInFlight = 200
	if err := opts.validate(); err == nil {
		t.Error("expected error for MaxInFlight > 100")
	}

	opts.MaxInFlight = 10
	opts.WSURL = ""
	if err := opts.validate(); err == nil {
		t.Error("expected error for empty WSURL")
	}
}

func TestNowISO(t *testing.T) {
	ts := nowISO()
	if ts == "" {
		t.Error("nowISO() returned empty string")
	}
	// Should end with Z (UTC) or contain a timezone offset.
	if len(ts) < 20 {
		t.Errorf("nowISO() timestamp too short: %q", ts)
	}
}
