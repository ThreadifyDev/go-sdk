package threadify

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersionMatchesVersionFile(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(".", "VERSION"))
	if err != nil {
		t.Fatalf("read VERSION: %v", err)
	}

	want := strings.TrimSpace(string(data))
	if want == "" {
		t.Fatal("VERSION file must not be empty")
	}

	if Version != want {
		t.Fatalf("Version = %q, want %q", Version, want)
	}
}
