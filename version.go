package threadify

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var rawVersion string

// Version is the current version of the Threadify Go SDK.
var Version = strings.TrimSpace(rawVersion)
