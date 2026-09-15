# Threadify Go SDK

The official Go SDK for connecting to the Threadify Engine.

## Installation

```bash
go get github.com/ThreadifyDev/go-sdk
```

## Quick Start

### 1. Connect to the Engine

Use `threadify.Connect` to establish a connection. You can configure the connection using Functional Options.

```go
package main

import (
	"context"
	"log"
	"github.com/ThreadifyDev/go-sdk"
)

func main() {
	ctx := context.Background()
conn, _ := threadify.Connect(ctx, "your-api-key")
defer conn.Close()

	thread, err := conn.Start(ctx, "", threadify.WithContract("order_flow"))

    if err != nil {
        log.Fatal(err)
    }

	// Easy chaining!
	err := thread.Step("payment_processed").
		AddContext(map[string]any{"amount": 99.99}).
		Success(ctx)

	if err != nil {
		log.Fatal(err)
	}
}
```

Ordinary SDK requests have a 1-second timeout by default, so callers can use
`context.Background()` without creating a deadline for every operation. Caller
cancellation and shorter deadlines still take precedence. Configure the SDK
default when connecting if needed:

```go
conn, err := threadify.Connect(ctx, "your-api-key",
    threadify.WithRequestTimeout(15*time.Second),
)
```

### 2. Start a New Thread

Start a thread, optionally associating it with a contract.

```go
// Start a generic thread
thread, err := conn.Start(ctx, "")

if err != nil {
    log.Fatal(err)
}

// Start a thread for a specific contract
thread, err := conn.Start(ctx, "Order Processing Label", threadify.WithContract("order_processing"))

if err != nil {
    log.Fatal(err)
}
```

### 3. Join an Existing Thread

You can join a thread by its ID or using a secure token.

```go
// Join by ID
thread, err := conn.Join(ctx, 
    threadify.WithJoinThreadID("thread-123"), 
    threadify.WithJoinRole("logistics"),
)

if err != nil {
    log.Fatal(err)
}

// Join by Token
thread, err := conn.Join(ctx, 
    threadify.WithJoinToken("ey..."),
)

if err != nil {
    log.Fatal(err)
}
```

### 4. Record Steps

Record steps in a thread's lifecycle. Add thread refs on the thread, and keep step data on the step.

```go
err := thread.AddRefs(ctx, map[string]string{
    "orderId": "ORD-999",
})
if err != nil {
    log.Fatal(err)
}

step := thread.Step("order_shipped")

_, err = step.
    AddContext(map[string]any{
        "trackingNumber": "TRK123456",
        "carrier":        "FedEx",
    }).
    Success(ctx, "Order has been shipped successfully")

if err != nil {
    log.Fatal(err)
}
```

## Event Subscriptions

Listen for real-time events from the engine.

```go
// Subscribe to 'step.success' events for the 'order_placed' step
err := conn.Subscribe(ctx, "step.success", "order_placed", func(n *threadify.Notification) {
    log.Printf("Order placed: %s", n.ThreadID)
    
    // Acknowledge receipt
    n.Ack()
})

// Unsubscribe when done
defer conn.Unsubscribe(ctx, "step.success", "order_placed")
```

### Event Patterns

| Pattern | Description |
| :--- | :--- |
| `step.success` | Step completed successfully |
| `step.failed` | Step failed |
| `step.*` | Any step execution event |
| `rule.violated` | Validation rule violated |
| `rule.passed` | Validation rule passed |
| `*` | All events |

## Functional Options

The SDK uses the Functional Option pattern for configuration.

### Connection Options

-   `WithServiceName(string)`: Set the service name (default: "default").
-   `WithEngineURL(string)`: Set one HTTP(S) Engine base URL, retaining proxy prefixes.
-   `WithWSURL(string)`: Set an explicit WebSocket URL for split deployments.
-   `WithDebug(bool)`: Enable debug logging.
-   `WithConnectTimeout(time.Duration)`: Set the connection timeout.
-   `WithMaxInFlight(int)`: Set the maximum number of concurrent requests.

### Join Options

-   `WithJoinThreadID(string)`: Join by Thread ID.
-   `WithJoinRole(string)`: Set the role when joining by ID.
-   `WithJoinToken(string)`: Join using a secure invitation token.

## Versioning & Releases

This SDK follows [Semantic Versioning](https://semver.org/). Releases are published from Git tags via GitHub Actions.

### Bumping a Version
Update the SDK version locally with:

```bash
make bump-version VERSION=0.3.0
```

This updates the repo's `VERSION` file, which is the source for `threadify.Version`.

### Publishing a Release
After updating `VERSION` and merging your changes:

```bash
git tag v0.3.0
git push origin v0.3.0
```

Pushing a `v*` tag triggers the release workflow, which:
- runs the root SDK test suite
- runs the `otel` sub-module test suite
- creates a GitHub Release for that tag

### Manual Version Access
The current version of the SDK is available via the `threadify.Version` value, which is sourced from the repo's `VERSION` file.

```go
fmt.Println("Threadify Go SDK Version:", threadify.Version)
```

## OpenTelemetry Integration

Because Go is statically typed, the OpenTelemetry integration requires its own sub-module to avoid bloating the core SDK for users who do not use OpenTelemetry.

**Install:**

```bash
go get github.com/ThreadifyDev/go-sdk/otel
```

**Usage:**

```go
import (
    "go.opentelemetry.io/otel"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"

    threadify "github.com/ThreadifyDev/go-sdk"
    threadifyotel "github.com/ThreadifyDev/go-sdk/otel"
)

conn, _ := threadify.Connect(ctx, "api-key")

exporter := threadifyotel.NewSpanExporter(conn, threadifyotel.SpanExporterOptions{
    Refs:    []string{"rider.id"},
    Filters: []string{"invoke_llm", "adk.before*", "llm.*"},
})

provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
otel.SetTracerProvider(provider)
```

**Filter patterns:**

- `"invoke_llm"` — exact match
- `"adk.before*"` — prefix wildcard, drops any span starting with `adk.before`
- `"llm.*"` — prefix wildcard, drops any span starting with `llm.`

## Testing

To run the SDK tests, execute:

```bash
make test
```

Alternatively, use the Go command:

```bash
go test -v ./...
```


## Contract coordination (0.4)

```go
conn, err := threadify.Connect(ctx, apiKey,
    threadify.WithEngineURL("https://threadify.example.com"),
    threadify.WithServiceName("payments"))
if err != nil { return err }
defer conn.Close()
threads, err := conn.GetThreadsByRef(ctx, map[string]string{"order_id": "ORD-1001"},
    threadify.RefQuery{Status: "active", Limit: 25})
if err != nil { return err }
if len(threads) == 0 { return fmt.Errorf("order thread not found") }
thread, err := conn.Join(ctx, threadify.WithJoinThreadID(threads[0].ID), threadify.WithJoinRole("processor"))
if err != nil { return err }
grant, err := thread.WaitFor(ctx, "charge", &threadify.WaitOptions{Timeout: 15 * time.Second})
if err != nil { return err }
// Execute the permitted business operation here, or grant.Cancel(ctx) to release it.
_ = grant
result, err := thread.Step("charge").AddContext(map[string]any{"amount": 42}).Success(
    ctx, "charged", threadify.ReportOptions{WaitFor: true, Timeout: 15 * time.Second})
if err != nil { return err }
validation, err := thread.WaitForValidation(ctx, "charge", result.StepID, nil)
```

`WaitFor` now asks the Engine for permission for one invocation. The previous
notification helper is `WaitForNotification`; a notification is not permission.
Cancelling a grant closes that invocation; it does not restore consumed fresh
prerequisites. A fresh-prerequisite contract needs a new successful predecessor
before another grant. The grant's invocation ID and default idempotency key accompany the next report
of that step. Synchronous reports return the acknowledged `StepID` and the exact
validation result. Violations, unavailable validation, duplicate synchronous
reports, and mismatched responses return errors.

Waits send one correlated request and receive one final response. Cancellation
and deadlines cancel the server wait; `errors.Is(err, context.Canceled)` and
`errors.Is(err, context.DeadlineExceeded)` remain usable. Inspect `*RequestError`
for `Code`, `RequestID`, `InvocationID`, `StepID`, and `IdempotencyKey` when supplied.
A timeout does not prove an accepted event was rolled back; query or resume
validation before retrying. Writes are not automatically retried. Default wait
time is 10 seconds, maximum 5 minutes, bounded by the caller's context. Ordinary
requests continue to use `WithRequestTimeout`.

The OTEL exporter writes original span and span-event timestamps to the event
fields and adds `otel_trace_id`/`otel_span_id` refs. `SpanExporterOptions.RefsMap`
renames selected attributes into refs; `Refs` still copies named attributes.
`ThreadStep.RecordedTimes(start, end)` also supports delayed direct events.

CI runs race tests for both the root and OTEL modules. Release tags must match
`VERSION`: `v0.4.0` for the root module and `otel/v0.4.0` for the separate OTEL
module. Both use the existing release workflow; ordinary branch pushes do not
publish releases. Before tagging, update the OTEL module's root dependency to the
published root version; local tests use its existing `replace` directive.
