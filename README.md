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

    threadify "github.com/ThreadifyDev/go-sdk"
)

func main() {
    ctx := context.Background()
    conn, err := threadify.Connect(ctx, "your-api-key")
    if err != nil { log.Fatal(err) }
    defer conn.Close()

    // Use the application's order ID, stable across requests and workers.
    thread, err := conn.Thread(ctx, "order:ORD-123", threadify.ThreadOptions{
        Label: "Order processing",
        Contract: "order_flow",
        Refs: map[string]string{"customerId": "CUSTOMER-42"},
    })
    if err != nil { log.Fatal(err) }

    _, err = thread.Step("payment_processed").
        AddContext(map[string]any{"amount": 99.99}).
        Success(ctx)
    if err != nil { log.Fatal(err) }
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

### 2. Create or Resume by Thread Key

`Thread(ctx, threadKey, options...)` atomically creates or resumes a thread within
the authenticated tenant. Choose a stable application identifier for the session,
order, or process. The Engine owns the generated `ThreadID` and key mapping, so
callers do not need to persist the ID or retain a handle across requests.

```go
// Initialization: options supply creation defaults.
thread, err := conn.Thread(ctx, "session:SESSION-123", threadify.ThreadOptions{
    Label: "Agent session",
    Contract: "agent_contract",
    Refs: map[string]string{"customerId": "CUSTOMER-42"},
    Tags: []string{"agent"},
})
if err != nil { log.Fatal(err) }

// A later request or worker only needs the key.
thread, err = conn.Thread(ctx, "session:SESSION-123")
if err != nil { log.Fatal(err) }
_, err = thread.Step("tool_call").AddContext(map[string]any{"tool": "search"}).Success(ctx)
```

Existing threads return their stored contract and pinned version, label, refs, and
tags. Omitting the contract on resume is expected. Repeating the same contract is
allowed and keeps the pinned version; a conflicting contract returns an error.
Creation options do not overwrite existing metadata; use `AddRefs` for reference
updates. `ThreadInstance` exposes `ThreadKey`, `Label`, `ContractName`,
`ContractVersion`, `ContractID`, `Refs`, and `Tags` from the response.

An unknown key with no options creates a free-form thread. Initialize contracted
sessions before workers or exporters report their first steps. Closed threads
reject resolution and writes; keys stay bound to the original thread. A new
process needs a new key. Keys are trimmed, non-empty UTF-8 strings limited to
1024 bytes. Concurrent resolutions of the same key return the same Engine thread.

`Start` remains deprecated for compatibility. New integrations should use `Thread`.

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

### Thread Options

`ThreadOptions` contains optional creation defaults:

- `Label string`: Display label; never used as identity.
- `Contract string`: Contract name, resolved and pinned at creation.
- `Refs map[string]string`: Initial searchable references.
- `Tags []string`: Initial tags.
- `ServiceName string`: Override the connection service.
- `Role string`: Role used when creating a contract thread.

Omit the entire options value when resuming with an existing key.

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

Set `threadify.thread_key` on spans (or resource attributes) to use the same
application identity as `conn.Thread`. `workflow.run_id` is the fallback when
`UseWorkflowRunID` is nil or true. Direct `threadify.thread_id` targeting takes
precedence. `threadify.label`, `threadify.contract`, `threadify.role`,
`threadify.service`, and `threadify.tags` supply creation defaults. Prefer
initializing contracted sessions with `conn.Thread` before spans are exported;
subsequent spans only need `threadify.thread_key`.

```go
ctx, span := otel.Tracer("agent").Start(ctx, "tool_call", trace.WithAttributes(
    attribute.String("threadify.thread_key", sessionID),
    attribute.String("threadify.ref.customerId", customerID),
))
// Perform the tool call, then finish this span.
span.End()
```

This span example also imports `go.opentelemetry.io/otel/attribute` and
`go.opentelemetry.io/otel/trace`. Keyed threads remain active across traces and
requests. An explicit boolean `threadify.run.complete` closes a free-form keyed
thread after every span in the export batch has been recorded. Contracts control
their own completion. Without any application key or direct thread ID, the
exporter uses trace-ID correlation and completes a free-form thread after its root
span. This internal fallback retains the legacy creation operation to preserve the
Engine's separate trace-ID namespace; it does not turn trace IDs into application
keys. Resolution, reference, step, and completion errors are returned to the caller.

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
