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

	thread, err := conn.Start(ctx, threadify.WithContract("order_flow"))

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

### 2. Start a New Thread

Start a thread, optionally associating it with a contract.

```go
// Start a generic thread
thread, err := conn.Start(ctx)

if err != nil {
    log.Fatal(err)
}

// Start a thread for a specific contract
thread, err := conn.Start(ctx, 
    threadify.WithContract("order_processing"),
)

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

Record steps in a thread's lifecycle. You can add context, references, and sub-steps.

```go
step := thread.Step("order_shipped")

_, err = step.
    AddContext(map[string]any{
        "trackingNumber": "TRK123456",
        "carrier":        "FedEx",
    }).
    AddRefs(map[string]string{
        "orderId": "ORD-999",
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
-   `WithWSURL(string)`: Set the WebSocket URL.
-   `WithDebug(bool)`: Enable debug logging.
-   `WithConnectTimeout(time.Duration)`: Set the connection timeout.
-   `WithMaxInFlight(int)`: Set the maximum number of concurrent requests.

### Join Options

-   `WithJoinThreadID(string)`: Join by Thread ID.
-   `WithJoinRole(string)`: Set the role when joining by ID.
-   `WithJoinToken(string)`: Join using a secure invitation token.

## Versioning & Releases

This SDK follows [Semantic Versioning](https://semver.org/) and [Conventional Commits](https://www.conventionalcommits.org/). Releases are automated via GitHub Actions.

### Automated Increments
Whenever changes are merged to the `main` branch, the release system analyzes commit messages to determine the next version:
- `fix: ...` -> Patch bump (e.g., v0.1.0 -> v0.1.1)
- `feat: ...` -> Minor bump (e.g., v0.1.0 -> v0.2.0)
- `feat!: ...` or `BREAKING CHANGE: ...` -> Major bump (e.g., v0.1.0 -> v1.0.0)

### Manual Version Access
The current version of the SDK is available via the `threadify.Version` constant.

```go
fmt.Println("Threadify Go SDK Version:", threadify.Version)
```

## Testing

To run the SDK tests, execute:

```bash
make test
```

Alternatively, use the Go command:

```bash
go test -v ./...
```
