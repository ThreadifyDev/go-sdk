# OTel thread keys

The exporter selects an explicit `threadify.thread_id`, then
`threadify.thread_key`, then `workflow.run_id`, then trace-ID correlation.
Different traces with the same thread key share one thread and pinned contract.
`threadify.thread_key` uses the same identity as `connection.Thread(ctx, threadKey)`.
The Engine rejects conflicting contracts and closed threads instead of reopening
or replacing them.

`UseWorkflowRunID` defaults to enabled when nil. To disable only the workflow fallback:

```go
useWorkflow := false
exporter := otel.NewSpanExporter(connection, otel.SpanExporterOptions{
    UseWorkflowRunID: &useWorkflow,
})
```

Set the key on all relevant spans or a resource dedicated to that run. Use the
application's stable session, order, or process ID, with a different key for each
new process. Keys are trimmed, case-sensitive, non-empty strings of at most
1024 UTF-8 bytes. A blank explicit `threadify.thread_key` is an error; an empty
`workflow.run_id` falls through to trace-ID correlation.

Initialize contracted sessions before reporting spans:

```go
_, err := connection.Thread(ctx, sessionID, threadify.ThreadOptions{
    Label: "Agent session", Contract: "agent_contract",
})
// Handle err before recording spans with threadify.thread_key=sessionID.
```

Later spans only need the key. Resuming loads the stored contract and pinned
version. Creation options (`threadify.label`, `threadify.contract`,
`threadify.role`, `threadify.service`, `threadify.tags`) never overwrite existing
metadata. Repeating the same contract is allowed; supplying another is rejected.
An unknown key without a contract creates a free-form thread.

Root spans do not automatically finish a keyed thread. The boolean
`threadify.run.complete` marker explicitly finishes a free-form keyed thread after
all spans in the batch are recorded; contracts control their own completion.
Trace-only free-form threads complete after their root span. The internal
trace-only fallback retains the legacy creation operation to preserve the Engine's
separate trace-ID namespace. Trace and span IDs remain in step context and span
idempotency keys. Export failures, including completion errors, are returned.

This WebSocket exporter requires the matching updated Engine. Standard OTLP/HTTP
exporters can use `/v1/traces?use_workflow_run_id=false` instead; both paths use the
same Engine resolver. Existing stored threads are not retroactively merged.

Run `go test -race ./...` from this `otel` module.
