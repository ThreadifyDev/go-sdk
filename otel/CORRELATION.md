# OTel thread references

The exporter selects an explicit `threadify.thread_id`, then `threadify.external_ref`, then `workflow.run_id`, then the trace ID. Different traces with the same external/workflow reference share one thread and contract. The Engine rejects conflicting references or contracts instead of moving steps.

`UseWorkflowRunID` defaults to enabled when nil. To disable only the workflow fallback:

```go
useWorkflow := false
exporter := otel.NewSpanExporter(connection, otel.SpanExporterOptions{
    UseWorkflowRunID: &useWorkflow,
})
```

Set the reference on all relevant spans or a resource dedicated to that run. Use unique run references, not category names. References are trimmed, case-sensitive strings of at most 1024 UTF-8 bytes. Blank values fall through to the next identity choice. Root spans do not automatically finish a shared thread; the Boolean `threadify.run.complete` marker explicitly finishes the run. Trace and span IDs remain in step context and span idempotency keys.

This WebSocket exporter requires the matching updated Engine. Standard OTLP/HTTP exporters can use `/v1/traces?use_workflow_run_id=false` instead; both paths use the same Engine resolver. Existing stored threads are not retroactively merged.

Run `go test -race ./...` from this `otel` module.
