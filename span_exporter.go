package threadify

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type SpanExporter struct {
	conn    *Connection
	options SpanExporterOptions

	mu             sync.Mutex
	traceThreadMap map[string]*ThreadInstance
}

type SpanExporterOptions struct {
	Refs []string
}

func NewSpanExporter(conn *Connection, opts SpanExporterOptions) *SpanExporter {
	if opts.Refs == nil {
		opts.Refs = []string{}
	}
	return &SpanExporter{
		conn:           conn,
		options:        opts,
		traceThreadMap: make(map[string]*ThreadInstance),
	}
}

func (e *SpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if !e.conn.IsConnected() {
		return fmt.Errorf("threadify connection is not open")
	}

	for _, span := range spans {
		if err := e.processSpan(ctx, span); err != nil {
			e.conn.logger.Debug("failed to process span", "error", err)
		}
	}
	return nil
}

func (e *SpanExporter) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.traceThreadMap = nil
	return nil
}

func (e *SpanExporter) processSpan(ctx context.Context, span sdktrace.ReadOnlySpan) error {
	thread, err := e.getOrStartThread(ctx, span)
	if err != nil {
		return err
	}

	stepName := span.Name()
	if v := attrString(span.Attributes(), "threadify.step_name"); v != "" {
		stepName = v
	}

	step := thread.Step(stepName)

	contextData := make(map[string]any)
	refs := map[string]string{
		"otel_trace_id": span.SpanContext().TraceID().String(),
		"otel_span_id":  span.SpanContext().SpanID().String(),
	}

	refKeys := refKeySet(e.options.Refs)
	skipKeys := map[string]bool{
		"threadify.thread_id": true,
		"threadify.contract":  true,
		"threadify.label":     true,
		"threadify.step_name": true,
		"threadify.role":      true,
		"threadify.service":   true,
	}

	for _, attr := range span.Attributes() {
		key := string(attr.Key)
		if skipKeys[key] {
			continue
		}

		if refKeys[key] || hasPrefix(key, "threadify.ref.") {
			refKey := key
			if hasPrefix(key, "threadify.ref.") {
				refKey = key[len("threadify.ref."):]
			}
			refs[refKey] = attrValueToString(attr.Value)
		} else if hasPrefix(key, "threadify.context.") {
			contextData[key[len("threadify.context."):]] = attrValueToAny(attr.Value)
		} else {
			contextData[key] = attrValueToAny(attr.Value)
		}
	}

	if len(contextData) > 0 {
		step.AddContext(contextData)
	}
	if len(refs) > 0 {
		step.AddRefs(refs)
	}

	if !span.StartTime().IsZero() {
		step.event[FieldStartedAt] = span.StartTime().UTC().Format(time.RFC3339Nano)
	}
	if !span.EndTime().IsZero() {
		step.event[FieldFinishedAt] = span.EndTime().UTC().Format(time.RFC3339Nano)
	}

	for _, evt := range span.Events() {
		payload := make(map[string]any, len(evt.Attributes))
		for _, attr := range evt.Attributes {
			payload[string(attr.Key)] = attrValueToAny(attr.Value)
		}
		step.SubStep(evt.Name, payload)
	}

	statusCode := span.Status().Code
	var targetStatus string
	var message string

	switch {
	case statusCode == 2: // Error
		targetStatus = StatusFailed
		message = span.Status().Description
		if message == "" {
			message = "Span ended with error status"
		}
	default:
		targetStatus = StatusSuccess
		message = span.Status().Description
	}

	var resultErr error
	if targetStatus == StatusSuccess {
		_, resultErr = step.Success(ctx, message)
	} else {
		_, resultErr = step.Failed(ctx, message)
	}

	parentSpanID := span.Parent().SpanID()
	if !parentSpanID.IsValid() {
		if targetStatus == StatusSuccess {
			_, _ = thread.Complete(ctx, "Root span completed successfully")
		} else {
			_, _ = thread.Cancel(ctx, message)
		}
		e.mu.Lock()
		delete(e.traceThreadMap, span.SpanContext().TraceID().String())
		e.mu.Unlock()
	}

	return resultErr
}

func (e *SpanExporter) getOrStartThread(ctx context.Context, span sdktrace.ReadOnlySpan) (*ThreadInstance, error) {
	traceID := span.SpanContext().TraceID().String()

	e.mu.Lock()
	thread, ok := e.traceThreadMap[traceID]
	e.mu.Unlock()

	if ok {
		return thread, nil
	}

	existingThreadID := attrString(span.Attributes(), "threadify.thread_id")
	if existingThreadID != "" {
		role := attrString(span.Attributes(), "threadify.role")
		if role == "" {
			role = "participant"
		}
		thread, err := e.conn.Join(ctx, WithJoinThreadID(existingThreadID), WithJoinRole(role))
		if err != nil {
			return nil, err
		}
		e.cacheThread(traceID, thread)
		return thread, nil
	}

	archived, err := e.conn.GetThreadsByRef(ctx, &RefQuery{
		RefKey:   "otel_trace_id",
		RefValue: traceID,
	})
	if err == nil && len(archived) > 0 {
		role := attrString(span.Attributes(), "threadify.role")
		if role == "" {
			role = "participant"
		}
		thread, err := e.conn.Join(ctx, WithJoinThreadID(archived[0].ID), WithJoinRole(role))
		if err != nil {
			return nil, err
		}
		e.cacheThread(traceID, thread)
		return thread, nil
	}

	contractName := attrString(span.Attributes(), "threadify.contract")
	label := attrString(span.Attributes(), "threadify.label")
	if label == "" {
		label = span.Name()
	}
	serviceName := attrString(span.Attributes(), "threadify.service")
	if serviceName == "" {
		serviceName = e.conn.serviceName
	}

	thread, err = e.conn.Start(ctx, label, contractName, WithService(serviceName))
	if err != nil {
		return nil, err
	}

	e.cacheThread(traceID, thread)
	return thread, nil
}

func (e *SpanExporter) cacheThread(traceID string, thread *ThreadInstance) {
	e.mu.Lock()
	e.traceThreadMap[traceID] = thread
	e.mu.Unlock()

	time.AfterFunc(10*time.Minute, func() {
		e.mu.Lock()
		delete(e.traceThreadMap, traceID)
		e.mu.Unlock()
	})
}

func refKeySet(refs []string) map[string]bool {
	m := make(map[string]bool, len(refs))
	for _, r := range refs {
		m[r] = true
	}
	return m
}

func attrString(attrs []attribute.KeyValue, key string) string {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}
	return ""
}

func attrValueToAny(v attribute.Value) any {
	switch v.Type() {
	case attribute.BOOL:
		return v.AsBool()
	case attribute.INT64:
		return v.AsInt64()
	case attribute.FLOAT64:
		return v.AsFloat64()
	case attribute.STRING:
		return v.AsString()
	case attribute.BOOLSLICE:
		return v.AsBoolSlice()
	case attribute.INT64SLICE:
		return v.AsInt64Slice()
	case attribute.FLOAT64SLICE:
		return v.AsFloat64Slice()
	case attribute.STRINGSLICE:
		return v.AsStringSlice()
	default:
		return v.AsString()
	}
}

func attrValueToString(v attribute.Value) string {
	switch v.Type() {
	case attribute.STRING:
		return v.AsString()
	default:
		return fmt.Sprintf("%v", attrValueToAny(v))
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

var _ sdktrace.SpanExporter = (*SpanExporter)(nil)
