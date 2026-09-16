package otel

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	threadify "github.com/ThreadifyDev/go-sdk"
)

// SpanExporter translates OpenTelemetry spans into Threadify threads and steps.
type SpanExporter struct {
	conn    *threadify.Connection
	options SpanExporterOptions

	startMu         sync.Mutex
	mu              sync.Mutex
	traceThreadMap  map[string]*threadify.ThreadInstance
	traceReferences map[string]string
}

type SpanExporterOptions struct {
	// UseWorkflowRunID defaults to true when nil; explicit references always take precedence.
	UseWorkflowRunID *bool
	Refs             []string
	RefsMap          map[string]string
	Filters          []string
}

// NewSpanExporter creates a new OpenTelemetry exporter for Threadify.
func NewSpanExporter(conn *threadify.Connection, opts SpanExporterOptions) *SpanExporter {
	if opts.Refs == nil {
		opts.Refs = []string{}
	}
	if opts.Filters == nil {
		opts.Filters = []string{}
	}
	return &SpanExporter{
		conn:            conn,
		options:         opts,
		traceThreadMap:  make(map[string]*threadify.ThreadInstance),
		traceReferences: make(map[string]string),
	}
}

// ExportSpans converts spans to Threadify events.
func (e *SpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if !e.conn.IsConnected() {
		return fmt.Errorf("threadify connection is not open")
	}

	for _, span := range spans {
		if e.shouldDrop(span.Name()) {
			continue
		}
		if err := e.processSpan(ctx, span); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown does nothing for now as the connection handles its own closure.
func (e *SpanExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (e *SpanExporter) processSpan(ctx context.Context, span sdktrace.ReadOnlySpan) error {
	thread, err := e.getOrStartThread(ctx, span)
	if err != nil {
		return err
	}

	stepName := attrString(span.Attributes(), "threadify.step_name")
	if stepName == "" {
		stepName = span.Name()
	}

	step := thread.Step(stepName).IdempotencyKey("otel:" + span.SpanContext().TraceID().String() + ":" + span.SpanContext().SpanID().String())

	contextData := make(map[string]any)
	refs := map[string]string{"otel_trace_id": span.SpanContext().TraceID().String(), "otel_span_id": span.SpanContext().SpanID().String()}

	externalRef, err := e.externalRef(span)
	if err != nil {
		return err
	}
	if externalRef != "" {
		delete(refs, "otel_trace_id")
	}
	contextData["otel.trace_id"] = span.SpanContext().TraceID().String()
	contextData["otel.span_id"] = span.SpanContext().SpanID().String()
	refKeys := refKeySet(e.options.Refs)
	skipKeys := map[string]bool{
		"threadify.external_ref": true,
		"threadify.thread_id":    true,
		"threadify.contract":     true,
		"threadify.label":        true,
		"threadify.step_name":    true,
		"threadify.role":         true,
		"threadify.service":      true,
		"threadify.tags":         true,
	}

	for _, attr := range span.Attributes() {
		key := string(attr.Key)
		if skipKeys[key] {
			continue
		}

		if refKeys[key] || e.options.RefsMap[key] != "" || hasPrefix(key, "threadify.ref.") {
			refKey := key
			if mapped := e.options.RefsMap[key]; mapped != "" {
				refKey = mapped
			}
			if hasPrefix(key, "threadify.ref.") {
				refKey = key[len("threadify.ref."):]
			}
			if refKey != "threadify.external_ref" {
				refs[refKey] = attrValueToString(attr.Value)
			}
		} else if hasPrefix(key, "threadify.context.") {
			contextData[key[len("threadify.context."):]] = attrValueToAny(attr.Value)
		} else {
			contextData[key] = attrValueToAny(attr.Value)
		}
	}

	step.RecordedTimes(span.StartTime(), span.EndTime())

	if len(contextData) > 0 {
		step.AddContext(contextData)
	}
	if len(refs) > 0 {
		if err := thread.AddRefs(ctx, refs); err != nil {
			return fmt.Errorf("add span refs: %w", err)
		}
	}

	for _, evt := range span.Events() {
		payload := make(map[string]any, len(evt.Attributes))
		for _, attr := range evt.Attributes {
			payload[string(attr.Key)] = attrValueToAny(attr.Value)
		}
		step.SubStepAt(evt.Name, payload, evt.Time)
	}

	statusCode := span.Status().Code
	var targetStatus string
	var message string

	switch {
	case statusCode == 2: // Error
		targetStatus = "failed"
		message = span.Status().Description
		if message == "" {
			message = "Span ended with error status"
		}
	default:
		targetStatus = "success"
		message = span.Status().Description
	}

	var resultErr error
	if targetStatus == "success" {
		_, resultErr = step.Success(ctx, message)
	} else {
		_, resultErr = step.Failed(ctx, message)
	}

	if resultErr != nil {
		return resultErr
	}
	parentSpanID := span.Parent().SpanID()
	if (!parentSpanID.IsValid() && externalRef == "") || (externalRef != "" && attrBool(span.Attributes(), "threadify.run.complete")) {
		if targetStatus == "success" {
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

func (e *SpanExporter) getOrStartThread(ctx context.Context, span sdktrace.ReadOnlySpan) (*threadify.ThreadInstance, error) {
	e.startMu.Lock()
	defer e.startMu.Unlock()
	traceID := span.SpanContext().TraceID().String()
	externalRef, err := e.externalRef(span)
	if err != nil {
		return nil, err
	}
	if externalRef != "" {
		attrs := mergedSpanAttributes(span)
		label := attrString(attrs, "threadify.label")
		if label == "" {
			label = span.Name()
		}
		opts := []threadify.StartOption{threadify.WithRefs(map[string]any{"threadify.external_ref": externalRef, "otel_trace_id": traceID})}
		if c := attrString(attrs, "threadify.contract"); c != "" {
			opts = append(opts, threadify.WithContract(c))
		}
		if role := attrString(attrs, "threadify.role"); role != "" {
			opts = append(opts, threadify.WithRole(role))
		}
		if service := attrString(attrs, "threadify.service"); service != "" {
			opts = append(opts, threadify.WithService(service))
		}
		if tags := attrStringSlice(attrs, "threadify.tags"); len(tags) > 0 {
			opts = append(opts, threadify.WithTags(tags...))
		}
		thread, err := e.conn.Start(ctx, label, opts...)
		if err == nil {
			e.rememberReference(traceID, externalRef)
		}
		return thread, err
	}

	e.mu.Lock()
	thread, ok := e.traceThreadMap[traceID]
	e.mu.Unlock()

	if ok {
		return thread, nil
	}

	existingThreadID := attrString(mergedSpanAttributes(span), "threadify.thread_id")
	if existingThreadID != "" {
		role := attrString(span.Attributes(), "threadify.role")
		if role == "" {
			role = "participant"
		}
		thread, err := e.conn.Join(ctx, threadify.WithJoinThreadID(existingThreadID), threadify.WithJoinRole(role))
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

	opts := []threadify.StartOption{threadify.WithRefs(map[string]any{"otel_trace_id": traceID})}
	if role := attrString(span.Attributes(), "threadify.role"); role != "" {
		opts = append(opts, threadify.WithRole(role))
	}
	if serviceName != "" {
		opts = append(opts, threadify.WithService(serviceName))
	}
	if tags := attrStringSlice(span.Attributes(), "threadify.tags"); len(tags) > 0 {
		opts = append(opts, threadify.WithTags(tags...))
	}
	if contractName != "" {
		opts = append(opts, threadify.WithContract(contractName))
	}

	thread, err = e.conn.Start(ctx, label, opts...)
	if err != nil {
		return nil, err
	}

	e.cacheThread(traceID, thread)
	return thread, nil
}

func (e *SpanExporter) cacheThread(traceID string, thread *threadify.ThreadInstance) {
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

func attrStringSlice(attrs []attribute.KeyValue, key string) []string {
	for _, attr := range attrs {
		if string(attr.Key) != key {
			continue
		}
		switch attr.Value.Type() {
		case attribute.STRINGSLICE:
			return attr.Value.AsStringSlice()
		case attribute.STRING:
			if s := attr.Value.AsString(); s != "" {
				return []string{s}
			}
		}
	}
	return nil
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

func (e *SpanExporter) shouldDrop(name string) bool {
	for _, filter := range e.options.Filters {
		if filter == "" {
			continue
		}
		if strings.HasSuffix(filter, "*") {
			prefix := filter[:len(filter)-1]
			if strings.HasPrefix(name, prefix) {
				return true
			}
			continue
		}
		if name == filter {
			return true
		}
	}
	return false
}

var _ sdktrace.SpanExporter = (*SpanExporter)(nil)

// externalRef resolves caller-controlled identity without treating names as correlation.
func (e *SpanExporter) externalRef(span sdktrace.ReadOnlySpan) (string, error) {
	attrs := mergedSpanAttributes(span)
	if attrString(attrs, "threadify.thread_id") != "" {
		return "", nil
	}
	set := attribute.NewSet(attrs...)
	keys := []string{"threadify.external_ref"}
	if e.options.UseWorkflowRunID == nil || *e.options.UseWorkflowRunID {
		keys = append(keys, "workflow.run_id")
	}
	for _, key := range keys {
		value, ok := set.Value(attribute.Key(key))
		if !ok {
			continue
		}
		if value.Type() != attribute.STRING {
			return "", fmt.Errorf("%s must be a string", key)
		}
		ref := strings.TrimSpace(value.AsString())
		if len(ref) > 1024 {
			return "", fmt.Errorf("%s exceeds 1024 bytes", key)
		}
		if ref != "" {
			return ref, nil
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.traceReferences[span.SpanContext().TraceID().String()], nil
}

func attrBool(attrs []attribute.KeyValue, key string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.Type() == attribute.BOOL && attr.Value.AsBool()
		}
	}
	return false
}

func (e *SpanExporter) rememberReference(traceID, ref string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.traceReferences[traceID]; ok {
		return
	}
	e.traceReferences[traceID] = ref
	time.AfterFunc(10*time.Minute, func() { e.mu.Lock(); delete(e.traceReferences, traceID); e.mu.Unlock() })
}

func mergedSpanAttributes(span sdktrace.ReadOnlySpan) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if span.Resource() != nil {
		attrs = append(attrs, span.Resource().Attributes()...)
	}
	attrs = append(attrs, span.Attributes()...)
	set := attribute.NewSet(attrs...)
	return set.ToSlice()
}
