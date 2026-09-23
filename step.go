package threadify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"
)

type ThreadStep struct {
	stepName    string
	thread      *ThreadInstance
	serviceName string

	manualIdempotencyKey string
	subSteps             []SubStepData
	event                map[string]any
	context              map[string]string
	metadata             map[string]any
	err                  error
	reportOptions        ReportOptions
}

func newThreadStep(stepName string, thread *ThreadInstance, serviceName string) *ThreadStep {
	return &ThreadStep{
		stepName:    stepName,
		thread:      thread,
		serviceName: serviceName,
		context:     make(map[string]string),
		event: map[string]any{
			FieldAction:     ActionRecordThreadEvent,
			FieldThreadID:   thread.ThreadID,
			FieldStepName:   stepName,
			FieldStartedAt:  nowISO(),
			FieldFinishedAt: nil,
			FieldStatus:     StatusInProgress,
			FieldService:    serviceName,
		},
	}
}

func (s *ThreadStep) IdempotencyKey(key string) *ThreadStep {
	if s.err != nil {
		return s
	}
	if strings.TrimSpace(key) == "" {
		s.err = fmt.Errorf("idempotency key must be a non-empty string")
		return s
	}
	s.manualIdempotencyKey = key
	return s
}

func (s *ThreadStep) AddContext(data map[string]any) *ThreadStep {
	if s.err != nil || data == nil {
		return s
	}
	for k, v := range data {
		s.context[k] = fmt.Sprintf("%v", v)
	}
	return s
}

func (s *ThreadStep) AddPrivateContext(data map[string]any) *ThreadStep {
	if s.err != nil || data == nil {
		return s
	}
	for k, v := range data {
		str := fmt.Sprintf("%v", v)
		s.context[k] = str
		s.context["private_"+k] = str
	}
	return s
}

func (s *ThreadStep) SubStep(name string, data map[string]any, status ...string) *ThreadStep {
	if s.err != nil {
		return s
	}
	if strings.TrimSpace(name) == "" {
		s.err = fmt.Errorf("sub-step name must be a non-empty string")
		return s
	}

	st := StatusSuccess
	if len(status) > 0 && status[0] != "" {
		st = status[0]
		if st != StatusSuccess && st != StatusFailed {
			s.err = fmt.Errorf("sub-step status must be either %q or %q", StatusSuccess, StatusFailed)
			return s
		}
	}

	s.subSteps = append(s.subSteps, SubStepData{
		Name:       name,
		Status:     st,
		Payload:    data,
		RecordedAt: nowISO(),
	})
	return s
}

func (s *ThreadStep) Success(ctx context.Context, messageOrData ...any) (*StepResult, error) {
	return s.stop(ctx, StatusSuccess, messageOrData...)
}

func (s *ThreadStep) Failed(ctx context.Context, messageOrData ...any) (*StepResult, error) {
	return s.stop(ctx, StatusFailed, messageOrData...)
}

func (s *ThreadStep) Error(ctx context.Context, messageOrData ...any) (*StepResult, error) {
	return s.stop(ctx, StatusError, messageOrData...)
}

func (s *ThreadStep) stop(ctx context.Context, status string, messageOrData ...any) (*StepResult, error) {
	if s.err != nil {
		return nil, s.err
	}

	if err := s.prepareReport(status, messageOrData...); err != nil {
		return nil, err
	}

	result, err := s.sendEvent(ctx)
	if err != nil {
		var requestErr *RequestError
		if errors.As(err, &requestErr) {
			requestErr.IdempotencyKey = asString(s.event[FieldIdempotencyKey])
			requestErr.InvocationID = asString(s.event["invocationId"])
		}
		// Check for duplicate.
		if isDup, ok := err.(*duplicateError); ok && !s.reportOptions.WaitFor {
			return &StepResult{
				StepName:       s.stepName,
				ThreadID:       s.thread.ThreadID,
				Status:         status,
				IdempotencyKey: asString(s.event[FieldIdempotencyKey]),
				Timestamp:      firstNonEmpty(asString(s.event[FieldFinishedAt]), asString(s.event[FieldStartedAt])),
				Duplicate:      true,
			}, isDup
		}
		return nil, err
	}
	var validation *WaitResult
	if s.reportOptions.WaitFor {
		id := asString(result[FieldStepID])
		if id == "" {
			return nil, &RequestError{Code: CodeInvalidWaitResponse, Message: "Engine did not return an event ID"}
		}
		validation, err = checkedValidation(asMap(result["validation"]), id)
		if err != nil {
			return nil, err
		}
	}

	return &StepResult{
		StepID:         asString(result[FieldStepID]),
		Validation:     validation,
		StepName:       s.stepName,
		ThreadID:       s.thread.ThreadID,
		Status:         status,
		IdempotencyKey: asString(s.event[FieldIdempotencyKey]),
		Timestamp:      firstNonEmpty(asString(s.event[FieldFinishedAt]), asString(s.event[FieldStartedAt])),
	}, nil
}

type duplicateError struct {
	Message string
}

func (e *duplicateError) Error() string { return e.Message }

func IsDuplicateError(err error) bool {
	_, ok := err.(*duplicateError)
	return ok
}

func (s *ThreadStep) sendEvent(ctx context.Context) (map[string]any, error) {
	if s.thread.ThreadID == "" {
		return nil, fmt.Errorf("thread has not been resolved")
	}

	var resp map[string]any
	var err error
	if s.reportOptions.WaitFor || asString(s.event["invocationId"]) != "" {
		timeout, timeoutErr := waitTimeout(s.reportOptions.Timeout)
		if timeoutErr != nil {
			return nil, timeoutErr
		}
		event := make(map[string]any, len(s.event)+2)
		for k, v := range s.event {
			event[k] = v
		}
		if s.reportOptions.WaitFor {
			event["waitFor"] = true
			event["timeoutMs"] = int((timeout + time.Millisecond - 1) / time.Millisecond)
		}
		resp, err = s.thread.conn.correlatedRequest(ctx, event, timeout)
		var reqErr *RequestError
		if errors.As(err, &reqErr) && asBool(reqErr.Response[FieldIsDuplicate]) && !s.reportOptions.WaitFor {
			return nil, &duplicateError{Message: reqErr.Message}
		}
	} else {
		resp, err = s.thread.conn.request(ctx, s.event, func(m map[string]any) bool { return asString(m[FieldAction]) == ActionRecordThreadEvent })
	}
	if err != nil {
		return nil, fmt.Errorf("record step: %w", err)
	}

	if asString(resp[FieldStatus]) != StatusSuccess {
		msg := firstNonEmpty(asString(resp[FieldMessage]), "failed to record step event")
		if asBool(resp[FieldIsDuplicate]) {
			return nil, &duplicateError{Message: msg}
		}
		return nil, fmt.Errorf("%s", msg)
	}

	return resp, nil
}

func (s *ThreadStep) generateIdempotencyKey() string {
	if s.manualIdempotencyKey != "" {
		return s.manualIdempotencyKey
	}

	keys := make([]string, 0, len(s.context))
	for k := range s.context {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Use json.Marshal for each key and value so that special characters
	// (quotes, backslashes, control chars, etc.) are escaped exactly the
	// same way JavaScript's JSON.stringify escapes them. Without this, the
	// two SDKs produce different byte sequences and therefore different hashes.
	var sb strings.Builder
	sb.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(",")
		}
		keyBytes, _ := json.Marshal(k)
		valBytes, _ := json.Marshal(s.context[k])
		sb.Write(keyBytes)
		sb.WriteString(":")
		sb.Write(valBytes)
	}
	sb.WriteString("}")

	input := s.stepName + sb.String()

	h := fnv.New32a()
	_, _ = h.Write([]byte(input))
	return fmt.Sprintf("%08x", h.Sum32())
}

func (s *ThreadStep) GetEventData() map[string]any {
	out := make(map[string]any, len(s.event))
	for k, v := range s.event {
		out[k] = v
	}
	return out
}

func (s *ThreadStep) GetStepName() string {
	return s.stepName
}

func (s *ThreadStep) GetStatus() string {
	return asString(s.event[FieldStatus])
}

func (s *ThreadStep) GetContext() map[string]string {
	out := make(map[string]string, len(s.context))
	for k, v := range s.context {
		out[k] = v
	}
	return out
}

func (s *ThreadStep) handleStopMetadata(v any) {
	if v == nil {
		return
	}
	switch val := v.(type) {
	case string:
		if val != "" {
			if s.metadata == nil {
				s.metadata = make(map[string]any)
			}
			s.metadata[FieldMessage] = val
		}
	case map[string]any:
		if len(val) > 0 {
			if s.metadata == nil {
				s.metadata = make(map[string]any)
			}
			for k, v := range val {
				s.metadata[k] = v
			}
		}
	}
}

// RecordedTimes preserves source timestamps, including delayed OpenTelemetry spans.
func (s *ThreadStep) RecordedTimes(start, end time.Time) *ThreadStep {
	if !start.IsZero() {
		s.event[FieldStartedAt] = start.UTC().Format(time.RFC3339Nano)
	}
	if !end.IsZero() {
		s.event[FieldFinishedAt] = end.UTC().Format(time.RFC3339Nano)
	}
	return s
}

// SubStepAt records a span event at its original timestamp.
func (s *ThreadStep) SubStepAt(name string, data map[string]any, at time.Time) *ThreadStep {
	previous := len(s.subSteps)
	s.SubStep(name, data)
	if len(s.subSteps) > previous && !at.IsZero() {
		s.subSteps[previous].RecordedAt = at.UTC().Format(time.RFC3339Nano)
	}
	return s
}

func (s *ThreadStep) prepareReport(status string, messageOrData ...any) error {
	if asString(s.event[FieldFinishedAt]) == "" {
		s.event[FieldFinishedAt] = nowISO()
	}
	s.event[FieldStatus] = status
	s.event[FieldContext] = s.context

	for _, value := range messageOrData {
		if options, ok := value.(ReportOptions); ok {
			s.reportOptions = options
		} else {
			s.handleStopMetadata(value)
		}
	}
	if s.reportOptions.WaitFor {
		if _, err := waitTimeout(s.reportOptions.Timeout); err != nil {
			return err
		}
	}

	if s.metadata != nil {
		s.event[FieldMetadata] = s.metadata
	}

	if len(s.subSteps) > 0 {
		subStepMaps := make([]map[string]any, len(s.subSteps))
		for i, ss := range s.subSteps {
			subStepMaps[i] = map[string]any{
				FieldName:       ss.Name,
				FieldStatus:     ss.Status,
				FieldPayload:    ss.Payload,
				FieldRecordedAt: ss.RecordedAt,
			}
		}
		s.event[FieldSubSteps] = subStepMaps
	}

	if value, ok := s.thread.runtime().invocationGrants.LoadAndDelete(s.stepName); ok {
		grant := value.(*PermissionGrant)
		if current := asString(s.event["invocationId"]); current != "" && current != grant.InvocationID {
			return fmt.Errorf("invocation does not match the claimed step")
		}
		s.event["invocationId"] = grant.InvocationID
		if s.manualIdempotencyKey == "" {
			s.manualIdempotencyKey = grant.InvocationID
		}
	}
	s.event[FieldIdempotencyKey] = s.generateIdempotencyKey()

	return nil
}
