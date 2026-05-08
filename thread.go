package threadify

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

type ThreadInstance struct {
	conn        *Connection
	ThreadID    string
	ContractID  string
	Role        string
	AccessLevel string
	Refs        map[string]string

	steps        sync.Map
	pendingWaits sync.Map
}

type pendingWait struct {
	ch       chan *Notification
	cancel   context.CancelFunc
	statuses []string
}

func newThreadInstance(conn *Connection, threadID, contractID, role, accessLevel string, refs map[string]string) *ThreadInstance {
	return &ThreadInstance{
		conn:        conn,
		ThreadID:    threadID,
		ContractID:  contractID,
		Role:        role,
		AccessLevel: accessLevel,
		Refs:        refs,
	}
}

func (t *ThreadInstance) Step(stepName string) *ThreadStep {
	if t == nil {
		return &ThreadStep{err: fmt.Errorf("ThreadInstance is nil")}
	}

	step := newThreadStep(stepName, t, firstNonEmpty(t.conn.serviceName))
	if stepName == "" {
		step.err = fmt.Errorf("stepName cannot be empty")
	}

	t.steps.Store(stepName, step)
	return step
}

func (t *ThreadInstance) InviteParty(ctx context.Context, opts InviteOptions) (*InviteResponse, error) {
	if t == nil {
		return nil, fmt.Errorf("ThreadInstance is nil")
	}
	if err := requireNonEmpty("role", opts.Role); err != nil {
		return nil, fmt.Errorf("role is required for InviteParty")
	}

	accessLevel := opts.AccessLevel
	if accessLevel == "" {
		accessLevel = ForExternal
	}
	expiresIn := opts.ExpiresIn
	if expiresIn == "" {
		expiresIn = "24h"
	}

	msg := map[string]any{
		FieldAction:      ActionInviteParty,
		FieldThreadID:    t.ThreadID,
		FieldRole:        opts.Role,
		FieldAccessLevel: accessLevel,
		FieldExpiresIn:   expiresIn,
	}

	if err := t.send(msg); err != nil {
		return nil, err
	}

	resp, err := t.conn.waitResponse(ctx, func(m map[string]any) bool {
		return asString(m[FieldAction]) == ActionInviteParty
	})
	if err != nil {
		return nil, fmt.Errorf("invite party: %w", err)
	}

	if asString(resp[FieldStatus]) != StatusSuccess {
		return nil, fmt.Errorf("%s", firstNonEmpty(asString(resp[FieldMessage]), "failed to create invitation token"))
	}

	return &InviteResponse{
		Token:       asString(resp[FieldThreadToken]),
		ThreadID:    t.ThreadID,
		Role:        asString(resp[FieldRole]),
		AccessLevel: asString(resp[FieldAccessLevel]),
		ExpiresAt:   asString(resp[FieldExpiresAt]),
	}, nil
}

func (t *ThreadInstance) WaitFor(ctx context.Context, stepName string, opts *WaitOptions) (*Notification, error) {
	if t == nil {
		return nil, fmt.Errorf("ThreadInstance is nil")
	}
	if err := requireNonEmpty("stepName", stepName); err != nil {
		return nil, err
	}

	timeout := defaultWaitTimeout
	var statuses []string
	if opts != nil {
		if opts.Timeout > 0 {
			timeout = opts.Timeout
		}
		statuses = opts.Statuses
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	ch := make(chan *Notification, 1)

	pw := &pendingWait{
		ch:       ch,
		cancel:   cancel,
		statuses: statuses,
	}
	t.pendingWaits.Store(stepName, pw)

	select {
	case <-waitCtx.Done():
		cancel()
		t.pendingWaits.Delete(stepName)
		return nil, fmt.Errorf("timeout waiting for step: %s (%v)", stepName, timeout)
	case notif := <-ch:
		cancel()
		t.pendingWaits.Delete(stepName)
		return notif, nil
	}
}

func (t *ThreadInstance) AddRefs(ctx context.Context, refs map[string]string) error {
	if t == nil {
		return fmt.Errorf("ThreadInstance is nil")
	}
	if len(refs) == 0 {
		return fmt.Errorf("refs must be a non-empty map")
	}

	refsAny := make(map[string]any, len(refs))
	for k, v := range refs {
		refsAny[k] = v
	}

	msg := map[string]any{
		FieldAction:   ActionAddRefs,
		FieldThreadID: t.ThreadID,
		FieldRefs:     refsAny,
	}

	if err := t.send(msg); err != nil {
		return err
	}

	resp, err := t.conn.waitResponse(ctx, func(m map[string]any) bool {
		return asString(m[FieldAction]) == ActionAddRefs
	})
	if err != nil {
		return fmt.Errorf("add refs: %w", err)
	}

	if asString(resp[FieldStatus]) != StatusSuccess {
		return fmt.Errorf("%s", firstNonEmpty(asString(resp[FieldMessage]), "failed to add refs"))
	}

	// Update local refs.
	if t.Refs == nil {
		t.Refs = make(map[string]string)
	}
	for k, v := range refs {
		t.Refs[k] = v
	}

	return nil
}

var uuidRegex = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (t *ThreadInstance) LinkThread(ctx context.Context, threadID, relationship string) error {
	if t == nil {
		return fmt.Errorf("ThreadInstance is nil")
	}
	if err := requireNonEmpty("threadID", threadID); err != nil {
		return err
	}
	if !uuidRegex.MatchString(strings.ToLower(threadID)) {
		return fmt.Errorf("invalid thread ID format")
	}
	if relationship == "" {
		relationship = "parent"
	}

	refKey := "linkedThread:" + relationship
	return t.AddRefs(ctx, map[string]string{refKey: threadID})
}

// Cancel marks the thread as cancelled (for non-contract threads)
func (t *ThreadInstance) Cancel(ctx context.Context, reason ...string) (*ThreadEndResponse, error) {
	if t == nil {
		return nil, fmt.Errorf("ThreadInstance is nil")
	}
	reasonStr := ""
	if len(reason) > 0 {
		reasonStr = reason[0]
	}
	return t.endThread(ctx, StatusCancelled, reasonStr)
}

// Complete marks the thread as completed (for non-contract threads)
func (t *ThreadInstance) Complete(ctx context.Context, reason ...string) (*ThreadEndResponse, error) {
	if t == nil {
		return nil, fmt.Errorf("ThreadInstance is nil")
	}
	reasonStr := ""
	if len(reason) > 0 {
		reasonStr = reason[0]
	}
	return t.endThread(ctx, StatusCompleted, reasonStr)
}

func (t *ThreadInstance) endThread(ctx context.Context, status, reason string) (*ThreadEndResponse, error) {
	msg := map[string]any{
		FieldAction:   ActionThreadEnd,
		FieldThreadID: t.ThreadID,
		FieldStatus:   status,
	}
	if reason != "" {
		msg[FieldReason] = reason
	}

	if err := t.send(msg); err != nil {
		return nil, err
	}

	resp, err := t.conn.waitResponse(ctx, func(m map[string]any) bool {
		a := asString(m[FieldAction])
		return a == ActionThreadEnd
	})
	if err != nil {
		return nil, fmt.Errorf("end thread: %w", err)
	}

	if asString(resp[FieldStatus]) != StatusSuccess {
		return nil, fmt.Errorf("%s", firstNonEmpty(asString(resp[FieldMessage]), "failed to end thread"))
	}

	// Cleanup.
	t.cleanup()

	endedAt := firstNonEmpty(
		asString(resp[FieldClosedAt]),
		asString(resp[FieldCompletedAt]),
		asString(resp[FieldCancelledAt]),
		time.Now().UTC().Format(time.RFC3339),
	)

	return &ThreadEndResponse{
		ThreadID: t.ThreadID,
		Status:   asString(resp[FieldThreadStatus]),
		EndedAt:  endedAt,
		Message:  asString(resp["message"]),
	}, nil
}

func (t *ThreadInstance) send(msg map[string]any) error {
	return t.conn.send(msg)
}

func (t *ThreadInstance) handleNotification(notif *Notification) {
	val, ok := t.pendingWaits.Load(notif.StepName)
	if !ok {
		return
	}

	pw := val.(*pendingWait)

	// Check status filter.
	if len(pw.statuses) > 0 {
		matched := false
		for _, s := range pw.statuses {
			if s == notif.StepStatus {
				matched = true
				break
			}
		}
		if !matched {
			return
		}
	}

	// Deliver notification.
	select {
	case pw.ch <- notif:
	default:
	}
}

func (t *ThreadInstance) cleanup() {
	t.pendingWaits.Range(func(key, value any) bool {
		pw := value.(*pendingWait)
		pw.cancel()
		t.pendingWaits.Delete(key)
		return true
	})
	t.conn.threads.Delete(t.ThreadID)
}
