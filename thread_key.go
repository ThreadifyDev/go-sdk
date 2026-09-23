package threadify

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// ThreadOptions are creation defaults. Existing threads retain their stored
// contract version, label, refs and tags. Role is used when creating a contract thread.
type ThreadOptions struct {
	Label       string
	Contract    string
	Refs        map[string]string
	Tags        []string
	ServiceName string
	Role        string
}

// Thread atomically creates or resumes a tenant-scoped application key.
// Omit options on later requests; terminal threads and conflicting contracts are rejected.
func (c *Connection) Thread(ctx context.Context, threadKey string, options ...ThreadOptions) (*ThreadInstance, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	threadKey = strings.TrimSpace(threadKey)
	if threadKey == "" || len(threadKey) > 1024 || !utf8.ValidString(threadKey) {
		return nil, fmt.Errorf("threadKey must be a non-empty string of at most 1024 UTF-8 bytes")
	}
	if len(options) > 1 {
		return nil, fmt.Errorf("Thread accepts at most one ThreadOptions value")
	}
	var opts ThreadOptions
	if len(options) == 1 {
		opts = options[0]
	}
	for _, tag := range opts.Tags {
		if strings.TrimSpace(tag) == "" {
			return nil, fmt.Errorf("tags must contain non-empty strings")
		}
	}
	for key := range opts.Refs {
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("ref keys must be non-empty strings")
		}
	}
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected. Call Connect() first")
	}
	msg := map[string]any{FieldAction: "thread", "threadKey": threadKey, FieldService: firstNonEmpty(opts.ServiceName, c.serviceName)}
	if opts.Label != "" {
		msg["label"] = opts.Label
	}
	if opts.Contract != "" {
		msg[FieldContractName] = opts.Contract
	}
	if opts.Role != "" {
		msg[FieldRole] = opts.Role
	}
	if opts.Refs != nil {
		msg[FieldRefs] = opts.Refs
	}
	if opts.Tags != nil {
		msg[FieldTags] = opts.Tags
	}
	timeout := c.requestTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	resp, err := c.correlatedRequest(ctx, msg, timeout)
	if err != nil {
		return nil, fmt.Errorf("resolve thread: %w", err)
	}
	// Decode the wire response once into its actual types, including the pinned version.
	var data struct {
		ThreadID        string            `json:"threadId"`
		ThreadKey       string            `json:"threadKey"`
		Label           string            `json:"label"`
		ContractID      string            `json:"contractId"`
		ContractName    string            `json:"contractName"`
		ContractVersion int               `json:"contractVersion"`
		Refs            map[string]string `json:"refs"`
		Tags            []string          `json:"tags"`
		Role            string            `json:"role"`
		AccessLevel     string            `json:"accessLevel"`
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, fmt.Errorf("invalid thread response: %w", err)
	}
	if data.ThreadID == "" || data.ThreadKey != threadKey {
		return nil, fmt.Errorf("Engine returned an invalid thread identity")
	}
	thread := newThreadInstance(c, data.ThreadID, data.ContractID, data.Role, data.AccessLevel, data.Refs)
	thread.ThreadKey, thread.Label = data.ThreadKey, data.Label
	thread.ContractName, thread.ContractVersion, thread.Tags = data.ContractName, data.ContractVersion, data.Tags
	// Each resolution gets fresh metadata without racing with readers of an older
	// handle. Runtime waits and invocation grants remain shared for notification routing.
	if existing, loaded := c.threads.LoadOrStore(thread.ThreadID, thread); loaded {
		thread.shared = existing.(*ThreadInstance).runtime()
	}
	return thread, nil
}
