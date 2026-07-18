package threadify

import (
	"context"
	"fmt"
	"time"
)

// validateContext prevents requests from being sent when their context can no
// longer accept a response. A nil context is always a caller error.
func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}
	return ctx.Err()
}

func boundedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if err := validateContext(parent); err != nil {
		return nil, nil, err
	}
	if timeout <= 0 {
		return nil, nil, fmt.Errorf("timeout must be greater than zero")
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	return ctx, cancel, nil
}
