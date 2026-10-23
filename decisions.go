package threadify

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// DecisionCandidate names one authored action or describes one action goal.
// Context is used only by Can to validate candidate step content.
type DecisionCandidate struct {
	Action  string
	Goal    string
	Context map[string]any
}

type CanDecision struct {
	ThreadID       string   `json:"threadId"`
	StepName       string   `json:"stepName"`
	Allowed        bool     `json:"allowed"`
	Status         string   `json:"status"`
	MatchedBy      string   `json:"matchedBy"`
	RequiredSteps  []string `json:"requiredSteps"`
	SatisfiedSteps []string `json:"satisfiedSteps"`
	MissingSteps   []string `json:"missingSteps"`
	PreviousStep   *string  `json:"previousStep"`
	Reason         string   `json:"reason"`
}

type ShouldDecision struct {
	ThreadID       string `json:"threadId"`
	StepName       string `json:"stepName"`
	Eligible       bool   `json:"eligible"`
	Recommendation string `json:"recommendation"`
	Reason         string `json:"reason"`
}

type NextPath struct {
	Actions []string `json:"actions"`
	Status  string   `json:"status"`
	Reason  string   `json:"reason"`
}

type NextDecision struct {
	ThreadID string     `json:"threadId"`
	Paths    []NextPath `json:"paths"`
}

const canQuery = `query Can($threadId: ID!, $action: String, $goal: String, $context: JSON) {
  can(threadId: $threadId, action: $action, goal: $goal, context: $context) {
    threadId stepName allowed status matchedBy requiredSteps satisfiedSteps missingSteps previousStep reason
  }
}`
const shouldQuery = `query Should($threadId: ID!, $action: String, $goal: String) {
  should(threadId: $threadId, action: $action, goal: $goal) {
    threadId stepName eligible recommendation reason
  }
}`
const nextQuery = `query Next($threadId: ID!) {
  next(threadId: $threadId) { threadId paths { actions status reason } }
}`

func (candidate DecisionCandidate) variables() (map[string]any, error) {
	action, goal := strings.TrimSpace(candidate.Action), strings.TrimSpace(candidate.Goal)
	if (action == "") == (goal == "") {
		return nil, fmt.Errorf("provide exactly one non-empty action or goal")
	}
	if len(goal) > 4096 {
		return nil, fmt.Errorf("goal must be at most 4096 characters")
	}
	return map[string]any{"action": optionalDecisionString(action), "goal": optionalDecisionString(goal)}, nil
}

func optionalDecisionString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (t *ThreadInstance) decisionQuery(ctx context.Context, query, field string, variables map[string]any, target any) error {
	if t == nil || t.conn == nil || t.ThreadID == "" {
		return fmt.Errorf("thread is unavailable")
	}
	retriever, err := t.conn.getDataRetriever()
	if err != nil {
		return err
	}
	variables["threadId"] = t.ThreadID
	data, err := retriever.client.query(ctx, query, variables)
	if err != nil {
		return err
	}
	value, ok := data[field]
	if !ok || value == nil {
		return fmt.Errorf("GraphQL response has no %s decision", field)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}

// Can is a read-only Contract eligibility snapshot. WaitFor still claims execution.
func (t *ThreadInstance) Can(ctx context.Context, candidate DecisionCandidate) (*CanDecision, error) {
	variables, err := candidate.variables()
	if err != nil {
		return nil, err
	}
	variables["context"] = candidate.Context
	var decision CanDecision
	if err := t.decisionQuery(ctx, canQuery, "can", variables, &decision); err != nil {
		return nil, err
	}
	return &decision, nil
}

// Should is classifier advice for one candidate, never execution permission.
func (t *ThreadInstance) Should(ctx context.Context, candidate DecisionCandidate) (*ShouldDecision, error) {
	variables, err := candidate.variables()
	if err != nil {
		return nil, err
	}
	var decision ShouldDecision
	if err := t.decisionQuery(ctx, shouldQuery, "should", variables, &decision); err != nil {
		return nil, err
	}
	return &decision, nil
}

// Next lists bounded Contract-valid paths from the current Thread state.
func (t *ThreadInstance) Next(ctx context.Context) (*NextDecision, error) {
	var decision NextDecision
	if err := t.decisionQuery(ctx, nextQuery, "next", map[string]any{}, &decision); err != nil {
		return nil, err
	}
	return &decision, nil
}
