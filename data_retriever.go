package threadify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	queryKey     = "query"
	variablesKey = "variables"
	refkey       = "refKey"
	statusKey    = "status"
	refValue     = "refValue"
	rootIdKey    = "rootId"
	maxDepthKey  = "maxDepth"

	idempotency   = "idempotencyKey"
	startedAfter  = "startedAfter"
	startAt       = "startAt"
	endAt         = "endAt"
	threadID      = "threadId"
	stepName      = "stepName"
	activityType  = "activityType"
	actor         = "actor"
	startedBefore = "startedBefore"
	limit         = "limit"
	offset        = "offset"

	contentType  = "Content-Type"
	apiKeyHeader = "X-API-Key" //nolint:gosec // false positive: header name is not a credential
	jsonType     = "application/json"
)

const (
	threadFields = `
		id
		contractId
		contractName
		contractVersion
		ownerId
		companyId
		status
		lastHash
		startedAt
		completedAt
		error
		refs
	`

	stepFields = `
		threadId
		stepName
		idempotencyKey
		status
		retryCount
		firstSeenAt
		lastUpdatedAt
		latestStepID
		previousStep
		verified
		verificationError
	`

	stepHistoryFields = `
		attempt
		timestamp
		status
		context
		duration
		error
	`

	subStepFields = `
		id
		threadId
		stepId
		name
		status
		payload
		recordedAt
	`

	validationResultFields = `
		validationId
		threadId
		stepId
		stepName
		idempotencyKey
		timestamp
		validations {
			type
			message
			field
			expected
			actual
			rule
		}
		overallStatus
		hasCriticalViolation
		criticalCount
		warningCount
		infoCount
	`
)

type GraphQLClient struct {
	url    string
	apiKey string
	client *http.Client
}

func NewGraphQLClient(url, apiKey string) *GraphQLClient {
	return &GraphQLClient{
		url:    url,
		apiKey: apiKey,
		client: http.DefaultClient,
	}
}

func (g *GraphQLClient) query(ctx context.Context, gqlQuery string, variables map[string]any) (map[string]any, error) {
	body := map[string]any{
		queryKey:     gqlQuery,
		variablesKey: variables,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal GraphQL body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set(contentType, jsonType)
	req.Header.Set(apiKeyHeader, g.apiKey)

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GraphQL request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GraphQL request failed: %d %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Data   map[string]any `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}

	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("GraphQL errors: %s", result.Errors[0].Message)
	}

	return result.Data, nil
}

type DataRetriever struct {
	client *GraphQLClient
}

func NewDataRetriever(graphqlURL, apiKey string) *DataRetriever {
	return &DataRetriever{
		client: NewGraphQLClient(graphqlURL, apiKey),
	}
}

func (d *DataRetriever) GetThread(ctx context.Context, threadID string) (*ArchivedThread, error) {
	query := fmt.Sprintf(`
		query GetThread($id: ID!) {
			thread(id: $id) {
				%s
			}
		}
	`, threadFields)

	data, err := d.client.query(ctx, query, map[string]any{"id": threadID})
	if err != nil {
		return nil, err
	}

	threadData := asMap(data["thread"])
	if threadData == nil {
		return nil, fmt.Errorf("thread not found: %s", threadID)
	}

	return newArchivedThread(threadData, d.client), nil
}

func (d *DataRetriever) GetThreadsByRef(ctx context.Context, q *RefQuery) ([]*ArchivedThread, error) {
	query := fmt.Sprintf(`
		query GetThreadsByRef(
			$refKey: String!
			$refValue: String!
			$status: String
			$startedAfter: String
			$startedBefore: String
			$limit: Int
			$offset: Int
		) {
			threadsByRef(
				refKey: $refKey
				refValue: $refValue
				status: $status
				startedAfter: $startedAfter
				startedBefore: $startedBefore
				limit: $limit
				offset: $offset
			) {
				threads {
					%s
				}
				totalCount
			}
		}
	`, threadFields)

	variables := map[string]any{
		refkey:   q.RefKey,
		refValue: q.RefValue,
	}
	if q.Status != "" {
		variables[statusKey] = q.Status
	}
	if q.StartedAfter != "" {
		variables[startedAfter] = q.StartedAfter
	}
	if q.StartedBefore != "" {
		variables[startedBefore] = q.StartedBefore
	}
	if q.Limit > 0 {
		variables[limit] = q.Limit
	} else {
		variables[limit] = 50
	}
	if q.Offset > 0 {
		variables[offset] = q.Offset
	} else {
		variables[offset] = 0
	}

	data, err := d.client.query(ctx, query, variables)
	if err != nil {
		return nil, err
	}

	connection := asMap(data["threadsByRef"])
	if connection == nil {
		return nil, nil
	}

	threadsList := asSlice(connection["threads"])
	if threadsList == nil {
		return nil, nil
	}

	result := make([]*ArchivedThread, 0, len(threadsList))
	for _, t := range threadsList {
		if td, ok := t.(map[string]any); ok {
			result = append(result, newArchivedThread(td, d.client))
		}
	}

	return result, nil
}

func (d *DataRetriever) GetThreadChain(ctx context.Context, rootID string, maxDepth int) ([]*ArchivedThread, error) {
	if rootID == "" {
		return nil, fmt.Errorf("rootID is required")
	}

	if maxDepth <= 0 {
		maxDepth = 3
	}

	query := fmt.Sprintf(`
		query GetThreadChain($rootId: ID!, $maxDepth: Int) {
			threadChain(rootId: $rootId, maxDepth: $maxDepth) {
				%s
			}
		}
	`, threadFields)

	data, err := d.client.query(ctx, query, map[string]any{
		rootIdKey:   rootID,
		maxDepthKey: maxDepth,
	})
	if err != nil {
		return nil, err
	}

	chainList := asSlice(data["threadChain"])
	if chainList == nil {
		return nil, nil
	}

	result := make([]*ArchivedThread, 0, len(chainList))
	for _, t := range chainList {
		if td, ok := t.(map[string]any); ok {
			result = append(result, newArchivedThread(td, d.client))
		}
	}

	return result, nil
}

type ArchivedThread struct {
	ID              string
	ContractID      string
	ContractName    string
	ContractVersion string
	OwnerID         string
	CompanyID       string
	Status          string
	LastHash        string
	StartedAt       string
	CompletedAt     string
	Error           string
	Refs            map[string]any

	client *GraphQLClient
}

func newArchivedThread(data map[string]any, client *GraphQLClient) *ArchivedThread {
	var refs map[string]any
	if refsStr := asString(data["refs"]); refsStr != "" {
		_ = json.Unmarshal([]byte(refsStr), &refs)
	}

	return &ArchivedThread{
		ID:              asString(data["id"]),
		ContractID:      asString(data["contractId"]),
		ContractName:    asString(data["contractName"]),
		ContractVersion: asString(data["contractVersion"]),
		OwnerID:         asString(data["ownerId"]),
		CompanyID:       asString(data["companyId"]),
		Status:          asString(data["status"]),
		LastHash:        asString(data["lastHash"]),
		StartedAt:       asString(data["startedAt"]),
		CompletedAt:     asString(data["completedAt"]),
		Error:           asString(data["error"]),
		Refs:            refs,
		client:          client,
	}
}

func (at *ArchivedThread) Steps(ctx context.Context, stepName, idempotencyKey, status string) ([]*ArchivedStep, error) {
	if at.ID == "" {
		return nil, fmt.Errorf("thread ID is required")
	}

	query := fmt.Sprintf(`
		query GetThreadSteps($threadId: ID!, $stepName: String, $idempotencyKey: String, $status: String) {
			thread(id: $threadId) {
				steps(stepName: $stepName, idempotencyKey: $idempotencyKey, status: $status) {
					%s
					history(limit: 1) {
						%s
					}
				}
			}
		}
	`, stepFields, stepHistoryFields)

	variables := map[string]any{"threadId": at.ID}
	if stepName != "" {
		variables["stepName"] = stepName
	}
	if idempotencyKey != "" {
		variables[idempotency] = idempotencyKey
	}
	if status != "" {
		variables[statusKey] = status
	}

	data, err := at.client.query(ctx, query, variables)
	if err != nil {
		return nil, err
	}

	threadData := asMap(data["thread"])
	if threadData == nil {
		return nil, nil
	}

	stepsList := asSlice(threadData["steps"])
	if stepsList == nil {
		return nil, nil
	}

	result := make([]*ArchivedStep, 0, len(stepsList))
	for _, s := range stepsList {
		if sd, ok := s.(map[string]any); ok {
			result = append(result, newArchivedStep(sd, at.client))
		}
	}

	return result, nil
}

func (at *ArchivedThread) ValidationResults(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 10
	}

	query := fmt.Sprintf(`
		query GetThreadValidations($threadId: ID!, $options: ValidationQueryOptions) {
			thread(id: $threadId) {
				validationResults(options: $options) {
					%s
				}
			}
		}
	`, validationResultFields)

	data, err := at.client.query(ctx, query, map[string]any{
		"threadId": at.ID,
		"options":  map[string]any{"limit": limit},
	})
	if err != nil {
		return nil, err
	}

	threadData := asMap(data["thread"])
	if threadData == nil {
		return nil, nil
	}

	valList := asSlice(threadData["validationResults"])
	if valList == nil {
		return nil, nil
	}

	result := make([]map[string]any, 0, len(valList))
	for _, v := range valList {
		if vd, ok := v.(map[string]any); ok {
			result = append(result, vd)
		}
	}

	return result, nil
}

func (at *ArchivedThread) GetCompleteData(ctx context.Context, opts *CompleteDataOptions) (map[string]any, error) {
	if opts == nil {
		opts = &CompleteDataOptions{}
	}
	if opts.StepHistoryLimit <= 0 {
		opts.StepHistoryLimit = 50
	}
	if opts.ValidationLimit <= 0 {
		opts.ValidationLimit = 10
	}

	query := fmt.Sprintf(`
		query GetCompleteThread(
			$id: ID!
			$stepName: String
			$idempotencyKey: String
			$status: String
			$stepHistoryLimit: Int
			$validationLimit: Int
		) {
			thread(id: $id) {
				%s
				steps(stepName: $stepName, idempotencyKey: $idempotencyKey, status: $status) {
					%s
					history(limit: $stepHistoryLimit) {
						%s
					}
				}
				validationResults(options: {limit: $validationLimit}) {
					%s
				}
			}
		}
	`, threadFields, stepFields, stepHistoryFields, validationResultFields)

	variables := map[string]any{
		"id":               at.ID,
		"stepHistoryLimit": opts.StepHistoryLimit,
		"validationLimit":  opts.ValidationLimit,
	}
	if opts.StepName != "" {
		variables["stepName"] = opts.StepName
	}
	if opts.IdempotencyKey != "" {
		variables[idempotency] = opts.IdempotencyKey
	}
	if opts.Status != "" {
		variables[statusKey] = opts.Status
	}

	data, err := at.client.query(ctx, query, variables)
	if err != nil {
		return nil, err
	}

	threadData := asMap(data["thread"])
	if threadData == nil {
		return nil, fmt.Errorf("thread not found: %s", at.ID)
	}

	return threadData, nil
}

type ArchivedStep struct {
	ThreadID          string
	StepName          string
	IdempotencyKey    string
	Status            string
	RetryCount        int
	FirstSeenAt       string
	LastUpdatedAt     string
	LatestStepID      string
	PreviousStep      string
	Verified          bool
	VerificationError string
	LastExecution     map[string]any

	client *GraphQLClient
}

func newArchivedStep(data map[string]any, client *GraphQLClient) *ArchivedStep {
	var lastExec map[string]any
	if history := asSlice(data["history"]); len(history) > 0 {
		lastExec, _ = history[0].(map[string]any)
	}

	retryCount := 0
	if rc, ok := data["retryCount"].(float64); ok {
		retryCount = int(rc)
	}

	return &ArchivedStep{
		ThreadID:          asString(data["threadId"]),
		StepName:          asString(data["stepName"]),
		IdempotencyKey:    asString(data["idempotencyKey"]),
		Status:            asString(data["status"]),
		RetryCount:        retryCount,
		FirstSeenAt:       asString(data["firstSeenAt"]),
		LastUpdatedAt:     asString(data["lastUpdatedAt"]),
		LatestStepID:      asString(data["latestStepID"]),
		PreviousStep:      asString(data["previousStep"]),
		Verified:          asBool(data["verified"]),
		VerificationError: asString(data["verificationError"]),
		LastExecution:     lastExec,
		client:            client,
	}
}

func (as *ArchivedStep) History(ctx context.Context, opts *HistoryQueryOptions) ([]map[string]any, error) {
	if opts == nil {
		opts = &HistoryQueryOptions{}
	}
	if opts.Limit <= 0 {
		opts.Limit = 100
	}

	query := fmt.Sprintf(`
		query GetStepHistory(
			$threadId: String!
			$stepName: String!
			$idempotencyKey: String
			$limit: Int
			$offset: Int
			$startAt: String
			$endAt: String
			$activityType: String
			$actor: String
		) {
			stepHistory(
				threadId: $threadId
				stepName: $stepName
				idempotencyKey: $idempotencyKey
				limit: $limit
				offset: $offset
				startAt: $startAt
				endAt: $endAt
				activityType: $activityType
				actor: $actor
			) {
				%s
			}
		}
	`, stepHistoryFields)

	variables := map[string]any{
		"threadId": as.ThreadID,
		"stepName": as.StepName,
		"limit":    opts.Limit,
	}
	if as.IdempotencyKey != "" {
		variables["idempotencyKey"] = as.IdempotencyKey
	}
	if opts.Offset > 0 {
		variables[offset] = opts.Offset
	}
	if opts.StartAt != "" {
		variables[startAt] = opts.StartAt
	}
	if opts.EndAt != "" {
		variables[endAt] = opts.EndAt
	}
	if opts.ActivityType != "" {
		variables[activityType] = opts.ActivityType
	}
	if opts.Actor != "" {
		variables[actor] = opts.Actor
	}

	data, err := as.client.query(ctx, query, variables)
	if err != nil {
		return nil, err
	}

	histList := asSlice(data["stepHistory"])
	if histList == nil {
		return nil, nil
	}

	result := make([]map[string]any, 0, len(histList))
	for _, h := range histList {
		if hd, ok := h.(map[string]any); ok {
			result = append(result, hd)
		}
	}

	return result, nil
}

// SubSteps retrieves sub-steps for this step.
func (as *ArchivedStep) SubSteps(ctx context.Context) ([]map[string]any, error) {
	query := fmt.Sprintf(`
		query GetStepSubSteps(
			$threadId: String!
			$stepName: String!
			$idempotencyKey: String
		) {
			thread(id: $threadId) {
				steps(stepName: $stepName, idempotencyKey: $idempotencyKey) {
					subSteps {
						%s
					}
				}
			}
		}
	`, subStepFields)

	variables := map[string]any{
		threadID: as.ThreadID,
		stepName: as.StepName,
	}
	if as.IdempotencyKey != "" {
		variables[idempotency] = as.IdempotencyKey
	}

	data, err := as.client.query(ctx, query, variables)
	if err != nil {
		return nil, err
	}

	threadData := asMap(data["thread"])
	if threadData == nil {
		return nil, nil
	}

	stepsList := asSlice(threadData["steps"])
	if len(stepsList) == 0 {
		return nil, nil
	}

	firstStep, _ := stepsList[0].(map[string]any)
	if firstStep == nil {
		return nil, nil
	}

	subStepsList := asSlice(firstStep["subSteps"])
	if subStepsList == nil {
		return nil, nil
	}

	result := make([]map[string]any, 0, len(subStepsList))
	for _, ss := range subStepsList {
		if ssd, ok := ss.(map[string]any); ok {
			result = append(result, ssd)
		}
	}

	return result, nil
}
