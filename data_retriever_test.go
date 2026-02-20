package threadify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGraphQLClient_Query_Success(t *testing.T) {
	responseData := map[string]any{
		"data": map[string]any{
			"thread": map[string]any{
				"id":           "thread-001",
				"contractName": "order_flow",
				"status":       "completed",
				"startedAt":    "2026-02-18T12:00:00Z",
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify headers.
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Errorf("expected X-API-Key test-key, got %q", r.Header.Get("X-API-Key"))
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseData)
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	ctx := context.Background()

	data, err := client.query(ctx, "query { thread(id: $id) { id } }", map[string]any{"id": "thread-001"})
	if err != nil {
		t.Fatalf("query error: %v", err)
	}

	threadData := asMap(data["thread"])
	if threadData == nil {
		t.Fatal("expected thread data")
	}
	if asString(threadData["id"]) != "thread-001" {
		t.Errorf("expected thread id 'thread-001', got %q", asString(threadData["id"]))
	}
}

func TestGraphQLClient_Query_GraphQLErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": nil,
			"errors": []map[string]any{
				{"message": "Thread not found"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	ctx := context.Background()

	_, err := client.query(ctx, "query { thread(id: $id) { id } }", map[string]any{"id": "x"})
	if err == nil {
		t.Error("expected error for GraphQL errors")
	}
}

func TestGraphQLClient_Query_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	ctx := context.Background()

	_, err := client.query(ctx, "query { }", nil)
	if err == nil {
		t.Error("expected error for HTTP 500")
	}
}

func TestDataRetriever_GetThread(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"thread": map[string]any{
					"id":              "thread-dr-001",
					"contractId":      "c-001",
					"contractName":    "order_flow",
					"contractVersion": "v1",
					"ownerId":         "owner-1",
					"companyId":       "company-1",
					"status":          "completed",
					"lastHash":        "abc123",
					"startedAt":       "2026-02-18T10:00:00Z",
					"completedAt":     "2026-02-18T12:00:00Z",
					"error":           "",
					"refs":            `{"orderId":"ORD-123"}`,
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	dr := NewDataRetriever(server.URL, "test-key")
	ctx := context.Background()

	thread, err := dr.GetThread(ctx, "thread-dr-001")
	if err != nil {
		t.Fatalf("GetThread error: %v", err)
	}

	if thread.ID != "thread-dr-001" {
		t.Errorf("expected ID 'thread-dr-001', got %q", thread.ID)
	}
	if thread.ContractName != "order_flow" {
		t.Errorf("expected contractName 'order_flow', got %q", thread.ContractName)
	}
	if thread.Status != "completed" {
		t.Errorf("expected status 'completed', got %q", thread.Status)
	}

	// Refs should be parsed from JSON string.
	if thread.Refs == nil {
		t.Fatal("expected Refs to be non-nil")
	}
	if asString(thread.Refs["orderId"]) != "ORD-123" {
		t.Errorf("expected ref orderId 'ORD-123', got %v", thread.Refs["orderId"])
	}
}

func TestDataRetriever_GetThread_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"thread": nil,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	dr := NewDataRetriever(server.URL, "test-key")
	ctx := context.Background()

	_, err := dr.GetThread(ctx, "nonexistent-id")
	if err == nil {
		t.Error("expected error for not found thread")
	}
}

func TestDataRetriever_GetThreadsByRef(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"threadsByRef": []any{
					map[string]any{
						"id":           "thread-ref-001",
						"contractName": "order_flow",
						"status":       "completed",
					},
					map[string]any{
						"id":           "thread-ref-002",
						"contractName": "order_flow",
						"status":       "in_progress",
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	dr := NewDataRetriever(server.URL, "test-key")
	ctx := context.Background()

	threads, err := dr.GetThreadsByRef(ctx, &RefQuery{
		RefKey:   "orderId",
		RefValue: "ORD-123",
	})
	if err != nil {
		t.Fatalf("GetThreadsByRef error: %v", err)
	}

	if len(threads) != 2 {
		t.Fatalf("expected 2 threads, got %d", len(threads))
	}
	if threads[0].ID != "thread-ref-001" {
		t.Errorf("expected first thread ID 'thread-ref-001', got %q", threads[0].ID)
	}
}

func TestDataRetriever_GetThreadsByRef_Empty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"threadsByRef": nil,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	dr := NewDataRetriever(server.URL, "test-key")
	ctx := context.Background()

	threads, err := dr.GetThreadsByRef(ctx, &RefQuery{
		RefKey:   "orderId",
		RefValue: "NONEXISTENT",
	})
	if err != nil {
		t.Fatalf("GetThreadsByRef error: %v", err)
	}

	if threads != nil {
		t.Errorf("expected nil for empty results, got %v", threads)
	}
}

func TestDataRetriever_GetThreadChain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"threadChain": []any{
					map[string]any{
						"id":     "root-001",
						"status": "completed",
					},
					map[string]any{
						"id":     "child-001",
						"status": "completed",
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	dr := NewDataRetriever(server.URL, "test-key")
	ctx := context.Background()

	chain, err := dr.GetThreadChain(ctx, "root-001", 3)
	if err != nil {
		t.Fatalf("GetThreadChain error: %v", err)
	}

	if len(chain) != 2 {
		t.Fatalf("expected 2 threads in chain, got %d", len(chain))
	}
}

func TestArchivedThread_Steps(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"thread": map[string]any{
					"steps": []any{
						map[string]any{
							"threadId":       "thread-steps-001",
							"stepName":       "order_placed",
							"idempotencyKey": "abc123",
							"status":         "success",
							"retryCount":     0.0,
							"firstSeenAt":    "2026-02-18T10:00:00Z",
							"lastUpdatedAt":  "2026-02-18T10:01:00Z",
							"history": []any{
								map[string]any{
									"attempt":   1.0,
									"status":    "success",
									"timestamp": "2026-02-18T10:01:00Z",
								},
							},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	thread := &ArchivedThread{
		ID:     "thread-steps-001",
		client: client,
	}

	ctx := context.Background()
	steps, err := thread.Steps(ctx, "", "", "")
	if err != nil {
		t.Fatalf("Steps() error: %v", err)
	}

	if len(steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(steps))
	}
	if steps[0].StepName != "order_placed" {
		t.Errorf("expected stepName 'order_placed', got %q", steps[0].StepName)
	}
	if steps[0].Status != StatusSuccess {
		t.Errorf("expected status 'success', got %q", steps[0].Status)
	}
	if steps[0].LastExecution == nil {
		t.Error("expected LastExecution to be populated")
	}
}

func TestArchivedStep_History(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"stepHistory": []any{
					map[string]any{
						"attempt":   1.0,
						"timestamp": "2026-02-18T10:00:00Z",
						"status":    "failed",
						"error":     "timeout",
						"duration":  1500.0,
					},
					map[string]any{
						"attempt":   2.0,
						"timestamp": "2026-02-18T10:01:00Z",
						"status":    "success",
						"error":     "",
						"duration":  200.0,
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	step := &ArchivedStep{
		ThreadID:       "thread-hist-001",
		StepName:       "payment_processed",
		IdempotencyKey: "idem-001",
		client:         client,
	}

	ctx := context.Background()
	history, err := step.History(ctx, nil)
	if err != nil {
		t.Fatalf("History() error: %v", err)
	}

	if len(history) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(history))
	}
	if asString(history[0]["status"]) != "failed" {
		t.Errorf("expected first attempt status 'failed', got %q", asString(history[0]["status"]))
	}
	if asString(history[1]["status"]) != "success" {
		t.Errorf("expected second attempt status 'success', got %q", asString(history[1]["status"]))
	}
}

func TestArchivedThread_ValidationResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"thread": map[string]any{
					"validationResults": []any{
						map[string]any{
							"validationId":         "val-001",
							"overallStatus":        "violated",
							"hasCriticalViolation": true,
							"criticalCount":        1.0,
							"warningCount":         2.0,
							"infoCount":            0.0,
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	thread := &ArchivedThread{
		ID:     "thread-val-001",
		client: client,
	}

	ctx := context.Background()
	results, err := thread.ValidationResults(ctx, 10)
	if err != nil {
		t.Fatalf("ValidationResults() error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 validation result, got %d", len(results))
	}
	if asString(results[0]["overallStatus"]) != "violated" {
		t.Errorf("expected overallStatus 'violated', got %q", asString(results[0]["overallStatus"]))
	}
}

func TestArchivedThread_GetCompleteData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"thread": map[string]any{
					"id":           "thread-complete-001",
					"contractName": "order_flow",
					"status":       "completed",
					"steps": []any{
						map[string]any{
							"stepName": "order_placed",
							"status":   "success",
						},
					},
					"validationResults": []any{
						map[string]any{
							"overallStatus": "passed",
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	thread := &ArchivedThread{
		ID:     "thread-complete-001",
		client: client,
	}

	ctx := context.Background()
	data, err := thread.GetCompleteData(ctx, nil)
	if err != nil {
		t.Fatalf("GetCompleteData() error: %v", err)
	}

	if asString(data["id"]) != "thread-complete-001" {
		t.Errorf("expected id 'thread-complete-001', got %v", data["id"])
	}

	steps := asSlice(data["steps"])
	if len(steps) != 1 {
		t.Errorf("expected 1 step in complete data, got %d", len(steps))
	}
}

func TestArchivedStep_SubSteps(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		response := map[string]any{
			"data": map[string]any{
				"thread": map[string]any{
					"steps": []any{
						map[string]any{
							"subSteps": []any{
								map[string]any{
									"name":       "inner-1",
									"status":     "success",
									"payload":    "done",
									"recordedAt": "2026-02-18T10:00:10Z",
								},
							},
						},
					},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := NewGraphQLClient(server.URL, "test-key")
	step := &ArchivedStep{
		ThreadID:       "thread-sub-001",
		StepName:       "outer-1",
		IdempotencyKey: "idem-sub-001",
		client:         client,
	}

	ctx := context.Background()
	subSteps, err := step.SubSteps(ctx)
	if err != nil {
		t.Fatalf("SubSteps() error: %v", err)
	}

	if len(subSteps) != 1 {
		t.Fatalf("expected 1 sub-step, got %d", len(subSteps))
	}
	if asString(subSteps[0]["name"]) != "inner-1" {
		t.Errorf("expected sub-step name 'inner-1', got %q", asString(subSteps[0]["name"]))
	}
}
