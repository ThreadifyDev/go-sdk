package threadify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type decisionRoundTrip func(*http.Request) (*http.Response, error)

func (f decisionRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestThreadDecisionQueriesUseGraphQLAndReturnTypedResults(t *testing.T) {
	var calls []map[string]any
	client := NewGraphQLClient("https://engine.example/graphql", "test-key")
	client.client = &http.Client{Transport: decisionRoundTrip(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("X-API-Key") != "test-key" {
			t.Fatal("GraphQL credential missing")
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		variables := body["variables"].(map[string]any)
		if variables["threadId"] != "thread-1" {
			t.Fatal("wrong thread ID")
		}
		calls = append(calls, variables)
		response := `{"data":{"next":{"threadId":"thread-1","paths":[{"actions":["issue_refund"],"status":"allowed","reason":"eligible"}]}}}`
		query := body["query"].(string)
		if strings.Contains(query, "query Can") {
			response = `{"data":{"can":{"threadId":"thread-1","stepName":"issue_refund","allowed":true,"status":"allowed","matchedBy":"classifier","requiredSteps":[],"satisfiedSteps":[],"missingSteps":[],"previousStep":null,"reason":"eligible"}}}`
		} else if strings.Contains(query, "query Should") {
			response = `{"data":{"should":{"threadId":"thread-1","stepName":"issue_refund","eligible":true,"recommendation":"uncertain","reason":"unclear"}}}`
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
	})}
	connection := &Connection{dataRetriever: &DataRetriever{client: client}}
	connection.dataRetrieverOnce.Do(func() {})
	thread := &ThreadInstance{conn: connection, ThreadID: "thread-1"}
	can, err := thread.Can(context.Background(), DecisionCandidate{Goal: "I want to process a refund", Context: map[string]any{"amount": 12}})
	if err != nil || !can.Allowed || can.MatchedBy != "classifier" {
		t.Fatalf("can: %#v, %v", can, err)
	}
	should, err := thread.Should(context.Background(), DecisionCandidate{Action: "issue_refund"})
	if err != nil || should.Recommendation != "uncertain" {
		t.Fatalf("should: %#v, %v", should, err)
	}
	next, err := thread.Next(context.Background())
	if err != nil || len(next.Paths) != 1 || next.Paths[0].Actions[0] != "issue_refund" {
		t.Fatalf("next: %#v, %v", next, err)
	}
	if calls[0]["goal"] != "I want to process a refund" || calls[1]["action"] != "issue_refund" || len(calls[2]) != 1 {
		t.Fatalf("wrong query variables: %#v", calls)
	}
	if _, err := thread.Can(context.Background(), DecisionCandidate{Action: "issue_refund", Goal: "refund"}); err == nil {
		t.Fatal("accepted both action and goal")
	}
}
