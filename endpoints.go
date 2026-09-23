package threadify

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

func engineEndpoints(base string) (wsURL, graphqlURL string, err error) {
	base = strings.TrimSpace(base)
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" || u.Scheme != "http" && u.Scheme != "https" || u.User != nil || strings.ContainsAny(base, "?#") || strings.IndexFunc(base, unicode.IsSpace) >= 0 {
		return "", "", fmt.Errorf("EngineURL must be an absolute HTTP(S) URL without credentials, query, or fragment")
	}
	path := strings.TrimRight(u.Path, "/")
	rawPath := strings.TrimRight(u.EscapedPath(), "/")
	u.Path, u.RawPath = path+"/graphql", rawPath+"/graphql"
	gql := u.String()
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path, u.RawPath = path+"/threads", rawPath+"/threads"
	return u.String(), gql, nil
}

func referenceQuery(refs any, filters ...RefQuery) (*RefQuery, error) {
	var q RefQuery
	switch value := refs.(type) {
	case *RefQuery:
		if value == nil {
			return nil, fmt.Errorf("reference query is required")
		}
		q = *value
	case RefQuery:
		q = value
	case map[string]string:
		if len(value) != 1 {
			return nil, fmt.Errorf("refs must contain exactly one reference pair")
		}
		for k, v := range value {
			q.RefKey, q.RefValue = k, v
		}
	default:
		return nil, fmt.Errorf("use a reference map or RefQuery")
	}
	if len(filters) > 1 {
		return nil, fmt.Errorf("at most one filter object is allowed")
	}
	if len(filters) == 1 {
		key, value := q.RefKey, q.RefValue
		q = filters[0]
		q.RefKey, q.RefValue = key, value
	}
	if strings.TrimSpace(q.RefKey) == "" || strings.TrimSpace(q.RefValue) == "" {
		return nil, fmt.Errorf("reference key and value must be non-empty strings")
	}
	return &q, nil
}

func (c *Connection) GetThreadByRef(ctx context.Context, refs map[string]string) (*ArchivedThread, error) {
	threads, err := c.GetThreadsByRef(ctx, refs, RefQuery{Limit: 1})
	if err != nil || len(threads) == 0 {
		return nil, err
	}
	return threads[0], nil
}
