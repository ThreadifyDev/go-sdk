package threadify

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

type Transport interface {
	Send(msg map[string]any) error
	Recv() (map[string]any, error)
	Close() error
}

type Dialer interface {
	Dial(ctx context.Context, wsURL string) (Transport, error)
}

type GorillaDialer struct{}

func (d *GorillaDialer) Dial(ctx context.Context, wsURL string) (Transport, error) {
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket dial: %w", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return &GorillaTransport{conn: conn}, nil
}

type GorillaTransport struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (t *GorillaTransport) Send(msg map[string]any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conn.WriteMessage(websocket.TextMessage, data)
}

func (t *GorillaTransport) Recv() (map[string]any, error) {
	_, data, err := t.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return msg, nil
}

func (t *GorillaTransport) Close() error {
	return t.conn.Close()
}
