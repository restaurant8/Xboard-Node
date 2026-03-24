package panel

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
	"github.com/gorilla/websocket"
)

// WSEvent types
const (
	WSEventSyncConfig    = "sync.config"
	WSEventSyncUsers     = "sync.users"
	WSEventSyncUserDelta = "sync.user.delta"
)

// WSEvent is a parsed data event delivered to the service layer.
type WSEvent struct {
	Type        string
	Config      *NodeConfig
	Users       []User
	DeltaAction string // "add" or "remove" (only for sync.user.delta)
	DeltaUsers  []User // users affected by the delta
}

// WSStatusChange notifies the service when WS connectivity changes.
type WSStatusChange struct {
	Connected bool
}

// wsMessage is the JSON envelope for all WS messages.
type wsMessage struct {
	Event     string          `json:"event"`
	Data      json.RawMessage `json:"data,omitempty"`
	Timestamp int64           `json:"timestamp,omitempty"`
}

// Payload structures for data events
type syncConfigPayload struct {
	Config    NodeConfig `json:"config"`
	Timestamp int64      `json:"timestamp"`
}

type syncUsersPayload struct {
	Users     []User `json:"users"`
	Timestamp int64  `json:"timestamp"`
}

type syncUserDeltaPayload struct {
	Action    string `json:"action"`
	Users     []User `json:"users"`
	Timestamp int64  `json:"timestamp"`
}

// WSClient connects to the panel's Workerman WS server using native WebSocket.
// Authentication is done via query parameters (token + node_id) during the
// WebSocket handshake — no separate auth step needed.
type WSClient struct {
	wsURL    string // base WS URL, e.g. ws://panel.example.com:8076
	token    string
	nodeID   int
	onEvent  func(WSEvent)
	onStatus func(WSStatusChange)
	onPing   func() map[string]interface{}

	connected atomic.Bool
}

// NewWSClient creates a new WebSocket client.
// wsURL is the base WebSocket URL (e.g. "ws://panel.example.com:8076").
// token and nodeID are used for authentication via query parameters.
func NewWSClient(wsURL string, token string, nodeID int, onEvent func(WSEvent), onStatus func(WSStatusChange), onPing func() map[string]interface{}) *WSClient {
	return &WSClient{
		wsURL:    wsURL,
		token:    token,
		nodeID:   nodeID,
		onEvent:  onEvent,
		onStatus: onStatus,
		onPing:   onPing,
	}
}

func (w *WSClient) IsConnected() bool { return w.connected.Load() }

func (w *WSClient) notifyStatus(connected bool) {
	if w.onStatus != nil {
		w.onStatus(WSStatusChange{Connected: connected})
	}
}

// Run connects and reconnects until ctx is cancelled.
func (w *WSClient) Run(ctx context.Context) {
	backoff := time.Second
	for {
		start := time.Now()
		err := w.connect(ctx)

		wasConnected := w.connected.Swap(false)
		if wasConnected {
			w.notifyStatus(false)
		}

		if err != nil {
			nlog.Core().Warn("ws disconnected", "error", err)
			if !wasConnected {
				w.notifyStatus(false)
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		// Reset backoff if the connection was up for a meaningful duration.
		if time.Since(start) > 2*time.Minute {
			backoff = time.Second
		}

		// Apply exponential backoff with jitter to prevent thundering herd.
		jitter := time.Duration(rand.Int63n(int64(backoff / 5)))
		wait := backoff + jitter

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 60*time.Second {
			backoff = min(backoff*2, 60*time.Second)
		}
	}
}

func (w *WSClient) connect(ctx context.Context) error {
	// Build WS URL with auth query params
	u, err := url.Parse(w.wsURL)
	if err != nil {
		return fmt.Errorf("parse ws url: %w", err)
	}
	q := u.Query()
	q.Set("token", w.token)
	q.Set("node_id", strconv.Itoa(w.nodeID))
	u.RawQuery = q.Encode()

	nlog.Core().Debug("ws connecting", "url", u.Host)

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
	}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	conn.SetReadLimit(10 << 20) // 10MB max message size

	// Read first message — expect auth.success or error
	var firstMsg wsMessage
	if err := conn.ReadJSON(&firstMsg); err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}

	if firstMsg.Event == "error" {
		var errData struct {
			Message string `json:"message"`
		}
		if err := json.Unmarshal(firstMsg.Data, &errData); err != nil {
			return fmt.Errorf("auth failed (unable to parse error: %v)", err)
		}
		return fmt.Errorf("auth failed: %s", errData.Message)
	}

	if firstMsg.Event != "auth.success" {
		// It might be a data event already (server pushed sync before auth.success)
		// Process it and continue
		w.connected.Store(true)
		w.notifyStatus(true)
		w.handleMessage(firstMsg)
	} else {
		w.connected.Store(true)
		w.notifyStatus(true)
	}

	// Ping interval: send pong responses to server pings.
	// We also use this timer to trigger periodic status pushes (10s interval).
	const reportInterval = 10 * time.Second

	msgCh := make(chan wsMessage, 16)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var msg wsMessage
			if err := conn.ReadJSON(&msg); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case msgCh <- msg:
			case <-ctx.Done():
				return
			default:
				nlog.Core().Warn("ws: message channel full, dropping message", "event", msg.Event)
			}
		}
	}()

	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()

	// writeCh decouples data collection from network I/O.
	writeCh := make(chan wsMessage, 8)

	for {
		select {
		case <-ctx.Done():
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			<-done
			return nil

		case err := <-errCh:
			return fmt.Errorf("read: %w", err)

		case msg := <-msgCh:
			w.handleMessage(msg)
			if msg.Event == "ping" {
				// Queue an immediate pong
				select {
				case writeCh <- wsMessage{Event: "pong"}:
				default:
					nlog.Core().Warn("ws write channel full, skipping pong")
				}
			}

		case <-reportTicker.C:
			// Send periodic node.status via WebSocket
			if w.onPing != nil {
				msg := wsMessage{Event: "node.status"}
				if stats := w.onPing(); stats != nil {
					data, _ := json.Marshal(stats)
					msg.Data = data
					msg.Timestamp = time.Now().Unix()
				}
				select {
				case writeCh <- msg:
				default:
					nlog.Core().Warn("ws write channel full, skipping status push (network slow?)")
				}
			}

		case msg := <-writeCh:
			// Perform the actual network write asynchronously in this loop.
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteJSON(msg); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
	}
}

func (w *WSClient) handleMessage(msg wsMessage) {
	switch msg.Event {
	case "ping":
		// Server ping — handled by the pong timer reset above
		nlog.Core().Debug("ws received ping")

	case "auth.success":
		nlog.Core().Debug("ws auth confirmed")

	case WSEventSyncConfig:
		w.handleDataEvent(msg)

	case WSEventSyncUsers:
		w.handleDataEvent(msg)

	case WSEventSyncUserDelta:
		w.handleDataEvent(msg)

	default:
		nlog.Core().Debug("ws unknown event", "event", msg.Event)
	}
}

func (w *WSClient) handleDataEvent(msg wsMessage) {
	var event WSEvent
	event.Type = msg.Event

	// Helper to unmarshal and decode with weak Typing
	decodeData := func(data []byte, target interface{}) error {
		var raw map[string]interface{}
		if err := json.Unmarshal(data, &raw); err != nil {
			return err
		}
		return decodeWeakRaw(raw, target)
	}

	switch msg.Event {
	case WSEventSyncConfig:
		nlog.Core().Debug("ws sync config event received")
		var p syncConfigPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode config payload", "error", err)
			return
		}
		if p.Config.Protocol == "" {
			nlog.Core().Warn("ws: config payload missing protocol")
			return
		}
		event.Config = &p.Config

	case WSEventSyncUsers:
		nlog.Core().Debug("ws sync users event received")
		var p syncUsersPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode users payload", "error", err)
			return
		}
		if len(p.Users) == 0 {
			nlog.Core().Warn("ws: users payload empty")
			return
		}
		event.Users = p.Users

	case WSEventSyncUserDelta:
		nlog.Core().Debug("ws sync user delta event received")
		var p syncUserDeltaPayload
		if err := decodeData(msg.Data, &p); err != nil {
			nlog.Core().Warn("ws: cannot decode user delta payload", "error", err)
			return
		}
		if p.Action == "" {
			nlog.Core().Warn("ws: user delta payload missing action")
			return
		}
		if len(p.Users) == 0 {
			nlog.Core().Warn("ws: user delta payload has no users")
			return
		}
		event.DeltaAction = p.Action
		event.DeltaUsers = p.Users
	}

	w.onEvent(event)
}
