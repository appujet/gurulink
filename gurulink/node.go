package gurulink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/appujet/gurulink/lavalink"
	"github.com/gorilla/websocket"
)

// NodeConfig describes one Lavalink node.
type NodeConfig struct {
	// Name identifies the node in logs and in [Client.Node]. Required.
	Name string
	// Address is the node's host:port. Required.
	Address string
	// Password is the node's authorization header.
	Password string
	// Secure switches to https and wss.
	Secure bool
	// SessionID resumes an earlier session of this node, so the players it
	// still holds keep playing instead of being rebuilt.
	SessionID string
	// Regions are the Discord voice regions this node should serve, matched
	// against the endpoint a guild's voice server reports. Empty serves any.
	Regions []string
}

// NodeStatus is where a node is in its lifecycle.
type NodeStatus int

const (
	StatusDisconnected NodeStatus = iota
	StatusConnecting
	StatusConnected
	StatusClosing
)

func (s NodeStatus) String() string {
	switch s {
	case StatusDisconnected:
		return "disconnected"
	case StatusConnecting:
		return "connecting"
	case StatusConnected:
		return "connected"
	case StatusClosing:
		return "closing"
	}
	return "unknown"
}

// Node is one Lavalink server: a websocket for events and a REST API for
// commands. Its methods are safe for concurrent use.
type Node struct {
	cfg    NodeConfig
	client *Client
	rest   string
	wsURL  string
	log    *slog.Logger

	mu        sync.RWMutex
	status    NodeStatus
	sessionID string
	stats     lavalink.Stats
	conn      *websocket.Conn
	stop      context.CancelFunc
	attempts  int

	// writeMu guards the few control frames we write; gorilla allows only one
	// writer at a time.
	writeMu sync.Mutex
}

// Name is the node's configured name.
func (n *Node) Name() string { return n.cfg.Name }

// Config returns the node's configuration.
func (n *Node) Config() NodeConfig { return n.cfg }

// Client is the client owning this node.
func (n *Node) Client() *Client { return n.client }

// Status is where the node currently is in its lifecycle.
func (n *Node) Status() NodeStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.status
}

// SessionID is the session the node handed out, or "" before it did.
func (n *Node) SessionID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.sessionID
}

// Stats is the last load report the node sent.
func (n *Node) Stats() lavalink.Stats {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stats
}

// Available reports whether the node can take requests: connected, with a
// session id.
func (n *Node) Available() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.status == StatusConnected && n.sessionID != ""
}

// penalty scores how loaded the node is, so [Client.BestNode] can pick the
// least busy one. Lower is better; an unavailable node scores the worst.
//
// ponytail: players plus CPU load, no frame-deficit term. Add one if you find
// nodes that look idle while dropping frames.
func (n *Node) penalty() float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.status != StatusConnected || n.sessionID == "" {
		return math.Inf(1)
	}
	load := n.stats.CPU.SystemLoad
	if cores := n.stats.CPU.Cores; cores > 0 {
		load /= float64(cores)
	}
	return float64(n.stats.PlayingPlayers) + load*10
}

// serves reports whether the node is configured for a voice region. The
// endpoint Discord hands out looks like "us-east1234.discord.media".
func (n *Node) serves(endpoint string) bool {
	if len(n.cfg.Regions) == 0 {
		return true
	}
	endpoint = strings.ToLower(endpoint)
	for _, region := range n.cfg.Regions {
		if strings.Contains(endpoint, strings.ToLower(region)) {
			return true
		}
	}
	return false
}

// maxReconnectDelay caps the backoff between reconnect attempts.
const maxReconnectDelay = 60 * time.Second

// Open dials the node's websocket and starts reading it. It returns as soon as
// the connection is up; the handshake that follows arrives as a [ReadyEvent],
// and only then can players be created.
func (n *Node) Open(ctx context.Context) error {
	n.mu.Lock()
	if n.status == StatusConnecting || n.status == StatusConnected {
		status := n.status
		n.mu.Unlock()
		return fmt.Errorf("gurulink: node %s is already %s", n.cfg.Name, status)
	}
	n.status = StatusConnecting
	n.mu.Unlock()

	conn, err := n.dial(ctx)
	if err != nil {
		n.mu.Lock()
		n.status = StatusDisconnected
		n.mu.Unlock()
		return err
	}

	// The read loop outlives the caller's context on purpose: Open is often
	// called with a request-scoped one.
	loopCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.mu.Lock()
	n.conn, n.stop, n.status, n.attempts = conn, cancel, StatusConnected, 0
	n.mu.Unlock()

	n.log.Info("gurulink: node connected", slog.String("node", n.cfg.Name))
	n.client.emit(&ConnectEvent{Node: n})
	go n.listen(loopCtx, conn)
	return nil
}

// Close shuts the websocket down. Players stay put: with resuming on the node
// keeps playing them until the timeout runs out.
func (n *Node) Close() {
	n.mu.Lock()
	conn, stop := n.conn, n.stop
	n.status = StatusClosing
	n.mu.Unlock()

	if stop != nil {
		stop()
	}
	if conn == nil {
		return
	}
	n.writeMu.Lock()
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	n.writeMu.Unlock()
	_ = conn.Close()
}

func (n *Node) dial(ctx context.Context) (*websocket.Conn, error) {
	header := http.Header{}
	header.Set("Authorization", n.cfg.Password)
	header.Set("User-Id", n.client.cfg.UserID)
	header.Set("Client-Name", n.client.cfg.ClientName)
	if id := n.SessionID(); id != "" {
		header.Set("Session-Id", id)
	}
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, n.wsURL, header)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("gurulink: dial node %s: %s: %w", n.cfg.Name, resp.Status, err)
		}
		return nil, fmt.Errorf("gurulink: dial node %s: %w", n.cfg.Name, err)
	}
	return conn, nil
}

// listen reads frames until the connection breaks.
func (n *Node) listen(ctx context.Context, conn *websocket.Conn) {
	defer conn.Close()

	// A node that stops sending stats without closing the socket would leave
	// every player silently stuck, so ping it and give up if no pong arrives.
	if hb := n.client.cfg.Heartbeat; hb > 0 {
		deadline := func() error { return conn.SetReadDeadline(time.Now().Add(2 * hb)) }
		_ = deadline()
		conn.SetPongHandler(func(string) error { return deadline() })
		go n.ping(ctx, conn, hb)
	}

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			n.disconnected(err)
			return
		}
		n.handle(data)
	}
}

func (n *Node) ping(ctx context.Context, conn *websocket.Conn, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			n.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// disconnected reports the socket going away and reconnects unless the close
// was ours.
func (n *Node) disconnected(err error) {
	n.mu.Lock()
	expected := n.status == StatusClosing
	n.status = StatusDisconnected
	n.conn = nil
	n.mu.Unlock()

	code, reason := websocket.CloseAbnormalClosure, err.Error()
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		code, reason = closeErr.Code, closeErr.Text
	}
	n.log.Warn("gurulink: node disconnected", slog.String("node", n.cfg.Name),
		slog.Int("code", code), slog.String("reason", reason))
	n.client.emit(&DisconnectEvent{Node: n, Code: code, Reason: reason})

	if !expected && !n.client.stopped() {
		go n.reconnect()
	}
}

// reconnect keeps dialling with a growing delay until it works or gives up.
func (n *Node) reconnect() {
	cfg := n.client.cfg
	for {
		n.mu.Lock()
		n.attempts++
		attempt := n.attempts
		n.mu.Unlock()

		if cfg.MaxReconnects >= 0 && attempt > cfg.MaxReconnects {
			n.client.emit(&ErrorEvent{Node: n, Err: fmt.Errorf(
				"gurulink: node %s gave up after %d reconnect attempts", n.cfg.Name, attempt-1)})
			return
		}
		delay := min(cfg.ReconnectDelay*time.Duration(attempt), maxReconnectDelay)
		n.client.emit(&ReconnectEvent{Node: n, Attempt: attempt, Delay: delay})
		select {
		case <-time.After(delay):
		case <-n.client.done:
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := n.Open(ctx)
		cancel()
		if err == nil {
			return
		}
		n.log.Warn("gurulink: reconnect failed", slog.String("node", n.cfg.Name),
			slog.Int("attempt", attempt), slog.Any("err", err))
	}
}

// handle turns one gateway frame into events.
func (n *Node) handle(data []byte) {
	var frame struct {
		Op      lavalink.Op        `json:"op"`
		Type    lavalink.EventType `json:"type"`
		GuildID string             `json:"guildId"`
	}
	if err := json.Unmarshal(data, &frame); err != nil {
		n.client.emit(&ErrorEvent{Node: n, Err: fmt.Errorf("gurulink: bad frame: %w", err)})
		return
	}

	switch frame.Op {
	case lavalink.OpReady:
		ready := &ReadyEvent{Node: n}
		if !n.decode(data, ready) {
			return
		}
		n.mu.Lock()
		n.sessionID = ready.SessionID
		n.mu.Unlock()
		n.client.emit(ready)
		go n.afterReady(ready.Resumed)

	case lavalink.OpStats:
		var stats lavalink.Stats
		if !n.decode(data, &stats) {
			return
		}
		n.mu.Lock()
		n.stats = stats
		n.mu.Unlock()
		n.client.emit(&StatsEvent{Node: n, Stats: stats})

	case lavalink.OpPlayerUpdate:
		player := n.client.Player(frame.GuildID)
		if player == nil {
			return
		}
		update := &PlayerUpdateEvent{Player: player}
		if !n.decode(data, update) {
			return
		}
		player.setState(update.State)
		n.client.emit(update)

	case lavalink.OpEvent:
		n.handleEvent(frame.Type, frame.GuildID, data)

	default:
		n.client.emit(&UnknownEvent{Node: n, GuildID: frame.GuildID, Type: frame.Type, Data: data})
	}
}

func (n *Node) decode(data []byte, out any) bool {
	if err := json.Unmarshal(data, out); err != nil {
		n.client.emit(&ErrorEvent{Node: n, Err: fmt.Errorf("gurulink: decode frame: %w", err)})
		return false
	}
	return true
}

// afterReady turns resuming on and puts our players back on the node. A resumed
// session kept playing, so its players are reported instead of rebuilt.
func (n *Node) afterReady(resumed bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if timeout := n.client.cfg.Resuming; timeout > 0 {
		on, seconds := true, int(timeout.Seconds())
		if _, err := n.UpdateSession(ctx, lavalink.SessionUpdate{Resuming: &on, Timeout: &seconds}); err != nil {
			n.client.emit(&ErrorEvent{Node: n, Err: fmt.Errorf("gurulink: enable resuming: %w", err)})
		}
	}
	if resumed {
		// Whoever handles this needs to know what the node kept, and after a
		// process restart we have no record of it.
		infos, err := n.PlayerInfos(ctx)
		if err != nil {
			n.client.emit(&ErrorEvent{Node: n, Err: fmt.Errorf("gurulink: list resumed players: %w", err)})
		}
		n.client.emit(&ResumedEvent{Node: n, Players: infos})
		return
	}
	for _, player := range n.client.Players() {
		if player.Node() != n {
			continue
		}
		if err := player.restore(ctx); err != nil {
			n.client.emit(&ErrorEvent{Node: n, Err: fmt.Errorf(
				"gurulink: restore player %s: %w", player.GuildID(), err)})
		}
	}
}

// handleEvent decodes an op:event frame. Listeners see it before the player
// reacts, so a handler still sees the queue as it was when the event happened.
func (n *Node) handleEvent(typ lavalink.EventType, guildID string, data []byte) {
	player := n.client.Player(guildID)
	if player == nil {
		n.log.Debug("gurulink: event for unknown player",
			slog.String("guild_id", guildID), slog.String("type", string(typ)))
		return
	}

	var event Event
	switch typ {
	case lavalink.EventTypeTrackStart:
		event = &TrackStartEvent{Player: player}
	case lavalink.EventTypeTrackPromoted:
		event = &TrackPromotedEvent{Player: player}
	case lavalink.EventTypeTrackEnd:
		event = &TrackEndEvent{Player: player}
	case lavalink.EventTypeTrackException:
		event = &TrackExceptionEvent{Player: player}
	case lavalink.EventTypeTrackStuck:
		event = &TrackStuckEvent{Player: player}
	case lavalink.EventTypeWebSocketClosed:
		event = &WebSocketClosedEvent{Player: player}
	case lavalink.EventTypeSegmentsLoaded:
		event = &SegmentsLoadedEvent{Player: player}
	case lavalink.EventTypeSegmentSkipped:
		event = &SegmentSkippedEvent{Player: player}
	case lavalink.EventTypeChaptersLoaded:
		event = &ChaptersLoadedEvent{Player: player}
	case lavalink.EventTypeChapterStarted:
		event = &ChapterStartedEvent{Player: player}
	case lavalink.EventTypeLyricsFound:
		event = &LyricsFoundEvent{Player: player}
	case lavalink.EventTypeLyricsNotFound:
		event = &LyricsNotFoundEvent{Player: player}
	case lavalink.EventTypeLyricsLine:
		event = &LyricsLineEvent{Player: player}
	default:
		n.client.emit(&UnknownEvent{Node: n, Player: player, GuildID: guildID, Type: typ, Data: data})
		return
	}
	if !n.decode(data, event) {
		return
	}
	n.client.emit(event)
	player.handle(event)
}
