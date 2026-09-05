package gurulink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"

	"github.com/appujet/gurulink/lavalink"
)

// Client owns the nodes and one [Player] per guild. Safe for concurrent use.
type Client struct {
	cfg  Config
	done chan struct{}

	mu        sync.RWMutex
	nodes     []*Node
	players   map[string]*Player
	listeners []Listener
	closed    bool
}

// New builds a client. Give it nodes with [Client.AddNode].
func New(cfg Config) (*Client, error) {
	if cfg.UserID == "" {
		return nil, errors.New("gurulink: Config.UserID is required")
	}
	if cfg.SendVoiceUpdate == nil {
		return nil, errors.New("gurulink: Config.SendVoiceUpdate is required")
	}
	cfg = cfg.withDefaults()
	// Once here: a typo fails at startup and an alias works where a prefix does.
	source, ok := Source(cfg.DefaultSource)
	if !ok {
		return nil, fmt.Errorf("gurulink: unknown Config.DefaultSource %q", cfg.DefaultSource)
	}
	cfg.DefaultSource = source
	return &Client{
		cfg:       cfg,
		done:      make(chan struct{}),
		players:   make(map[string]*Player),
		listeners: slices.Clone(cfg.Listeners),
	}, nil
}

// Config returns the client's configuration, defaults filled in.
func (c *Client) Config() Config { return c.cfg }

// Logger is the client's logger.
func (c *Client) Logger() *slog.Logger { return c.cfg.Logger }

// AddListener registers listeners for every later event.
func (c *Client) AddListener(listeners ...Listener) {
	c.mu.Lock()
	// Copy on write, so [Client.emit] walks the slice without the lock.
	c.listeners = append(slices.Clone(c.listeners), listeners...)
	c.mu.Unlock()
}

// emit runs every listener in order on the caller's goroutine, the node's read
// loop: a listener that blocks stalls the node.
func (c *Client) emit(event Event) {
	c.mu.RLock()
	listeners := c.listeners
	c.mu.RUnlock()
	for _, listen := range listeners {
		listen(event)
	}
}

// AddNode registers a node and connects it. The node is usable once its
// [ReadyEvent] arrives.
func (c *Client) AddNode(ctx context.Context, cfg NodeConfig) (*Node, error) {
	if cfg.Name == "" || cfg.Address == "" {
		return nil, errors.New("gurulink: NodeConfig needs a Name and an Address")
	}
	scheme, ws := "http", "ws"
	if cfg.Secure {
		scheme, ws = "https", "wss"
	}
	node := &Node{
		cfg:       cfg,
		client:    c,
		rest:      scheme + "://" + cfg.Address,
		wsURL:     ws + "://" + cfg.Address + "/v4/websocket",
		sessionID: cfg.SessionID,
		log:       c.cfg.Logger,
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("gurulink: client is closed")
	}
	if slices.ContainsFunc(c.nodes, func(n *Node) bool { return n.cfg.Name == cfg.Name }) {
		c.mu.Unlock()
		return nil, fmt.Errorf("gurulink: node %s already exists", cfg.Name)
	}
	c.nodes = append(c.nodes, node)
	c.mu.Unlock()

	if err := node.Open(ctx); err != nil {
		return node, err
	}
	return node, nil
}

// Node returns the node with this name, or nil.
func (c *Client) Node(name string) *Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, node := range c.nodes {
		if node.cfg.Name == name {
			return node
		}
	}
	return nil
}

// Nodes returns every registered node.
func (c *Client) Nodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return slices.Clone(c.nodes)
}

// RemoveNode closes a node and moves its players; the homeless are destroyed.
func (c *Client) RemoveNode(ctx context.Context, name string) error {
	node := c.Node(name)
	if node == nil {
		return fmt.Errorf("gurulink: unknown node %s", name)
	}
	c.mu.Lock()
	c.nodes = slices.DeleteFunc(c.nodes, func(n *Node) bool { return n == node })
	c.mu.Unlock()

	for _, player := range c.Players() {
		if player.Node() != node {
			continue
		}
		target := c.BestNode()
		if target == nil {
			if err := player.Destroy(ctx, DestroyNodeGone); err != nil {
				c.cfg.Logger.Error("gurulink: destroy player of removed node",
					slog.String("guild_id", player.GuildID()), slog.Any("err", err))
			}
			continue
		}
		if err := player.MoveNode(ctx, target); err != nil {
			c.cfg.Logger.Error("gurulink: move player off removed node",
				slog.String("guild_id", player.GuildID()), slog.Any("err", err))
		}
	}
	node.Close(ctx)
	c.emit(&NodeRemovedEvent{Node: node})
	return nil
}

// BestNode is the least loaded available node, or nil when none is up.
func (c *Client) BestNode() *Node { return c.bestNode("") }

// bestNode is the least loaded node serving an endpoint. An empty one ignores
// regions; an unclaimed one falls back, since a far node beats no node.
func (c *Client) bestNode(endpoint string) *Node {
	// Held throughout: penalty only takes the node's own lock.
	c.mu.RLock()
	defer c.mu.RUnlock()

	var best, bestInRegion *Node
	var bestScore, bestRegionScore float64
	for _, node := range c.nodes {
		score := node.penalty()
		if math.IsInf(score, 1) {
			continue
		}
		if best == nil || score < bestScore {
			best, bestScore = node, score
		}
		if endpoint != "" && node.serves(endpoint) && (bestInRegion == nil || score < bestRegionScore) {
			bestInRegion, bestRegionScore = node, score
		}
	}
	if bestInRegion != nil {
		return bestInRegion
	}
	return best
}

// Player returns the guild's player, or nil when it has none.
func (c *Client) Player(guildID string) *Player {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.players[guildID]
}

// Players returns every player.
func (c *Client) Players() []*Player {
	c.mu.RLock()
	defer c.mu.RUnlock()
	players := make([]*Player, 0, len(c.players))
	for _, player := range c.players {
		players = append(players, player)
	}
	return players
}

// NewPlayer returns the guild's player, creating it on the least loaded node. It
// only does bookkeeping: [Player.Connect], then [Player.Play].
func (c *Client) NewPlayer(guildID string) (*Player, error) {
	if guildID == "" {
		return nil, errors.New("gurulink: guild id is required")
	}
	if existing := c.Player(guildID); existing != nil {
		return existing, nil
	}
	node := c.BestNode()
	if node == nil {
		return nil, errors.New("gurulink: no node available")
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("gurulink: client is closed")
	}
	// Another goroutine may have created it while we picked a node.
	if existing := c.players[guildID]; existing != nil {
		c.mu.Unlock()
		return existing, nil
	}
	player := newPlayer(c, node, guildID)
	c.players[guildID] = player
	c.mu.Unlock()

	c.emit(&PlayerCreateEvent{Player: player})
	return player, nil
}

// forget drops a destroyed player. Only [Player.Destroy] calls it.
func (c *Client) forget(guildID string) {
	c.mu.Lock()
	delete(c.players, guildID)
	c.mu.Unlock()
}

// Search resolves a user query on the least loaded node. source is a prefix or
// alias ("spotify", "yt"), empty for [Config.DefaultSource]; a prefix in the
// query wins, and a URL is used as it is.
func (c *Client) Search(ctx context.Context, query, source string) (lavalink.LoadResult, error) {
	node := c.BestNode()
	if node == nil {
		return lavalink.LoadResult{}, errors.New("gurulink: no node available")
	}
	return c.searchOn(ctx, node, query, source)
}

func (c *Client) searchOn(ctx context.Context, node *Node, query, source string) (lavalink.LoadResult, error) {
	identifier, err := c.identifier(query, source)
	if err != nil {
		return lavalink.LoadResult{}, err
	}
	return node.LoadTracks(ctx, identifier)
}

// VoiceStateUpdate is Discord's VOICE_STATE_UPDATE, cut down. Forward every one
// a guild sends; UserID tells other users apart, and empty means the bot.
type VoiceStateUpdate struct {
	GuildID string
	// ChannelID is empty when the user left voice altogether.
	ChannelID  string
	UserID     string
	SessionID  string
	SelfMute   bool
	SelfDeaf   bool
	ServerMute bool
	ServerDeaf bool
	// Suppress is set for anyone who is not a speaker in a stage channel.
	Suppress bool
}

// OnVoiceStateUpdate feeds Discord's VOICE_STATE_UPDATE in. An empty ChannelID
// for the bot destroys the player; another user's only turns into a
// [PlayerVoiceJoinEvent] or [PlayerVoiceLeaveEvent].
func (c *Client) OnVoiceStateUpdate(ctx context.Context, update VoiceStateUpdate) error {
	player := c.Player(update.GuildID)
	if player == nil {
		return nil
	}
	if update.UserID != "" && update.UserID != c.cfg.UserID {
		player.otherVoiceState(update)
		return nil
	}
	if update.ChannelID == "" {
		// [Player.Disconnect] already cleared the channel, so this leave is ours.
		if player.ChannelID() == "" {
			return nil
		}
		c.emit(&PlayerDisconnectEvent{Player: player, ChannelID: player.ChannelID()})
		return player.Destroy(ctx, DestroyDisconnected)
	}
	return player.onVoiceState(ctx, update)
}

// OnVoiceServerUpdate feeds Discord's VOICE_SERVER_UPDATE in; this is what hands
// the voice connection to the node.
func (c *Client) OnVoiceServerUpdate(ctx context.Context, guildID, token, endpoint string) error {
	player := c.Player(guildID)
	if player == nil {
		return nil
	}
	return player.onVoiceServer(ctx, token, endpoint)
}

// Close closes every node and stops reconnecting. With [Config.Resuming] the
// nodes keep playing, so [NodeConfig.SessionID] can pick them back up; destroy
// the players first for a clean stop. ctx bounds the whole shutdown.
func (c *Client) Close(ctx context.Context) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	nodes := c.nodes
	close(c.done)
	c.mu.Unlock()

	for _, node := range nodes {
		node.Close(ctx)
	}
}

// stopped reports whether [Client.Close] was called.
func (c *Client) stopped() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
