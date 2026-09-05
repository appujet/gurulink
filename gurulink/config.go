package gurulink

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/appujet/gurulink/lavalink"
	"github.com/appujet/gurulink/queue"
)

// Config configures a [Client]. Only UserID and SendVoiceUpdate are required;
// everything else has a working default.
type Config struct {
	// UserID is the bot's own user id, which nodes want on every connection.
	UserID string
	// ClientName goes into the Client-Name header. Defaults to "gurulink".
	ClientName string
	// SendVoiceUpdate makes the Discord gateway shard holding guildID send a
	// voice state update; a nil channelID leaves the channel. This is the only
	// thing gurulink needs from a Discord library.
	SendVoiceUpdate func(ctx context.Context, guildID string, channelID *string, selfMute, selfDeaf bool) error
	// Logger defaults to [slog.Default].
	Logger *slog.Logger
	// HTTPClient talks to node REST APIs. Defaults to a 10s timeout; replace it
	// to get proxies, h2c or your own transport.
	HTTPClient *http.Client
	// Listeners receive every event, in order, on the node's read loop.
	Listeners []Listener

	// DefaultSource is the search prefix for queries that name none.
	DefaultSource string

	// QueueStore persists queues; nil keeps them in memory only.
	QueueStore queue.Store
	// OnQueueChange is told about every queue change, for a now-playing message
	// or a UI that has to keep up.
	OnQueueChange func(guildID string, change queue.Change, tracks []lavalink.Track)

	// Autoplay is asked for more tracks when a queue runs dry, before
	// [QueueEndEvent] fires. Add them to the player's queue; adding none ends
	// the queue.
	Autoplay func(ctx context.Context, player *Player) error
	// EmptyQueueTimeout destroys a player this long after its queue ran out.
	// Zero leaves the player connected.
	EmptyQueueTimeout time.Duration
	// MaxTrackErrors stops a player after this many tracks failed in a row.
	// Defaults to 3; negative never stops.
	MaxTrackErrors int

	// Resuming asks nodes to keep players alive for this long after the
	// websocket drops. Zero disables it, so a drop stops the music.
	Resuming time.Duration
	// Heartbeat pings a node this often and drops the connection when two
	// intervals pass without a pong. Zero trusts the socket.
	Heartbeat time.Duration
	// MaxReconnects is how many times a dropped node redials. Defaults to 10;
	// negative retries forever.
	MaxReconnects int
	// ReconnectDelay is the base delay between attempts, multiplied by the
	// attempt number and capped at a minute. Defaults to 5s.
	ReconnectDelay time.Duration

	// Crossfade overlaps tracks by pre-buffering the next one. Needs a Kairo
	// node; stock Lavalink ignores it. [Player.SetCrossfade] overrides it for
	// one player.
	Crossfade *lavalink.Crossfade
	// Tape ramps pitch down on pause and up on resume. Needs a Kairo node.
	// [Player.SetTape] overrides it for one player.
	Tape *lavalink.Tape
}

// withDefaults fills in everything the caller left blank.
func (c Config) withDefaults() Config {
	if c.ClientName == "" {
		c.ClientName = "Gurulink (https://github.com/appujet/gurulink)"
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if c.DefaultSource == "" {
		c.DefaultSource = DefaultSource
	}
	if c.MaxTrackErrors == 0 {
		c.MaxTrackErrors = 3
	}
	if c.MaxReconnects == 0 {
		c.MaxReconnects = 10
	}
	if c.ReconnectDelay <= 0 {
		c.ReconnectDelay = 5 * time.Second
	}
	return c
}
