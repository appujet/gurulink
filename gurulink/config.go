package gurulink

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/appujet/gurulink/lavalink"
	"github.com/appujet/gurulink/queue"
)

// Config configures a [Client]. Only UserID and SendVoiceUpdate are required.
type Config struct {
	// UserID is the bot's own user id, which nodes want on every connection.
	UserID string
	// ClientName goes into the Client-Name header. Defaults to "gurulink".
	ClientName string
	// SendVoiceUpdate sends a voice state update through your Discord library, the
	// one thing gurulink needs from it. A nil channelID leaves the channel.
	SendVoiceUpdate func(ctx context.Context, guildID string, channelID *string, selfMute, selfDeaf bool) error
	// Logger defaults to [slog.Default].
	Logger *slog.Logger
	// HTTPClient talks to node REST APIs. Defaults to a 10s timeout.
	HTTPClient *http.Client
	// Listeners receive every event, in order, on the node's read loop.
	Listeners []Listener

	// DefaultSource is the search prefix, or an alias, for queries naming none.
	// Defaults to [DefaultSource]; an unknown one fails [New].
	DefaultSource string

	// QueueStore persists queues; nil keeps them in memory only.
	QueueStore queue.Store
	// OnQueueChange is told about every queue change, for a now-playing message.
	OnQueueChange func(ctx context.Context, guildID string, change queue.Change, tracks []lavalink.Track)

	// Autoplay is asked for more tracks when a queue runs dry, before
	// [QueueEndEvent]. Add them to the player's queue; adding none ends it.
	Autoplay func(ctx context.Context, player *Player) error
	// EmptyQueueTimeout destroys an idle player after this long. Zero never does.
	EmptyQueueTimeout time.Duration
	// MaxTrackErrors stops a player after this many failures in a row. Defaults
	// to 3; negative never stops.
	MaxTrackErrors int

	// Resuming keeps players alive this long after a websocket drop. Zero
	// disables it, so a drop stops the music.
	Resuming time.Duration
	// Heartbeat pings this often and drops the socket after two silent
	// intervals. Zero trusts the socket.
	Heartbeat time.Duration
	// MaxReconnects redials this many times. Defaults to 10; negative is forever.
	MaxReconnects int
	// ReconnectDelay grows with the attempt number, capped at a minute. Defaults
	// to 5s.
	ReconnectDelay time.Duration

	// Crossfade overlaps tracks by pre-buffering the next. Needs a Kairo node;
	// [Player.SetCrossfade] overrides it per player.
	Crossfade *lavalink.Crossfade
	// Tape ramps pitch around a pause. Needs a Kairo node; [Player.SetTape]
	// overrides it per player.
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
