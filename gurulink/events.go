package gurulink

import (
	"encoding/json"

	"github.com/appujet/gurulink/lavalink"
)

// Event is any of the *Event types in this package. Handlers get it through a
// [Listener]; use [On] to receive just one type.
type Event any

// Listener receives every event a [Client] dispatches, in the order the node
// sent them. Listeners run on the node's read loop, so keep them quick or hand
// the work to a goroutine.
type Listener func(e Event)

// On adapts a handler for one event type into a [Listener] that drops the rest:
//
//	gurulink.On(func(e *gurulink.TrackStartEvent) { ... })
func On[E Event](f func(e E)) Listener {
	return func(e Event) {
		if typed, ok := e.(E); ok {
			f(typed)
		}
	}
}

// ReadyEvent is a node's handshake. From here on its session id works, so this
// is where resumed players are reclaimed.
type ReadyEvent struct {
	Node      *Node  `json:"-"`
	SessionID string `json:"sessionId"`
	Resumed   bool   `json:"resumed"`
}

// StatsEvent is a node's load report, sent every minute.
type StatsEvent struct {
	Node *Node `json:"-"`
	lavalink.Stats
}

// DisconnectEvent is a node's websocket going away. The node reconnects on its
// own unless [Config.Reconnect] says otherwise.
type DisconnectEvent struct {
	Node   *Node
	Code   int
	Reason string
}

// ErrorEvent is a failure the client had nowhere else to report: a broken
// frame, a failed reconnect, a rejected player update.
type ErrorEvent struct {
	Node *Node
	Err  error
}

// PlayerUpdateEvent carries the position report a node sends every ~100ms.
type PlayerUpdateEvent struct {
	Player *Player              `json:"-"`
	State  lavalink.PlayerState `json:"state"`
}

// TrackStartEvent fires once the node started decoding a track.
type TrackStartEvent struct {
	Player *Player        `json:"-"`
	Track  lavalink.Track `json:"track"`
}

// TrackPromotedEvent fires when a pre-buffered successor became the active
// track through a crossfade or gapless transition. The node is already playing
// it; the client only advances its own queue. Needs a Kairo node.
type TrackPromotedEvent struct {
	Player *Player        `json:"-"`
	Track  lavalink.Track `json:"track"`
}

// TrackEndEvent fires once a track stopped, for whatever reason.
type TrackEndEvent struct {
	Player *Player                 `json:"-"`
	Track  lavalink.Track          `json:"track"`
	Reason lavalink.TrackEndReason `json:"reason"`
}

// TrackExceptionEvent is a track that failed to play.
type TrackExceptionEvent struct {
	Player    *Player            `json:"-"`
	Track     lavalink.Track     `json:"track"`
	Exception lavalink.Exception `json:"exception"`
}

// TrackStuckEvent means the node got no audio frame for ThresholdMs.
type TrackStuckEvent struct {
	Player      *Player           `json:"-"`
	Track       lavalink.Track    `json:"track"`
	ThresholdMs lavalink.Duration `json:"thresholdMs"`
}

// WebSocketClosedEvent is Discord hanging up on the node's voice connection.
// Codes 4006, 4009 and 4014 mean the voice session is gone for good, so the
// player resends its voice state instead of waiting.
type WebSocketClosedEvent struct {
	Player   *Player `json:"-"`
	Code     int     `json:"code"`
	Reason   string  `json:"reason"`
	ByRemote bool    `json:"byRemote"`
}

// Voice close codes worth acting on.
const (
	CloseCodeSessionInvalid = 4006
	CloseCodeSessionExpired = 4009
	CloseCodeDisconnected   = 4014
)

// SegmentsLoadedEvent lists the skippable regions SponsorBlock found.
type SegmentsLoadedEvent struct {
	Player   *Player            `json:"-"`
	Segments []lavalink.Segment `json:"segments"`
}

// SegmentSkippedEvent fires each time SponsorBlock jumped a segment.
type SegmentSkippedEvent struct {
	Player  *Player          `json:"-"`
	Segment lavalink.Segment `json:"segment"`
}

// ChaptersLoadedEvent lists the chapters SponsorBlock found.
type ChaptersLoadedEvent struct {
	Player   *Player            `json:"-"`
	Chapters []lavalink.Chapter `json:"chapters"`
}

// ChapterStartedEvent fires when playback entered a chapter.
type ChapterStartedEvent struct {
	Player  *Player          `json:"-"`
	Chapter lavalink.Chapter `json:"chapter"`
}

// LyricsFoundEvent is LavaLyrics answering a subscription.
type LyricsFoundEvent struct {
	Player *Player         `json:"-"`
	Lyrics lavalink.Lyrics `json:"lyrics"`
}

// LyricsNotFoundEvent means LavaLyrics had nothing for the current track.
type LyricsNotFoundEvent struct {
	Player *Player `json:"-"`
}

// LyricsLineEvent fires as each synced line comes up. Skipped is set when the
// line was passed over, e.g. after a seek.
type LyricsLineEvent struct {
	Player    *Player             `json:"-"`
	LineIndex int                 `json:"lineIndex"`
	Line      lavalink.LyricsLine `json:"line"`
	Skipped   bool                `json:"skipped"`
}

// UnknownEvent is an event type this client does not know, usually from a
// plugin. Data is the whole frame.
type UnknownEvent struct {
	Node    *Node
	Player  *Player
	GuildID string
	Type    lavalink.EventType
	Data    json.RawMessage
}

// PlayerCreateEvent fires the first time a guild gets a player.
type PlayerCreateEvent struct {
	Player *Player
}

// PlayerDestroyEvent fires after a player was torn down; the player is unusable
// from here on.
type PlayerDestroyEvent struct {
	Player *Player
	Reason DestroyReason
}

// PlayerMoveEvent fires after a player was rebuilt on another node.
type PlayerMoveEvent struct {
	Player *Player
	From   *Node
	To     *Node
}

// QueueEndEvent fires when a track finished and the queue had nothing to follow
// it with. [Config.Autoplay] runs first, so this only fires when that also came
// up empty.
type QueueEndEvent struct {
	Player *Player
	// Track is the one that just ended.
	Track  lavalink.Track
	Reason lavalink.TrackEndReason
}
