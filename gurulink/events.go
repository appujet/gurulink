package gurulink

import (
	"encoding/json"
	"time"

	"github.com/appujet/gurulink/lavalink"
)

// Event is any of the *Event types here. [On] picks out one.
type Event any

// Listener gets every event in order, on the node's read loop: keep it quick.
type Listener func(e Event)

// On makes a [Listener] that only handles one event type:
//
//	gurulink.On(func(e *gurulink.TrackStartEvent) { ... })
func On[E Event](f func(e E)) Listener {
	return func(e Event) {
		if typed, ok := e.(E); ok {
			f(typed)
		}
	}
}

// ReadyEvent is a node's handshake: from here on it takes requests.
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

// ConnectEvent is a node's websocket coming up; wait for [ReadyEvent] to use it.
type ConnectEvent struct {
	Node *Node
}

// DisconnectEvent is a node's websocket going away. It redials up to
// [Config.MaxReconnects] times on its own.
type DisconnectEvent struct {
	Node   *Node
	Code   int
	Reason string
}

// ReconnectEvent fires before each redial. Attempt counts from 1.
type ReconnectEvent struct {
	Node    *Node
	Attempt int
	Delay   time.Duration
}

// ResumedEvent is a node handing our old session back, with the players it kept.
// After a restart those are unknown here: rebuild or destroy them.
type ResumedEvent struct {
	Node    *Node
	Players []lavalink.PlayerInfo
}

// NodeRemovedEvent is a node dropped by [Client.RemoveNode], players moved first.
type NodeRemovedEvent struct {
	Node *Node
}

// ErrorEvent is a failure with nowhere else to go: a broken frame, a failed
// reconnect, a rejected player update.
type ErrorEvent struct {
	Node *Node
	Err  error
}

// PlayerUpdateEvent is a node's position report, every few seconds.
// [Player.Position] interpolates between them.
type PlayerUpdateEvent struct {
	Player *Player              `json:"-"`
	State  lavalink.PlayerState `json:"state"`
}

// TrackStartEvent fires once the node started decoding a track.
type TrackStartEvent struct {
	Player *Player        `json:"-"`
	Track  lavalink.Track `json:"track"`
}

// TrackPromotedEvent is a pre-buffered successor taking over on the node, so the
// client only catches its queue up. Needs a Kairo node.
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
// 4006, 4009 and 4014 make the player rebuild the session.
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

// LyricsLineEvent is a synced line coming up. Skipped means it was passed over.
type LyricsLineEvent struct {
	Player    *Player             `json:"-"`
	LineIndex int                 `json:"lineIndex"`
	Line      lavalink.LyricsLine `json:"line"`
	Skipped   bool                `json:"skipped"`
}

// UnknownEvent is a frame this client cannot name, usually a plugin's.
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

// PlayerDestroyEvent fires after a teardown; the player is unusable.
type PlayerDestroyEvent struct {
	Player *Player
	Reason DestroyReason
}

// PlayerNodeMoveEvent fires after a player was rebuilt on another node.
type PlayerNodeMoveEvent struct {
	Player *Player
	From   *Node
	To     *Node
}

// PlayerChannelMoveEvent is a channel change, ours or a moderator's. From is
// empty on the first join.
type PlayerChannelMoveEvent struct {
	Player *Player
	From   string
	To     string
}

// PlayerPauseEvent is a pause the node accepted, so a no-op does not fire.
type PlayerPauseEvent struct {
	Player *Player
	Paused bool
}

// PlayerDisconnectEvent is the bot leaving voice, by [Player.Disconnect] or by a
// kick, with the player intact.
type PlayerDisconnectEvent struct {
	Player    *Player
	ChannelID string
}

// PlayerReconnectEvent is a player rejoining to rebuild a voice session Discord
// threw away. See [WebSocketClosedEvent].
type PlayerReconnectEvent struct {
	Player    *Player
	ChannelID string
}

// QueueEndEvent is a finished track with nothing to follow it, [Config.Autoplay]
// included.
type QueueEndEvent struct {
	Player *Player
	// Track is the one that just ended.
	Track  lavalink.Track
	Reason lavalink.TrackEndReason
}

// IdleStartEvent starts the [Config.EmptyQueueTimeout] countdown; letting it run
// out destroys the player with [DestroyQueueEmpty].
type IdleStartEvent struct {
	Player  *Player
	Timeout time.Duration
}

// IdleCancelEvent is a track arriving before the countdown ran out.
type IdleCancelEvent struct {
	Player *Player
}

// PlayerMuteChangeEvent is a mute change: SelfMute ours, ServerMute a
// moderator's.
type PlayerMuteChangeEvent struct {
	Player     *Player
	SelfMute   bool
	ServerMute bool
}

// PlayerDeafChangeEvent is a deaf change: SelfDeaf ours, ServerDeaf a
// moderator's.
type PlayerDeafChangeEvent struct {
	Player     *Player
	SelfDeaf   bool
	ServerDeaf bool
}

// PlayerSuppressChangeEvent is a stage channel making the bot a listener, or not.
type PlayerSuppressChangeEvent struct {
	Player   *Player
	Suppress bool
}

// PlayerVoiceJoinEvent is another user joining. Needs the GuildVoiceStates
// intent.
type PlayerVoiceJoinEvent struct {
	Player *Player
	UserID string
}

// PlayerVoiceLeaveEvent is another user leaving the player's channel.
//
// ponytail: any update naming another channel counts as a leave; an exact answer
// needs a member list, so check your Discord library's cache.
type PlayerVoiceLeaveEvent struct {
	Player *Player
	UserID string
}
