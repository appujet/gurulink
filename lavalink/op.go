package lavalink

// Op is the opcode of a gateway frame.
type Op string

const (
	OpReady        Op = "ready"
	OpStats        Op = "stats"
	OpPlayerUpdate Op = "playerUpdate"
	OpEvent        Op = "event"
)

// EventType is the "type" field of an op:event frame.
type EventType string

const (
	EventTypeTrackStart      EventType = "TrackStartEvent"
	EventTypeTrackEnd        EventType = "TrackEndEvent"
	EventTypeTrackException  EventType = "TrackExceptionEvent"
	EventTypeTrackStuck      EventType = "TrackStuckEvent"
	EventTypeWebSocketClosed EventType = "WebSocketClosedEvent"

	// EventTypeTrackPromoted needs a Kairo node.
	EventTypeTrackPromoted EventType = "TrackPromotedEvent"

	// SponsorBlock plugin.
	EventTypeSegmentsLoaded EventType = "SegmentsLoaded"
	EventTypeSegmentSkipped EventType = "SegmentSkipped"
	EventTypeChaptersLoaded EventType = "ChaptersLoaded"
	EventTypeChapterStarted EventType = "ChapterStarted"

	// LavaLyrics plugin.
	EventTypeLyricsFound    EventType = "LyricsFoundEvent"
	EventTypeLyricsNotFound EventType = "LyricsNotFoundEvent"
	EventTypeLyricsLine     EventType = "LyricsLineEvent"
)

// TrackEndReason says why a track stopped.
type TrackEndReason string

const (
	ReasonFinished   TrackEndReason = "finished"
	ReasonLoadFailed TrackEndReason = "loadFailed"
	ReasonStopped    TrackEndReason = "stopped"
	ReasonReplaced   TrackEndReason = "replaced"
	ReasonCleanup    TrackEndReason = "cleanup"

	// ReasonCrossfade and ReasonGapless mean a pre-buffered successor already
	// took over on the node, so no play request must follow.
	ReasonCrossfade TrackEndReason = "crossfade"
	ReasonGapless   TrackEndReason = "gapless"
)

// StartNext reports whether the client should now play the next queued track.
func (r TrackEndReason) StartNext() bool {
	return r == ReasonFinished || r == ReasonLoadFailed
}

// Promoted reports whether the node moved to the successor by itself.
func (r TrackEndReason) Promoted() bool {
	return r == ReasonCrossfade || r == ReasonGapless
}
