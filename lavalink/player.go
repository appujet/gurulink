package lavalink

import "encoding/json"

// PlayerState is the playback state a node reports every ~100ms.
type PlayerState struct {
	Time      Timestamp `json:"time"`
	Position  Duration  `json:"position"`
	Connected bool      `json:"connected"`
	Ping      int       `json:"ping"`
}

// VoiceState is the Discord voice connection a node should use.
type VoiceState struct {
	Token     string `json:"token"`
	Endpoint  string `json:"endpoint"`
	SessionID string `json:"sessionId"`
	ChannelID string `json:"channelId"`
}

// Complete reports whether the state can be handed to a node. A node rejects the whole update when
// any of the four is missing, channel id included.
func (v VoiceState) Complete() bool {
	return v.Token != "" && v.Endpoint != "" && v.SessionID != "" && v.ChannelID != ""
}

// PlayerInfo is a player as the node sees it.
type PlayerInfo struct {
	GuildID string      `json:"guildId"`
	Track   *Track      `json:"track"`
	Volume  int         `json:"volume"`
	Paused  bool        `json:"paused"`
	State   PlayerState `json:"state"`
	Voice   VoiceState  `json:"voice"`
	Filters Filters     `json:"filters"`
}

// UpdateTrack selects the track to play. Encoded may be set to null (see
// [Null]) to stop playback, or left out to only touch the other fields.
type UpdateTrack struct {
	Encoded    Nullable[string] `json:"encoded,omitzero"`
	Identifier string           `json:"identifier,omitzero"`
	UserData   json.RawMessage  `json:"userData,omitzero"`
}

// PlayerUpdate is a PATCH body for a player: every zero field is left untouched
// by the node.
type PlayerUpdate struct {
	Track    *UpdateTrack `json:"track,omitzero"`
	Position *Duration    `json:"position,omitzero"`
	EndTime  *Duration    `json:"endTime,omitzero"`
	Volume   *int         `json:"volume,omitzero"`
	Paused   *bool        `json:"paused,omitzero"`
	Voice    *VoiceState  `json:"voice,omitzero"`
	Filters  *Filters     `json:"filters,omitzero"`

	// NextTrack, Crossfade, Tape and Transition need a Kairo node; stock
	// Lavalink ignores unknown fields, so sending them is harmless.
	NextTrack  Nullable[UpdateTrack] `json:"nextTrack,omitzero"`
	Crossfade  Nullable[Crossfade]   `json:"crossfade,omitzero"`
	Tape       Nullable[Tape]        `json:"tape,omitzero"`
	Transition bool                  `json:"transition,omitzero"`

	// NoReplace becomes a query parameter, not part of the body.
	NoReplace bool `json:"-"`
}
