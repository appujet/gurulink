package lavalink

import (
	"encoding/json"
)

// CrossfadeCurve shapes the volume ramp of a crossfade.
type CrossfadeCurve string

const (
	CrossfadeLinear CrossfadeCurve = "linear"
	CrossfadeExp    CrossfadeCurve = "exp"
	CrossfadeLog    CrossfadeCurve = "log"
	CrossfadeSCurve CrossfadeCurve = "sCurve"
)

// Crossfade overlaps the end of a track with the pre-buffered successor. A zero
// field falls back to the node's default.
type Crossfade struct {
	Enable bool `json:"enable"`
	// ManualDurationMs is the overlap of an explicit skip.
	DurationMs       int            `json:"durationMs,omitzero"`
	ManualDurationMs int            `json:"manualDurationMs,omitzero"`
	Curve            CrossfadeCurve `json:"curve,omitzero"`
	Gapless          *bool          `json:"gapless,omitzero"`
}

// TapeCurve shapes the pitch ramp of a tape stop.
type TapeCurve string

const (
	TapeLinear TapeCurve = "linear"
	TapeExp    TapeCurve = "exponential"
	TapeSCurve TapeCurve = "sCurve"
	TapeQuad   TapeCurve = "quad"
)

// Tape spins the audio down on pause and up on resume, like a tape deck.
type Tape struct {
	Enable     bool      `json:"enable"`
	DurationMs int       `json:"durationMs,omitzero"`
	Curve      TapeCurve `json:"curve,omitzero"`
}

// Lyrics is a LavaLyrics result.
type Lyrics struct {
	SourceName string          `json:"sourceName"`
	Provider   string          `json:"provider"`
	Text       string          `json:"text"`
	Lines      []LyricsLine    `json:"lines"`
	Plugin     json.RawMessage `json:"plugin,omitempty"`
}

type LyricsLine struct {
	Timestamp Duration        `json:"timestamp"`
	Duration  Duration        `json:"duration"`
	Line      string          `json:"line"`
	Plugin    json.RawMessage `json:"plugin,omitempty"`
}

// SponsorBlockCategory is a segment category the SponsorBlock plugin can skip.
type SponsorBlockCategory string

const (
	CategorySponsor       SponsorBlockCategory = "sponsor"
	CategorySelfPromo     SponsorBlockCategory = "selfpromo"
	CategoryInteraction   SponsorBlockCategory = "interaction"
	CategoryIntro         SponsorBlockCategory = "intro"
	CategoryOutro         SponsorBlockCategory = "outro"
	CategoryPreview       SponsorBlockCategory = "preview"
	CategoryMusicOffTopic SponsorBlockCategory = "music_offtopic"
	CategoryFiller        SponsorBlockCategory = "filler"
)

// DefaultSponsorBlockCategories is what a player asks for when you never say.
var DefaultSponsorBlockCategories = []SponsorBlockCategory{CategorySponsor, CategorySelfPromo}

// Valid reports whether c is a category the plugin knows.
func (c SponsorBlockCategory) Valid() bool {
	switch c {
	case CategorySponsor, CategorySelfPromo, CategoryInteraction, CategoryIntro,
		CategoryOutro, CategoryPreview, CategoryMusicOffTopic, CategoryFiller:
		return true
	}
	return false
}

// Segment is a skippable region of a track.
type Segment struct {
	Category string   `json:"category"`
	Start    Duration `json:"start"`
	End      Duration `json:"end"`
}

// Chapter is a named region of a track.
type Chapter struct {
	Name     string   `json:"name"`
	Start    Duration `json:"start"`
	End      Duration `json:"end"`
	Duration Duration `json:"duration"`
}

// RoutePlanner is the IP rotation state of a node.
type RoutePlanner struct {
	Class   string               `json:"class"`
	Details *RoutePlannerDetails `json:"details"`
}

type RoutePlannerDetails struct {
	IPBlock struct {
		Type string `json:"type"`
		Size string `json:"size"`
	} `json:"ipBlock"`
	FailingAddresses []struct {
		FailingAddress string    `json:"failingAddress"`
		FailingTime    string    `json:"failingTime"`
		Timestamp      Timestamp `json:"failingTimestamp"`
	} `json:"failingAddresses"`
	RotateIndex         string `json:"rotateIndex,omitempty"`
	IPIndex             string `json:"ipIndex,omitempty"`
	CurrentAddress      string `json:"currentAddress,omitempty"`
	CurrentAddressIndex string `json:"currentAddressIndex,omitempty"`
	BlockIndex          string `json:"blockIndex,omitempty"`
}
