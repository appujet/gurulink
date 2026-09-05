package lavalink

import (
	"encoding/json"
	"fmt"
)

// Track is a playable track. Encoded is the only part the server needs back.
type Track struct {
	Encoded    string          `json:"encoded"`
	Info       TrackInfo       `json:"info"`
	PluginInfo json.RawMessage `json:"pluginInfo,omitempty"`
	UserData   json.RawMessage `json:"userData,omitempty"`
}

// TrackInfo describes a track. Nullable wire fields decode to their zero value.
type TrackInfo struct {
	Identifier string   `json:"identifier"`
	Title      string   `json:"title"`
	Author     string   `json:"author"`
	Length     Duration `json:"length"`
	Position   Duration `json:"position"`
	IsStream   bool     `json:"isStream"`
	IsSeekable bool     `json:"isSeekable"`
	URI        string   `json:"uri,omitempty"`
	ArtworkURL string   `json:"artworkUrl,omitempty"`
	ISRC       string   `json:"isrc,omitempty"`
	SourceName string   `json:"sourceName"`
}

// WithUserData returns a copy of t carrying data. Lavalink echoes user data back
// on every event, so it is the place for a requester.
func (t Track) WithUserData(data any) (Track, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return t, fmt.Errorf("marshal user data: %w", err)
	}
	t.UserData = raw
	return t, nil
}

// Playlist is a loaded playlist.
type Playlist struct {
	Info       PlaylistInfo    `json:"info"`
	PluginInfo json.RawMessage `json:"pluginInfo,omitempty"`
	Tracks     []Track         `json:"tracks"`
}

type PlaylistInfo struct {
	Name          string `json:"name"`
	SelectedTrack int    `json:"selectedTrack"`
}

// Severity tells how bad a Lavalink exception is.
type Severity string

const (
	SeverityCommon     Severity = "common"
	SeveritySuspicious Severity = "suspicious"
	SeverityFault      Severity = "fault"
)

// Exception is a track loading or playback failure.
type Exception struct {
	Message         string   `json:"message"`
	Severity        Severity `json:"severity"`
	Cause           string   `json:"cause"`
	CauseStackTrace string   `json:"causeStackTrace,omitempty"`
}

func (e Exception) Error() string { return string(e.Severity) + ": " + e.Message }

type LoadType string

const (
	LoadTypeTrack    LoadType = "track"
	LoadTypePlaylist LoadType = "playlist"
	LoadTypeSearch   LoadType = "search"
	LoadTypeEmpty    LoadType = "empty"
	LoadTypeError    LoadType = "error"
)

// LoadResult is what /loadtracks returned: LoadType picks the one payload field
// that is set. [LoadResult.AllTracks] skips the switch.
type LoadResult struct {
	LoadType  LoadType
	Track     *Track
	Playlist  *Playlist
	Tracks    []Track
	Exception *Exception
}

// AllTracks returns every track in the result, whatever shape it arrived in.
func (r LoadResult) AllTracks() []Track {
	switch {
	case r.Track != nil:
		return []Track{*r.Track}
	case r.Playlist != nil:
		return r.Playlist.Tracks
	default:
		return r.Tracks
	}
}

func (r *LoadResult) UnmarshalJSON(data []byte) error {
	var raw struct {
		LoadType LoadType        `json:"loadType"`
		Data     json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	r.LoadType = raw.LoadType

	var target any
	switch raw.LoadType {
	case LoadTypeTrack:
		r.Track = &Track{}
		target = r.Track
	case LoadTypePlaylist:
		r.Playlist = &Playlist{}
		target = r.Playlist
	case LoadTypeSearch:
		target = &r.Tracks
	case LoadTypeError:
		r.Exception = &Exception{}
		target = r.Exception
	case LoadTypeEmpty:
		return nil
	default:
		return fmt.Errorf("unknown load type %q", raw.LoadType)
	}
	if err := json.Unmarshal(raw.Data, target); err != nil {
		return fmt.Errorf("unmarshal %s load result: %w", raw.LoadType, err)
	}
	return nil
}

// LavaSearchType is a result kind the LavaSearch plugin can return.
type LavaSearchType string

const (
	LavaSearchTypeTrack    LavaSearchType = "track"
	LavaSearchTypeAlbum    LavaSearchType = "album"
	LavaSearchTypeArtist   LavaSearchType = "artist"
	LavaSearchTypePlaylist LavaSearchType = "playlist"
	LavaSearchTypeText     LavaSearchType = "text"
)

// LavaSearchResult is a filtered search result from the LavaSearch plugin.
type LavaSearchResult struct {
	Tracks     []Track          `json:"tracks"`
	Albums     []LavaSearchList `json:"albums"`
	Artists    []LavaSearchList `json:"artists"`
	Playlists  []LavaSearchList `json:"playlists"`
	Texts      []LavaSearchText `json:"texts"`
	PluginInfo json.RawMessage  `json:"pluginInfo,omitempty"`
}

// Empty reports whether the node found nothing at all.
func (r LavaSearchResult) Empty() bool {
	return len(r.Tracks) == 0 && len(r.Albums) == 0 && len(r.Artists) == 0 &&
		len(r.Playlists) == 0 && len(r.Texts) == 0
}

type LavaSearchList struct {
	Info       PlaylistInfo    `json:"info"`
	PluginInfo json.RawMessage `json:"pluginInfo,omitempty"`
	Tracks     []Track         `json:"tracks"`
}

type LavaSearchText struct {
	Text       string          `json:"text"`
	PluginInfo json.RawMessage `json:"pluginInfo,omitempty"`
}
