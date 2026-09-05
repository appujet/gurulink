package gurulink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/appujet/gurulink/lavalink"
)

// ErrNoSession means the node has no session id yet, so no player request works.
var ErrNoSession = errors.New("gurulink: node has no session yet")

// do sends one request and decodes the reply into out, which may be nil. A
// non-2xx reply becomes a [lavalink.RESTError].
func (n *Node) do(ctx context.Context, method, path string, body, out any) error {
	var payload io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal %s %s: %w", method, path, err)
		}
		payload = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, n.rest+path, payload)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", n.cfg.Password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := n.client.cfg.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("gurulink: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusMultipleChoices {
		var restErr lavalink.RESTError
		if json.NewDecoder(resp.Body).Decode(&restErr) == nil && restErr.Status != 0 {
			return restErr
		}
		return fmt.Errorf("gurulink: %s %s: %s", method, path, resp.Status)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, path, err)
	}
	return nil
}

// session returns the path prefix for session-scoped endpoints.
func (n *Node) session() (string, error) {
	id := n.SessionID()
	if id == "" {
		return "", ErrNoSession
	}
	return "/v4/sessions/" + id, nil
}

// Version returns the node's version string, the one unversioned endpoint.
func (n *Node) Version(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.rest+"/version", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", n.cfg.Password)
	resp, err := n.client.cfg.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("gurulink: get version: %w", err)
	}
	defer resp.Body.Close()
	version, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("gurulink: get version: %s", resp.Status)
	}
	return strings.TrimSpace(string(version)), nil
}

// Info describes the node: its version, plugins and available sources.
func (n *Node) Info(ctx context.Context) (lavalink.Info, error) {
	var info lavalink.Info
	err := n.do(ctx, http.MethodGet, "/v4/info", nil, &info)
	return info, err
}

// FetchStats asks for a load report; [Node.Stats] has the last unprompted one.
func (n *Node) FetchStats(ctx context.Context) (lavalink.Stats, error) {
	var stats lavalink.Stats
	err := n.do(ctx, http.MethodGet, "/v4/stats", nil, &stats)
	return stats, err
}

// LoadTracks resolves a URL or "source:terms". [Client.Search] builds one from a
// user query.
func (n *Node) LoadTracks(ctx context.Context, identifier string) (lavalink.LoadResult, error) {
	var result lavalink.LoadResult
	err := n.do(ctx, http.MethodGet, "/v4/loadtracks?identifier="+url.QueryEscape(identifier), nil, &result)
	return result, err
}

// LoadSearch runs a LavaSearch query, optionally limited to some result kinds.
// Nothing found is a zero result, not an error.
func (n *Node) LoadSearch(ctx context.Context, query string, types ...lavalink.LavaSearchType) (lavalink.LavaSearchResult, error) {
	path := "/v4/loadsearch?query=" + url.QueryEscape(query)
	if len(types) > 0 {
		names := make([]string, len(types))
		for i, t := range types {
			names[i] = string(t)
		}
		path += "&types=" + url.QueryEscape(strings.Join(names, ","))
	}
	var result lavalink.LavaSearchResult
	err := n.do(ctx, http.MethodGet, path, nil, &result)
	return result, err
}

// DecodeTrack turns an encoded track back into its metadata.
func (n *Node) DecodeTrack(ctx context.Context, encoded string) (lavalink.Track, error) {
	var track lavalink.Track
	err := n.do(ctx, http.MethodGet, "/v4/decodetrack?encodedTrack="+url.QueryEscape(encoded), nil, &track)
	return track, err
}

// DecodeTracks decodes a batch in one request.
func (n *Node) DecodeTracks(ctx context.Context, encoded []string) ([]lavalink.Track, error) {
	var tracks []lavalink.Track
	err := n.do(ctx, http.MethodPost, "/v4/decodetracks", encoded, &tracks)
	return tracks, err
}

// UpdateSession turns resuming on or off: a node keeps its players alive for the
// timeout after a drop, so a reconnect picks them back up.
func (n *Node) UpdateSession(ctx context.Context, update lavalink.SessionUpdate) (lavalink.Session, error) {
	prefix, err := n.session()
	if err != nil {
		return lavalink.Session{}, err
	}
	var session lavalink.Session
	err = n.do(ctx, http.MethodPatch, prefix, update, &session)
	return session, err
}

// PlayerInfos lists every player the node holds for this session.
func (n *Node) PlayerInfos(ctx context.Context) ([]lavalink.PlayerInfo, error) {
	prefix, err := n.session()
	if err != nil {
		return nil, err
	}
	var players []lavalink.PlayerInfo
	err = n.do(ctx, http.MethodGet, prefix+"/players", nil, &players)
	return players, err
}

// FetchPlayer asks the node about one guild's player.
func (n *Node) FetchPlayer(ctx context.Context, guildID string) (lavalink.PlayerInfo, error) {
	path, err := n.playerPath(guildID)
	if err != nil {
		return lavalink.PlayerInfo{}, err
	}
	var info lavalink.PlayerInfo
	err = n.do(ctx, http.MethodGet, path, nil, &info)
	return info, err
}

// UpdatePlayer patches a player, creating it if the guild has none. Only the
// fields set in update change.
func (n *Node) UpdatePlayer(ctx context.Context, guildID string, update lavalink.PlayerUpdate) (lavalink.PlayerInfo, error) {
	path, err := n.playerPath(guildID)
	if err != nil {
		return lavalink.PlayerInfo{}, err
	}
	if update.NoReplace {
		path += "?noReplace=true"
	}
	var info lavalink.PlayerInfo
	err = n.do(ctx, http.MethodPatch, path, update, &info)
	return info, err
}

// DestroyPlayer drops the node's player for a guild.
func (n *Node) DestroyPlayer(ctx context.Context, guildID string) error {
	path, err := n.playerPath(guildID)
	if err != nil {
		return err
	}
	return n.do(ctx, http.MethodDelete, path, nil, nil)
}

// SponsorBlockCategories lists the categories the node skips for this player.
func (n *Node) SponsorBlockCategories(ctx context.Context, guildID string) ([]lavalink.SponsorBlockCategory, error) {
	path, err := n.playerPath(guildID)
	if err != nil {
		return nil, err
	}
	var categories []lavalink.SponsorBlockCategory
	err = n.do(ctx, http.MethodGet, path+"/sponsorblock/categories", nil, &categories)
	return categories, err
}

// SetSponsorBlockCategories replaces the categories to skip, rejecting unknown
// ones before the node does.
func (n *Node) SetSponsorBlockCategories(ctx context.Context, guildID string, categories []lavalink.SponsorBlockCategory) error {
	for _, c := range categories {
		if !c.Valid() {
			return fmt.Errorf("gurulink: unknown sponsorblock category %q", c)
		}
	}
	path, err := n.playerPath(guildID)
	if err != nil {
		return err
	}
	return n.do(ctx, http.MethodPut, path+"/sponsorblock/categories", categories, nil)
}

// ClearSponsorBlockCategories stops skipping anything.
func (n *Node) ClearSponsorBlockCategories(ctx context.Context, guildID string) error {
	path, err := n.playerPath(guildID)
	if err != nil {
		return err
	}
	return n.do(ctx, http.MethodDelete, path+"/sponsorblock/categories", nil, nil)
}

// TrackLyrics fetches the playing track's lyrics. skipTrackSource looks past the
// source's own provider.
func (n *Node) TrackLyrics(ctx context.Context, guildID string, skipTrackSource bool) (lavalink.Lyrics, error) {
	path, err := n.playerPath(guildID)
	if err != nil {
		return lavalink.Lyrics{}, err
	}
	var lyrics lavalink.Lyrics
	err = n.do(ctx, http.MethodGet, path+"/track/lyrics?skipTrackSource="+strconv.FormatBool(skipTrackSource), nil, &lyrics)
	return lyrics, err
}

// Lyrics fetches the lyrics of an encoded track, no player needed.
func (n *Node) Lyrics(ctx context.Context, encoded string, skipTrackSource bool) (lavalink.Lyrics, error) {
	path := "/v4/lyrics?track=" + url.QueryEscape(encoded) +
		"&skipTrackSource=" + strconv.FormatBool(skipTrackSource)
	var lyrics lavalink.Lyrics
	err := n.do(ctx, http.MethodGet, path, nil, &lyrics)
	return lyrics, err
}

// SubscribeLyrics asks the node to push [LyricsLineEvent]s as the track plays.
func (n *Node) SubscribeLyrics(ctx context.Context, guildID string, skipTrackSource bool) error {
	path, err := n.playerPath(guildID)
	if err != nil {
		return err
	}
	return n.do(ctx, http.MethodPost, path+"/lyrics/subscribe?skipTrackSource="+strconv.FormatBool(skipTrackSource), nil, nil)
}

// UnsubscribeLyrics stops the line events.
func (n *Node) UnsubscribeLyrics(ctx context.Context, guildID string) error {
	path, err := n.playerPath(guildID)
	if err != nil {
		return err
	}
	return n.do(ctx, http.MethodDelete, path+"/lyrics/subscribe", nil, nil)
}

// RoutePlannerStatus reports IP rotation state; Details is nil without a planner.
func (n *Node) RoutePlannerStatus(ctx context.Context) (lavalink.RoutePlanner, error) {
	var planner lavalink.RoutePlanner
	err := n.do(ctx, http.MethodGet, "/v4/routeplanner/status", nil, &planner)
	return planner, err
}

// UnmarkFailedAddress puts one address back into rotation.
func (n *Node) UnmarkFailedAddress(ctx context.Context, address string) error {
	body := struct {
		Address string `json:"address"`
	}{address}
	return n.do(ctx, http.MethodPost, "/v4/routeplanner/free/address", body, nil)
}

// UnmarkAllFailedAddresses puts every address back into rotation.
func (n *Node) UnmarkAllFailedAddresses(ctx context.Context) error {
	return n.do(ctx, http.MethodPost, "/v4/routeplanner/free/all", nil, nil)
}

// playerPath is the session-scoped path of one guild's player.
func (n *Node) playerPath(guildID string) (string, error) {
	prefix, err := n.session()
	if err != nil {
		return "", err
	}
	// Guild ids come from Discord payloads, so escape rather than trust them.
	return prefix + "/players/" + url.PathEscape(guildID), nil
}
