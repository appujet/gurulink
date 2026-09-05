package gurulink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/appujet/gurulink/lavalink"
	"github.com/appujet/gurulink/queue"
)

// ErrPlayerDestroyed is returned by every command on a torn-down player.
var ErrPlayerDestroyed = errors.New("gurulink: player is destroyed")

// RepeatMode is what a player does when a track ends.
type RepeatMode int

const (
	// RepeatOff moves on to the next queued track.
	RepeatOff RepeatMode = iota
	// RepeatTrack plays the same track again.
	RepeatTrack
	// RepeatQueue puts the finished track at the back of the queue.
	RepeatQueue
)

func (m RepeatMode) String() string {
	switch m {
	case RepeatOff:
		return "off"
	case RepeatTrack:
		return "track"
	case RepeatQueue:
		return "queue"
	}
	return "unknown"
}

// DestroyReason says why a player was torn down.
type DestroyReason string

const (
	DestroyRequested    DestroyReason = "requested"
	DestroyDisconnected DestroyReason = "left the voice channel"
	DestroyQueueEmpty   DestroyReason = "queue stayed empty"
	DestroyTrackErrors  DestroyReason = "too many failed tracks"
	DestroyVoiceClosed  DestroyReason = "voice connection closed"
	DestroyNodeGone     DestroyReason = "node gone"
)

// Player is one guild's music player: a voice connection on a node plus a
// [queue.Queue]. Its methods are safe for concurrent use.
//
// Lock order: never take p.mu while holding the queue's lock. Node calls happen
// outside p.mu, so a slow node never blocks a reader.
type Player struct {
	client  *Client
	guildID string
	queue   *queue.Queue
	log     *slog.Logger

	mu         sync.RWMutex
	node       *Node
	channelID  string
	selfMute   bool
	selfDeaf   bool
	serverMute bool
	serverDeaf bool
	suppress   bool
	voice      lavalink.VoiceState
	state      lavalink.PlayerState
	stateAt    time.Time
	volume     int
	paused     bool
	filters    lavalink.Filters
	repeat     RepeatMode
	errors     int
	idleTimer  *time.Timer
	destroyed  bool
}

func newPlayer(client *Client, node *Node, guildID string) *Player {
	cfg := client.cfg
	return &Player{
		client:  client,
		guildID: guildID,
		queue: queue.New(guildID, queue.Config{
			Store:    cfg.QueueStore,
			Logger:   cfg.Logger,
			OnChange: cfg.OnQueueChange,
		}),
		log:    cfg.Logger.With(slog.String("guild_id", guildID)),
		node:   node,
		volume: 100,
	}
}

// GuildID is the guild this player belongs to.
func (p *Player) GuildID() string { return p.guildID }

// Client is the client owning this player.
func (p *Player) Client() *Client { return p.client }

// Queue is the player's track list.
func (p *Player) Queue() *queue.Queue { return p.queue }

// Node is the node currently holding this player.
func (p *Player) Node() *Node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.node
}

// ChannelID is the voice channel the player is in, or "" when it is in none.
func (p *Player) ChannelID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.channelID
}

// Voice is the Discord voice connection the node was given.
func (p *Player) Voice() lavalink.VoiceState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.voice
}

// State is the last state the node reported.
func (p *Player) State() lavalink.PlayerState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// Position estimates where the current track is, interpolating with the local
// clock between the updates a node sends every few seconds.
func (p *Player) Position() lavalink.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.paused || p.stateAt.IsZero() {
		return p.state.Position
	}
	return p.state.Position + lavalink.Duration(time.Since(p.stateAt).Milliseconds())
}

// Connected reports whether the node has a live voice connection.
func (p *Player) Connected() bool { return p.State().Connected }

// Ping is the node's round trip to Discord's voice server, or -1 when there is
// no connection.
func (p *Player) Ping() int { return p.State().Ping }

// Volume is the player's volume, 0 to 1000.
func (p *Player) Volume() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.volume
}

// Paused reports whether playback is paused.
func (p *Player) Paused() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.paused
}

// Playing reports whether a track is loaded and running.
func (p *Player) Playing() bool {
	return p.queue.Current() != nil && !p.Paused()
}

// Filters returns the filters the node has applied.
func (p *Player) Filters() lavalink.Filters {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.filters
}

// Repeat is the player's repeat mode.
func (p *Player) Repeat() RepeatMode {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.repeat
}

// SetRepeat changes what happens when a track ends. It takes effect on the next
// track end, so it needs no node call.
func (p *Player) SetRepeat(mode RepeatMode) {
	p.mu.Lock()
	p.repeat = mode
	p.mu.Unlock()
}

// Destroyed reports whether the player was torn down, which makes every command
// return [ErrPlayerDestroyed].
func (p *Player) Destroyed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.destroyed
}

// setState records a playerUpdate frame.
func (p *Player) setState(state lavalink.PlayerState) {
	p.mu.Lock()
	p.state, p.stateAt = state, time.Now()
	p.mu.Unlock()
}

// absorb takes the node's word for the player's state after a command.
func (p *Player) absorb(info lavalink.PlayerInfo) {
	p.mu.Lock()
	p.volume, p.paused, p.filters = info.Volume, info.Paused, info.Filters
	p.state, p.stateAt = info.State, time.Now()
	p.mu.Unlock()
}

// update is the one funnel for every command: it patches the player on its node
// and takes the reply as the new truth.
func (p *Player) update(ctx context.Context, update lavalink.PlayerUpdate) error {
	p.mu.RLock()
	node, destroyed := p.node, p.destroyed
	p.mu.RUnlock()
	if destroyed {
		return ErrPlayerDestroyed
	}
	info, err := node.UpdatePlayer(ctx, p.guildID, update)
	if err != nil {
		return err
	}
	p.absorb(info)
	return nil
}

// Update patches the player directly, for the odd field this package has no
// method for. Prefer the methods; they keep the queue in step.
func (p *Player) Update(ctx context.Context, update lavalink.PlayerUpdate) error {
	return p.update(ctx, update)
}

// Play starts a track now, making it the current one and unpausing. Queued
// tracks start on their own as the ones before them end.
func (p *Player) Play(ctx context.Context, track lavalink.Track) error {
	return p.play(ctx, track)
}

func (p *Player) play(ctx context.Context, track lavalink.Track) error {
	p.stopIdle()
	p.queue.SetCurrent(&track)
	resume := false
	return p.update(ctx, lavalink.PlayerUpdate{
		Track:  &lavalink.UpdateTrack{Encoded: lavalink.Value(track.Encoded), UserData: track.UserData},
		Paused: &resume,
	})
}

// PlayIdentifier plays whatever a search phrase or URL resolves to, so a node
// does the lookup. Use [Client.Search] when you want the tracks first.
func (p *Player) PlayIdentifier(ctx context.Context, identifier string) error {
	p.stopIdle()
	resume := false
	return p.update(ctx, lavalink.PlayerUpdate{
		Track:  &lavalink.UpdateTrack{Identifier: identifier},
		Paused: &resume,
	})
}

// Stop stops playback and clears the queue, leaving the player connected.
func (p *Player) Stop(ctx context.Context) error {
	p.queue.Clear()
	p.queue.SetCurrent(nil)
	return p.update(ctx, lavalink.PlayerUpdate{Track: &lavalink.UpdateTrack{Encoded: lavalink.Null[string]()}})
}

// Pause pauses or resumes playback. With [Config.Tape] set the node ramps the
// pitch down and up around it.
func (p *Player) Pause(ctx context.Context, pause bool) error {
	update := lavalink.PlayerUpdate{Paused: &pause}
	if tape := p.client.cfg.Tape; tape != nil {
		update.Tape = lavalink.Value(*tape)
	}
	was := p.Paused()
	if err := p.update(ctx, update); err != nil {
		return err
	}
	// The node's reply is the truth, so a pause that changed nothing stays quiet.
	if now := p.Paused(); now != was {
		p.client.emit(&PlayerPauseEvent{Player: p, Paused: now})
	}
	return nil
}

// Resume unpauses playback.
func (p *Player) Resume(ctx context.Context) error { return p.Pause(ctx, false) }

// Seek jumps to a position in the current track.
func (p *Player) Seek(ctx context.Context, position lavalink.Duration) error {
	if position < 0 {
		position = 0
	}
	if err := p.update(ctx, lavalink.PlayerUpdate{Position: &position}); err != nil {
		return err
	}
	// ponytail: a filtered stream swallows the first seek often enough that the
	// TS client nudges it twice; do the same, but only when filters are on.
	if p.Filters().Active() {
		return p.update(ctx, lavalink.PlayerUpdate{Position: &position})
	}
	return nil
}

// SetVolume sets the player volume, 0 to 1000. 100 is the node's untouched
// output; above it the node amplifies and may clip.
func (p *Player) SetVolume(ctx context.Context, volume int) error {
	volume = min(max(volume, 0), 1000)
	return p.update(ctx, lavalink.PlayerUpdate{Volume: &volume})
}

// SetEndTime stops the current track early, at a position in it.
func (p *Player) SetEndTime(ctx context.Context, end lavalink.Duration) error {
	return p.update(ctx, lavalink.PlayerUpdate{EndTime: &end})
}

// Skip drops the current track and starts the next one, ignoring
// [RepeatTrack] — a listener asking for the next track means it.
func (p *Player) Skip(ctx context.Context) error {
	return p.next(ctx, lavalink.ReasonStopped)
}

// SkipTo skips the queued tracks before index i and plays that one.
func (p *Player) SkipTo(ctx context.Context, i int) error {
	if i < 0 || i >= p.queue.Len() {
		return fmt.Errorf("gurulink: skip to %d out of range (%d tracks)", i, p.queue.Len())
	}
	p.queue.RemoveRange(0, i)
	return p.Skip(ctx)
}

// Back plays the previously played track again, pushing the current one back to
// the front of the queue.
func (p *Player) Back(ctx context.Context) error {
	track, ok := p.queue.Back()
	if !ok {
		return errors.New("gurulink: nothing played yet")
	}
	// Back already made it current; play it without touching the queue again.
	p.stopIdle()
	resume := false
	return p.update(ctx, lavalink.PlayerUpdate{
		Track:  &lavalink.UpdateTrack{Encoded: lavalink.Value(track.Encoded), UserData: track.UserData},
		Paused: &resume,
	})
}

// next advances to the following track. It asks [Config.Autoplay] for more when
// the queue is dry and emits [QueueEndEvent] when that comes up empty too.
func (p *Player) next(ctx context.Context, reason lavalink.TrackEndReason) error {
	ended := p.queue.Current()
	track, ok := p.queue.Advance()
	if !ok && p.client.cfg.Autoplay != nil {
		if err := p.client.cfg.Autoplay(ctx, p); err != nil {
			p.client.emit(&ErrorEvent{Node: p.Node(), Err: fmt.Errorf("gurulink: autoplay: %w", err)})
		}
		track, ok = p.queue.Advance()
	}
	if ok {
		return p.play(ctx, track)
	}

	p.startIdle()
	var last lavalink.Track
	if ended != nil {
		last = *ended
	}
	p.client.emit(&QueueEndEvent{Player: p, Track: last, Reason: reason})
	// Skipping the last track has to stop the audio; a track that ended already did.
	return p.update(ctx, lavalink.PlayerUpdate{Track: &lavalink.UpdateTrack{Encoded: lavalink.Null[string]()}})
}

// PreBuffer tells the node which track follows the current one so it can
// overlap them. The player does this on every track start; call it again after
// editing the queue to have the change affect the pending crossfade. Needs
// [Config.Crossfade] and a Kairo node.
func (p *Player) PreBuffer(ctx context.Context) error {
	crossfade := p.client.cfg.Crossfade
	if crossfade == nil || !crossfade.Enable {
		return nil
	}
	// A queue that ran dry clears the successor, so the node does not fade into
	// a track that is no longer next.
	update := lavalink.PlayerUpdate{Crossfade: lavalink.Value(*crossfade), NextTrack: lavalink.Null[lavalink.UpdateTrack]()}
	if next, ok := p.queue.Peek(); ok {
		update.NextTrack = lavalink.Value(lavalink.UpdateTrack{Encoded: lavalink.Value(next.Encoded), UserData: next.UserData})
	}
	return p.update(ctx, update)
}

// SetFilters replaces the player's filters.
func (p *Player) SetFilters(ctx context.Context, filters lavalink.Filters) error {
	return p.update(ctx, lavalink.PlayerUpdate{Filters: &filters})
}

// UpdateFilters edits the filters in place, so one can change without
// rebuilding the rest:
//
//	player.UpdateFilters(ctx, func(f *lavalink.Filters) { f.Timescale = &lavalink.Nightcore })
func (p *Player) UpdateFilters(ctx context.Context, edit func(*lavalink.Filters)) error {
	filters := p.Filters()
	edit(&filters)
	return p.SetFilters(ctx, filters)
}

// ClearFilters drops every filter.
func (p *Player) ClearFilters(ctx context.Context) error {
	return p.SetFilters(ctx, lavalink.Filters{})
}

// Search resolves a query on this player's node, so the tracks come from the
// node that will play them. See [Client.Search] for the query rules.
func (p *Player) Search(ctx context.Context, query, source string) (lavalink.LoadResult, error) {
	return p.client.searchOn(ctx, p.Node(), query, source)
}
