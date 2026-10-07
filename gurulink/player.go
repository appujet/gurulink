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

// Player is one guild's voice connection plus a [queue.Queue]. Safe for
// concurrent use.
//
// Lock order: never take p.mu while holding the queue's lock; node calls happen
// outside p.mu.
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
	crossfade  *lavalink.Crossfade
	tape       *lavalink.Tape
	errors     int
	idleTimer  *time.Timer
	destroyed  bool
	// manualSkip marks an explicit skip via a crossfade transition, so the
	// following TrackEnd (crossfade/gapless) advances even with RepeatTrack,
	// like lavalink-client's internal_manualSkipPending.
	manualSkip bool
	// nextSync debounces NextTrack re-syncs after queue edits, like
	// lavalink-client's scheduleNextTrackSync (50ms): rapid edits coalesce
	// into one node call, and tests reading the synchronous Skip request
	// win the race against the delayed sync.
	nextSyncTimer *time.Timer
	nextSyncGen   int
}

func newPlayer(client *Client, node *Node, guildID string) *Player {
	cfg := client.cfg
	p := &Player{
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
	// Keep the node's pre-buffered successor in step with the queue head, like
	// lavalink-client's headWatcher: any edit that changes what plays next
	// re-syncs NextTrack, so a transition never fades into a stale track.
	// Current-only changes (a new track taking over) leave the head alone.
	outer := p.queue.OnChange()
	p.queue.SetOnChange(func(ctx context.Context, guildID string, change queue.Change, tracks []lavalink.Track) {
		if outer != nil {
			outer(ctx, guildID, change, tracks)
		}
		if change == queue.Current {
			return
		}
		p.schedulePreBuffer()
	})
	return p
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

// Position interpolates the last node update with the local clock.
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

// Ping is the node's round trip to Discord's voice, or -1 when disconnected.
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

// SetRepeat takes effect on the next track end, and re-syncs the pre-buffered
// successor right away: looping one track must fade into itself, not into the
// head it named before.
func (p *Player) SetRepeat(mode RepeatMode) {
	p.mu.Lock()
	p.repeat = mode
	p.mu.Unlock()
	p.schedulePreBuffer()
}

// schedulePreBuffer re-syncs NextTrack after a short debounce, coalescing
// rapid queue edits into one node call like lavalink-client's
// scheduleNextTrackSync.
func (p *Player) schedulePreBuffer() {
	if !p.crossfading() || p.Destroyed() {
		return
	}
	p.mu.Lock()
	p.nextSyncGen++
	gen := p.nextSyncGen
	if p.nextSyncTimer != nil {
		p.nextSyncTimer.Stop()
	}
	p.nextSyncTimer = time.AfterFunc(50*time.Millisecond, func() {
		p.mu.Lock()
		if gen != p.nextSyncGen || p.destroyed {
			p.mu.Unlock()
			return
		}
		p.nextSyncTimer = nil
		p.mu.Unlock()
		p.background(p.PreBuffer)
	})
	p.mu.Unlock()
}

// Destroyed reports whether every command now returns [ErrPlayerDestroyed].
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

// update patches the player on its node and takes the reply as the new truth.
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

// Update patches a field this package has no method for. The methods keep the
// queue in step; this does not.
func (p *Player) Update(ctx context.Context, update lavalink.PlayerUpdate) error {
	return p.update(ctx, update)
}

// Play starts a track now and unpauses. Queued tracks follow on their own.
func (p *Player) Play(ctx context.Context, track lavalink.Track) error {
	return p.play(ctx, track)
}

func (p *Player) play(ctx context.Context, track lavalink.Track) error {
	p.stopIdle()
	p.queue.SetCurrent(ctx, &track)
	resume := false
	if err := p.update(ctx, lavalink.PlayerUpdate{
		Track:  &lavalink.UpdateTrack{Encoded: lavalink.Value(track.Encoded), UserData: track.UserData},
		Paused: &resume,
	}); err != nil {
		return err
	}
	// Keep the pre-buffered successor in step: the head changed (or the track
	// was replaced), so the node must fade into the new head, not a stale one.
	p.schedulePreBuffer()
	return nil
}

// PlayIdentifier lets the node resolve a search phrase or URL. Use
// [Client.Search] to see the tracks first.
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
	p.queue.Clear(ctx)
	p.queue.SetCurrent(ctx, nil)
	return p.update(ctx, lavalink.PlayerUpdate{Track: &lavalink.UpdateTrack{Encoded: lavalink.Null[string]()}})
}

// Pause pauses or resumes. A tape ramps the pitch around it; see
// [Player.SetTape].
func (p *Player) Pause(ctx context.Context, pause bool) error {
	update := lavalink.PlayerUpdate{Paused: &pause}
	if tape := p.Tape(); tape != nil {
		update.Tape = lavalink.Value(*tape)
	}
	was := p.Paused()
	if err := p.update(ctx, update); err != nil {
		return err
	}
	// The node's reply is the truth; a no-op pause stays quiet.
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
	// ponytail: filters swallow the first seek, so nudge twice like the TS client.
	if p.Filters().Active() {
		return p.update(ctx, lavalink.PlayerUpdate{Position: &position})
	}
	return nil
}

// SetVolume sets the volume, 0 to 1000. Above 100 the node amplifies and may
// clip.
func (p *Player) SetVolume(ctx context.Context, volume int) error {
	volume = min(max(volume, 0), 1000)
	return p.update(ctx, lavalink.PlayerUpdate{Volume: &volume})
}

// SetEndTime stops the current track early, at a position in it.
func (p *Player) SetEndTime(ctx context.Context, end lavalink.Duration) error {
	return p.update(ctx, lavalink.PlayerUpdate{EndTime: &end})
}

// Skip plays the next track, ignoring [RepeatTrack]. With crossfade on it fades
// into it rather than cutting. The queue moves on the following TrackEnd, like
// lavalink-client: the update only arms the transition, and manualSkip makes
// that end ignore repeat.
func (p *Player) Skip(ctx context.Context) error {
	if next, ok := p.queue.Peek(); ok && p.Playing() && p.crossfading() {
		p.mu.Lock()
		p.manualSkip = true
		p.mu.Unlock()
		err := p.update(ctx, lavalink.PlayerUpdate{
			NextTrack:  lavalink.Value(lavalink.UpdateTrack{Encoded: lavalink.Value(next.Encoded), UserData: next.UserData}),
			Transition: true,
		})
		if err == nil {
			return nil
		}
		p.mu.Lock()
		p.manualSkip = false
		p.mu.Unlock()
		p.log.Debug("gurulink: skip with a crossfade", slog.Any("err", err))
	}
	return p.next(ctx, lavalink.ReasonStopped)
}

// takeManualSkip reports and clears a pending manual skip.
func (p *Player) takeManualSkip() bool {
	p.mu.Lock()
	manual := p.manualSkip
	p.manualSkip = false
	p.mu.Unlock()
	return manual
}

// effectiveNext is the track the node should pre-buffer: the repeat-track
// itself when looping one, the head of the queue otherwise, or the current
// track again when looping a dry queue. Mirrors lavalink-client's
// effectiveNextCandidate.
func (p *Player) effectiveNext() (lavalink.Track, bool) {
	if p.Repeat() == RepeatTrack {
		if current := p.queue.Current(); current != nil {
			return *current, true
		}
	}
	if next, ok := p.queue.Peek(); ok {
		return next, true
	}
	if p.Repeat() == RepeatQueue {
		if current := p.queue.Current(); current != nil {
			return *current, true
		}
	}
	return lavalink.Track{}, false
}

// SkipTo skips the queued tracks before index i and plays that one.
func (p *Player) SkipTo(ctx context.Context, i int) error {
	if i < 0 || i >= p.queue.Len() {
		return fmt.Errorf("gurulink: skip to %d out of range (%d tracks)", i, p.queue.Len())
	}
	p.queue.RemoveRange(ctx, 0, i)
	return p.Skip(ctx)
}

// Back replays the last track, pushing the current one to the front of the
// queue.
func (p *Player) Back(ctx context.Context) error {
	track, ok := p.queue.Back(ctx)
	if !ok {
		return errors.New("gurulink: nothing played yet")
	}
	// Back already made it current.
	p.stopIdle()
	resume := false
	if err := p.update(ctx, lavalink.PlayerUpdate{
		Track:  &lavalink.UpdateTrack{Encoded: lavalink.Value(track.Encoded), UserData: track.UserData},
		Paused: &resume,
	}); err != nil {
		return err
	}
	p.schedulePreBuffer()
	return nil
}

// next plays the following track, asking [Config.Autoplay] when the queue is dry
// and emitting [QueueEndEvent] when that is empty too.
func (p *Player) next(ctx context.Context, reason lavalink.TrackEndReason) error {
	ended := p.queue.Current()
	track, ok := p.queue.Advance(ctx)
	if !ok && p.client.cfg.Autoplay != nil {
		if err := p.client.cfg.Autoplay(ctx, p); err != nil {
			p.client.emit(&ErrorEvent{Node: p.Node(), Err: fmt.Errorf("gurulink: autoplay: %w", err)})
		}
		track, ok = p.queue.Advance(ctx)
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
	// Skipping the last track has to stop the audio.
	return p.update(ctx, lavalink.PlayerUpdate{Track: &lavalink.UpdateTrack{Encoded: lavalink.Null[string]()}})
}

// Crossfade is [Player.SetCrossfade]'s override, else [Config.Crossfade].
func (p *Player) Crossfade() *lavalink.Crossfade {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.crossfade != nil {
		return p.crossfade
	}
	return p.client.cfg.Crossfade
}

// SetCrossfade overrides [Config.Crossfade] and tells the node now: nil falls
// back to the client, Enable false turns crossfading off. Needs a Kairo node.
func (p *Player) SetCrossfade(ctx context.Context, crossfade *lavalink.Crossfade) error {
	p.mu.Lock()
	p.crossfade = crossfade
	p.mu.Unlock()

	if p.crossfading() {
		return p.PreBuffer(ctx)
	}
	// Off: drop the successor too, so the node has nothing left to fade into.
	return p.update(ctx, lavalink.PlayerUpdate{
		Crossfade: lavalink.Null[lavalink.Crossfade](),
		NextTrack: lavalink.Null[lavalink.UpdateTrack](),
	})
}

// Tape is [Player.SetTape]'s override, else [Config.Tape].
func (p *Player) Tape() *lavalink.Tape {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.tape != nil {
		return p.tape
	}
	return p.client.cfg.Tape
}

// SetTape overrides [Config.Tape]: nil falls back to the client, Enable false
// makes pausing instant. Needs a Kairo node.
func (p *Player) SetTape(ctx context.Context, tape *lavalink.Tape) error {
	p.mu.Lock()
	p.tape = tape
	p.mu.Unlock()

	update := lavalink.PlayerUpdate{Tape: lavalink.Null[lavalink.Tape]()}
	if effective := p.Tape(); effective != nil && effective.Enable {
		update.Tape = lavalink.Value(*effective)
	}
	return p.update(ctx, update)
}

// PreBuffer names the successor so the node can overlap the two. Done on every
// track start and after every queue change; call it again after editing the
// queue. Needs a Kairo node.
func (p *Player) PreBuffer(ctx context.Context) error {
	crossfade := p.Crossfade()
	if crossfade == nil || !crossfade.Enable {
		return nil
	}
	// A dry queue clears the successor rather than fading into a stale track.
	// Repeat modes name the looped track, like lavalink-client's
	// effectiveNextCandidate, so a looped track fades into itself instead of
	// cutting or stalling.
	update := lavalink.PlayerUpdate{Crossfade: lavalink.Value(*crossfade), NextTrack: lavalink.Null[lavalink.UpdateTrack]()}
	if next, ok := p.effectiveNext(); ok {
		update.NextTrack = lavalink.Value(lavalink.UpdateTrack{Encoded: lavalink.Value(next.Encoded), UserData: next.UserData})
	}
	return p.update(ctx, update)
}

// SetFilters replaces the player's filters.
func (p *Player) SetFilters(ctx context.Context, filters lavalink.Filters) error {
	return p.update(ctx, lavalink.PlayerUpdate{Filters: &filters})
}

// UpdateFilters changes one filter without rebuilding the rest:
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

// Search resolves a query on this player's own node. See [Client.Search].
func (p *Player) Search(ctx context.Context, query, source string) (lavalink.LoadResult, error) {
	return p.client.searchOn(ctx, p.Node(), query, source)
}
