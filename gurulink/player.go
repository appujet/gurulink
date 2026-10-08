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
	client     *Client
	guildID    string
	queue      *queue.Queue
	log        *slog.Logger
	cmdMu      sync.Mutex
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
	idleTimer  *time.Timer
	destroyed  bool
	data       map[string]any
	// inCommand says a command holds cmdMu, so callbacks into user code wait in
	// pending until it is free. See [Player.cmd].
	inCommand bool
	pending   []func()
	// playing is the node's word for "a track is decoding", kept across a pause
	// like lavalink-client's player.playing. [Player.Playing] is the narrower
	// public question and stays derived.
	playing bool

	// These five mirror lavalink-client's internal_* player data: one-shot
	// intents a command leaves behind for the event that follows it.
	//
	// manualSkip is internal_manualSkipPending: an explicit skip through a
	// crossfade transition, so the following TrackEnd advances past RepeatTrack.
	manualSkip bool
	// skipped is internal_skipped: a skip stopped the track, so the following
	// TrackEnd (stopped) advances instead of being ignored.
	skipped bool
	// stopPlaying is internal_stopPlaying: a Stop did, so that same reason ends
	// the queue instead.
	stopPlaying bool
	// nodeChanging is internal_nodeChanging: silence the track events while the
	// player is rebuilt elsewhere, so the old node's events cannot drive it.
	nodeChanging bool
	// filtersDirty is FilterManager.filterUpdatedState: ask the next
	// playerUpdate for one re-seek, because a filter change can leave the node
	// decoding from a stale position.
	filtersDirty bool

	// errorAt is internal_erroredTracksTimestamps, a sliding window: failures
	// expire, so a long-lived player is not torn down by errors hours apart.
	errorAt []time.Time
	// autoplaying and autoplayFailed are internal_autoplay_in_progress and
	// internal_autoplay_failed_at: no re-entry, and no retry storm against a
	// source that is down.
	autoplaying    bool
	autoplayFailed time.Time

	// nextSyncTimer debounces NextTrack re-syncs after queue edits, like
	// lavalink-client's scheduleNextTrackSync (50ms): rapid edits coalesce into
	// one node call, and the generation lets a later edit cancel an earlier
	// sync that is still resolving.
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
			// Held until the command lock is free, for the same reason events
			// are: a now-playing handler that calls back into the player must
			// not deadlock against the command that changed the queue. The
			// store's own write stays inline, so persistence order is kept.
			p.notify(func() { outer(ctx, guildID, change, tracks) })
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

func (p *Player) applyState(state lavalink.PlayerState) bool {
	if !state.Time.IsZero() && !p.state.Time.IsZero() && state.Time.Before(p.state.Time.Time) {
		return false
	}
	p.state, p.stateAt = state, time.Now()
	return true
}

// setState records a playerUpdate frame and reports whether it was the newest.
func (p *Player) setState(state lavalink.PlayerState) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.applyState(state)
}

// absorb takes the node's word for the player's state after a command. A reply
// the node stamped before one already taken is stale whole: its volume, pause
// and filters are as out of date as its position.
func (p *Player) absorb(info lavalink.PlayerInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// A reply that carries no state at all says nothing about the timeline, so
	// it must not wipe a position just set locally — a seek would be undone by
	// its own reply. A real node always sends one; this is the guard for the
	// ones that do not.
	if info.State != (lavalink.PlayerState{}) && !p.applyState(info.State) {
		// Stale whole: a reply the node stamped before one already taken has a
		// volume and pause as out of date as its position.
		return
	}
	p.volume, p.paused, p.filters = info.Volume, info.Paused, info.Filters
	// Take the node's word for the transition settings too. It reports what is
	// actually in effect, with its own defaults filled in, and trusting the
	// local cache instead is what let a player claim crossfade was on while the
	// node was running its own configuration. lavalink-client mirrors
	// res.crossfade for the same reason. Stock Lavalink never sends the field,
	// which leaves the override alone.
	if !info.Crossfade.IsZero() {
		if crossfade, ok := info.Crossfade.Get(); ok {
			p.crossfade = &crossfade
		} else {
			// Explicitly null: no transitions, and not a reason to fall back to
			// Config.Crossfade.
			p.crossfade = &lavalink.Crossfade{}
		}
	}
}

// markPosition moves the local timeline without waiting for the node, the way
// lavalink-client's seek() writes lastPosition/lastPositionChange before its
// request. Without it Position() keeps interpolating the old timeline for a
// whole round trip, and a frame stamped before the seek can undo it entirely.
func (p *Player) markPosition(position lavalink.Duration) {
	now := time.Now()
	p.mu.Lock()
	p.state.Position, p.stateAt = position, now
	p.state.Time = lavalink.Timestamp{Time: now}
	p.mu.Unlock()
}

func (p *Player) restart(ticking bool) {
	p.mu.Lock()
	p.state.Position = 0
	if ticking {
		p.stateAt = time.Now()
	} else {
		p.stateAt = time.Time{}
	}
	p.mu.Unlock()
}

// resetStateClock reopens the freshness gate: a player that moved to another node
// must not hold the new node's frames against the old node's clock.
func (p *Player) resetStateClock() {
	p.mu.Lock()
	p.state.Time = lavalink.Timestamp{}
	p.mu.Unlock()
}

// cmdKey marks a context as already running inside a player's command.
type cmdKey struct{}

// cmd runs a track-changing command under cmdMu. Every path that edits the queue
// and tells the node about it in the same breath goes through here.
//
// User code never runs with the lock held: events and [Config.OnQueueChange] are
// held in p.pending and flushed once it is free, because a listener that calls
// back into the player would otherwise deadlock against the very command that
// triggered it — a QueueEndEvent handler calling Play is the obvious case.
// [Config.Autoplay] has to run inline, so it is covered by the context token
// instead.
func (p *Player) cmd(ctx context.Context, f func(context.Context) error) error {
	// Already inside this player's command on this call path: re-entering is
	// safe and must not block, so run inline rather than wait for a lock this
	// goroutine is holding.
	if ctx.Value(cmdKey{}) == p {
		return f(ctx)
	}
	p.cmdMu.Lock()
	p.mu.Lock()
	p.inCommand = true
	p.mu.Unlock()

	// Flushed on the way out, panic or not, and only once the lock is free.
	defer func() {
		p.mu.Lock()
		p.inCommand = false
		pending := p.pending
		p.pending = nil
		p.mu.Unlock()
		p.cmdMu.Unlock()
		for _, notify := range pending {
			notify()
		}
	}()
	return f(context.WithValue(ctx, cmdKey{}, p))
}

// notify runs a callback into user code, held until the command lock is free
// when one is held. See [Player.cmd].
func (p *Player) notify(f func()) {
	p.mu.Lock()
	if p.inCommand {
		p.pending = append(p.pending, f)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	f()
}

// emit sends a player event to the client's listeners, held while a command is
// running so a listener can call straight back into the player.
func (p *Player) emit(event Event) {
	p.notify(func() { p.client.emit(event) })
}

// goCmd runs one off the node's read loop, which must never block on a request.
func (p *Player) goCmd(f func(context.Context) error) {
	go p.background(func(ctx context.Context) error { return p.cmd(ctx, f) })
}

// started reports whether the node told us a track is decoding, which is
// lavalink-client's player.playing: unlike [Player.Playing] it stays true across
// a pause, because the node still holds the track.
func (p *Player) started() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.playing
}

// setStarted records the node starting or dropping a track.
func (p *Player) setStarted(playing bool) {
	p.mu.Lock()
	p.playing = playing
	p.mu.Unlock()
}

// changingNode reports whether a [Player.MoveNode] is in flight.
func (p *Player) changingNode() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nodeChanging
}

// Set attaches arbitrary data to the player, like lavalink-client's
// Player#setData: the usual home for a text channel id or a requester.
func (p *Player) Set(key string, value any) {
	p.mu.Lock()
	if p.data == nil {
		p.data = map[string]any{}
	}
	p.data[key] = value
	p.mu.Unlock()
}

// Get returns data [Player.Set] attached, or nil.
func (p *Player) Get(key string) any {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.data[key]
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
	return p.cmd(ctx, func(ctx context.Context) error { return p.play(ctx, track, false) })
}

func (p *Player) play(ctx context.Context, track lavalink.Track, noReplace bool) error {
	p.stopIdle()
	// A new track supersedes whatever the last command asked the coming TrackEnd
	// to do: that end belongs to a track nobody is waiting on any more, and a
	// flag left set would steer the wrong one.
	p.takeIntent()
	p.queue.SetCurrent(ctx, &track)
	resume := false
	update := lavalink.PlayerUpdate{
		Track:     &lavalink.UpdateTrack{Encoded: lavalink.Value(track.Encoded), UserData: track.UserData},
		Paused:    &resume,
		NoReplace: noReplace,
	}
	if crossfade := p.Crossfade(); crossfade != nil && crossfade.Enable {
		update.Crossfade = lavalink.Value(*crossfade)
	}
	if err := p.update(ctx, update); err != nil {
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
	return p.cmd(ctx, func(ctx context.Context) error {
		p.stopIdle()
		resume := false
		return p.update(ctx, lavalink.PlayerUpdate{
			Track:  &lavalink.UpdateTrack{Identifier: identifier},
			Paused: &resume,
		})
	})
}

// Stop stops playback and clears the queue, leaving the player connected.
func (p *Player) Stop(ctx context.Context) error {
	return p.cmd(ctx, func(ctx context.Context) error {
		p.queue.Clear(ctx)
		p.queue.SetCurrent(ctx, nil)
		p.mu.Lock()
		p.stopPlaying = true
		p.mu.Unlock()
		err := p.update(ctx, lavalink.PlayerUpdate{Track: &lavalink.UpdateTrack{Encoded: lavalink.Null[string]()}})
		if err != nil {
			p.mu.Lock()
			p.stopPlaying = false
			p.mu.Unlock()
		}
		return err
	})
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
		p.emit(&PlayerPauseEvent{Player: p, Paused: now})
	}
	return nil
}

// Resume unpauses playback.
func (p *Player) Resume(ctx context.Context) error { return p.Pause(ctx, false) }

// ErrNotSeekable is returned by [Player.Seek] for a live stream, or any track the
// node reported as not seekable.
var ErrNotSeekable = errors.New("gurulink: current track is not seekable")

// Seek jumps to a position in the current track. Nothing playing is a no-op; a
// position past the end is clamped to it.
func (p *Player) Seek(ctx context.Context, position lavalink.Duration) error {
	// The same three guards as lavalink-client's seek(), so an impossible seek
	// never reaches the node and comes back as a REST error.
	current := p.queue.Current()
	if current == nil {
		return nil
	}
	if current.Info.IsStream || !current.Info.IsSeekable {
		return ErrNotSeekable
	}
	if length := current.Info.Length; length > 0 {
		position = min(position, length)
	}
	position = max(position, 0)
	p.markPosition(position)
	return p.update(ctx, lavalink.PlayerUpdate{Position: &position})
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
// into it rather than cutting.
//
// Either way the queue moves on the following TrackEnd, never here: that is
// lavalink-client's design, and it is what keeps one writer on the queue. A skip
// that advanced locally would race the end of the track it is skipping, and the
// two together would consume two tracks for one press.
func (p *Player) Skip(ctx context.Context) error {
	return p.cmd(ctx, p.skip)
}

// skip is [Player.Skip] with cmdMu already held.
func (p *Player) skip(ctx context.Context) error {
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
	if p.queue.Current() == nil {
		return p.next(ctx, lavalink.ReasonStopped)
	}
	p.mu.Lock()
	p.skipped = true
	p.mu.Unlock()
	resume := false
	err := p.update(ctx, lavalink.PlayerUpdate{
		Track:  &lavalink.UpdateTrack{Encoded: lavalink.Null[string]()},
		Paused: &resume,
	})
	if err != nil {
		p.mu.Lock()
		p.skipped = false
		p.mu.Unlock()
	}
	return err
}

// takeManualSkip reports and clears a pending manual skip.
func (p *Player) takeManualSkip() bool {
	p.mu.Lock()
	manual := p.manualSkip
	p.manualSkip = false
	p.mu.Unlock()
	return manual
}

func (p *Player) takeIntent() (skipped, stopped bool) {
	p.mu.Lock()
	skipped, stopped = p.skipped, p.stopPlaying
	p.skipped, p.stopPlaying = false, false
	p.mu.Unlock()
	return skipped, stopped
}

func (p *Player) advance(ctx context.Context) (lavalink.Track, bool) {
	if p.Repeat() == RepeatQueue {
		if current := p.queue.Current(); current != nil && !p.queue.Replayed() {
			p.queue.Add(ctx, *current)
		}
	}
	return p.queue.Advance(ctx)
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
	return p.cmd(ctx, func(ctx context.Context) error {
		if i < 0 || i >= p.queue.Len() {
			return fmt.Errorf("gurulink: skip to %d out of range (%d tracks)", i, p.queue.Len())
		}
		p.queue.RemoveRange(ctx, 0, i)
		return p.skip(ctx)
	})
}

// Back replays the last track, pushing the current one to the front of the
// queue.
func (p *Player) Back(ctx context.Context) error {
	return p.cmd(ctx, func(ctx context.Context) error {
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
	})
}

func (p *Player) autoplay(ctx context.Context) bool {
	if p.client.cfg.Autoplay == nil {
		return false
	}
	p.mu.Lock()
	if p.autoplaying || time.Since(p.autoplayFailed) < p.client.cfg.AutoplayCooldown {
		p.mu.Unlock()
		return false
	}
	p.autoplaying = true
	p.mu.Unlock()

	before := p.queue.Len()
	err := p.client.cfg.Autoplay(ctx, p)
	grew := p.queue.Len() > before

	p.mu.Lock()
	p.autoplaying = false
	if !grew {
		p.autoplayFailed = time.Now()
	} else {
		p.autoplayFailed = time.Time{}
	}
	p.mu.Unlock()

	if err != nil {
		p.emit(&ErrorEvent{Node: p.Node(), Err: fmt.Errorf("gurulink: autoplay: %w", err)})
	}
	return grew
}

// next plays the following track, asking [Config.Autoplay] when the queue is dry
// and emitting [QueueEndEvent] when that is empty too.
func (p *Player) next(ctx context.Context, reason lavalink.TrackEndReason) error {
	ended := p.queue.Current()
	track, ok := p.advance(ctx)
	if !ok && p.autoplay(ctx) {
		track, ok = p.advance(ctx)
	}
	if ok {

		return p.play(ctx, track, true)
	}

	p.setStarted(false)
	p.startIdle()
	var last lavalink.Track
	if ended != nil {
		last = *ended
	}
	p.emit(&QueueEndEvent{Player: p, Track: last, Reason: reason})
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

	return p.cmd(ctx, func(ctx context.Context) error {
		if p.crossfading() {
			return p.preBuffer(ctx)
		}
		return p.update(ctx, lavalink.PlayerUpdate{
			Crossfade: lavalink.Null[lavalink.Crossfade](),
			NextTrack: lavalink.Null[lavalink.UpdateTrack](),
		})
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
	return p.cmd(ctx, p.preBuffer)
}

// preBuffer is [Player.PreBuffer] with cmdMu already held.
func (p *Player) preBuffer(ctx context.Context) error {
	crossfade := p.Crossfade()
	if crossfade == nil || !crossfade.Enable {
		return nil
	}
	if p.changingNode() {
		return nil
	}
	p.mu.RLock()
	gen := p.nextSyncGen
	p.mu.RUnlock()

	// A dry queue clears the successor rather than fading into a stale track.
	// Repeat modes name the looped track, like lavalink-client's
	// effectiveNextCandidate, so a looped track fades into itself instead of
	// cutting or stalling.
	update := lavalink.PlayerUpdate{Crossfade: lavalink.Value(*crossfade), NextTrack: lavalink.Null[lavalink.UpdateTrack]()}
	next, ok := p.effectiveNext()
	if !ok && p.Repeat() == RepeatOff && p.started() && p.queue.Current() != nil && p.autoplay(ctx) {
		next, ok = p.effectiveNext()
	}
	if ok {
		update.NextTrack = lavalink.Value(lavalink.UpdateTrack{Encoded: lavalink.Value(next.Encoded), UserData: next.UserData})
	}
	// Re-check after the work above, which can block on autoplay: a newer edit
	// means a newer sync is already on its way, and this one would overwrite it
	// with a stale successor. lavalink-client re-checks its generation the same
	// way after awaiting.
	p.mu.RLock()
	stale := gen != p.nextSyncGen || p.nodeChanging
	p.mu.RUnlock()
	if stale {
		return nil
	}
	return p.update(ctx, update)
}

// SetFilters replaces the player's filters.
func (p *Player) SetFilters(ctx context.Context, filters lavalink.Filters) error {
	if err := p.update(ctx, lavalink.PlayerUpdate{Filters: &filters}); err != nil {
		return err
	}
	if filters.Active() {
		p.mu.Lock()
		p.filtersDirty = true
		p.mu.Unlock()
	}
	return nil
}

// takeFiltersDirty reports and clears the pending filter re-seek.
func (p *Player) takeFiltersDirty() bool {
	p.mu.Lock()
	dirty := p.filtersDirty
	p.filtersDirty = false
	p.mu.Unlock()
	return dirty
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
