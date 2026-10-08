package gurulink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/appujet/gurulink/lavalink"
)

// startIdle arms the empty-queue countdown from [Config.EmptyQueueTimeout].
func (p *Player) startIdle() {
	timeout := p.client.cfg.EmptyQueueTimeout
	if timeout <= 0 {
		return
	}
	p.mu.Lock()
	if p.idleTimer != nil {
		p.idleTimer.Stop()
	}
	p.idleTimer = time.AfterFunc(timeout, func() {
		// A racing fire still gets here after the timer was cancelled.
		if p.queue.Current() != nil || p.queue.Len() > 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Destroy(ctx, DestroyQueueEmpty); err != nil {
			p.log.Warn("gurulink: destroy idle player", slog.Any("err", err))
		}
	})
	p.mu.Unlock()
	p.client.emit(&IdleStartEvent{Player: p, Timeout: timeout})
}

// stopIdle disarms the countdown.
func (p *Player) stopIdle() {
	p.mu.Lock()
	armed := p.idleTimer != nil
	if armed {
		p.idleTimer.Stop()
		p.idleTimer = nil
	}
	p.mu.Unlock()
	if armed {
		p.client.emit(&IdleCancelEvent{Player: p})
	}
}

// handle reacts to a node event after the listeners saw it, on the read loop:
// anything talking back to the node gets its own goroutine and timeout.
func (p *Player) handle(ctx context.Context, event Event) {
	// A player being rebuilt on another node must not be driven by the old
	// node's events: they describe a player that is about to be destroyed, and
	// letting one advance the queue would drive it against the new node
	// mid-rebuild. lavalink-client gates the same handlers on
	// internal_nodeChanging.
	if p.changingNode() {
		switch event.(type) {
		case *TrackStartEvent, *TrackPromotedEvent, *TrackEndEvent, *TrackStuckEvent:
			return
		}
	}

	switch e := event.(type) {
	case *TrackStartEvent:
		p.setStarted(true)
		p.clearErrors()
		p.stopIdle()
		p.adopt(ctx, e.Track)

	case *TrackPromotedEvent:
		p.setStarted(true)
		// The successor starts from the top. Without this Position() reports the
		// outgoing track's elapsed time against it, so a Now Playing progress
		// bar is wrong until the next playerUpdate.
		p.restart(true)
		p.adopt(ctx, e.Track)

	case *TrackEndEvent:
		p.ended(ctx, e)

	case *TrackExceptionEvent:
		// The node follows this with a TrackEnd that moves on, so only count it.
		p.trackFailed()

	case *TrackStuckEvent:
		// A stuck track gets no TrackEnd, so it needs skipping from here.
		if !p.trackFailed() {
			p.goCmd(func(ctx context.Context) error { return p.next(ctx, lavalink.ReasonLoadFailed) })
		}

	case *WebSocketClosedEvent:
		p.voiceClosed(e.Code)
	}
}

// ended decides what follows a finished track. Outside a crossfade handoff this
// is the only place the queue moves on, which is what stops a user skip and a
// natural end from each advancing it.
func (p *Player) ended(ctx context.Context, e *TrackEndEvent) {
	// A pre-buffered successor already took over on the node, so the queue
	// advances here — not on TrackPromotedEvent, which is state-sync only.
	// This mirrors lavalink-client: the preceding trackEnd (crossfade/gapless)
	// already advanced the queue, and the node is already playing the
	// successor, so no play request must follow.
	if e.Reason.Promoted() {
		p.endedTransition(ctx, e)
		return
	}

	if e.Reason == lavalink.ReasonReplaced {
		return
	}

	// Stale: a racing command already moved past this track, so advancing again
	// would consume the one now playing. lavalink-client cannot reach this (one
	// JS event loop), so this guard is the Go equivalent of its serialisation.
	if current := p.queue.Current(); current != nil && current.Encoded != e.Track.Encoded {
		return
	}

	// Read the intent only once this end is the one a command was waiting for.
	// Taking it above the two guards would let a replaced or stale end eat a
	// pending skip, and that skip would then never advance anything.
	skipped, stopped := p.takeIntent()

	// A Stop asked for silence, not the next track.
	if stopped {
		p.setStarted(false)
		p.goCmd(func(ctx context.Context) error { return p.next(ctx, e.Reason) })
		return
	}

	if !skipped && !e.Reason.StartNext() {
		return
	}

	// An explicit skip leaves a RepeatTrack loop, which is lavalink-client's
	// `repeatMode !== "track" || internal_skipped`.
	if !skipped && p.Repeat() == RepeatTrack {
		p.goCmd(func(ctx context.Context) error { return p.play(ctx, e.Track, false) })
		return
	}
	p.goCmd(func(ctx context.Context) error { return p.next(ctx, e.Reason) })
}

// endedTransition advances the queue after a crossfade/gapless handoff. The
// node is already playing the successor, so this never calls play(): it only
// retires the outgoing track and makes the head current, then pre-buffers the
// following one.
func (p *Player) endedTransition(ctx context.Context, e *TrackEndEvent) {
	manual := p.takeManualSkip()

	// The successor is already audible from its own start, so the timeline
	// restarts here. Frozen rather than ticking: lavalink-client nulls
	// lastPositionChange in this branch and lets the next playerUpdate start the
	// clock.
	p.restart(false)
	// Already moved on (a duplicate end, or a queue that changed under us):
	// the ended track is no longer current, so there is nothing to retire.
	if current := p.queue.Current(); current == nil || current.Encoded != e.Track.Encoded {
		p.schedulePreBuffer()
		return
	}
	// An explicit skip ignores RepeatTrack, like lavalink-client's
	// internal_manualSkipPending. Otherwise a looped track stays current.
	if !manual && p.Repeat() == RepeatTrack {
		p.schedulePreBuffer()
		return
	}

	// advance() does the repeat-queue re-add, so a skipped track stays in the
	// rotation exactly like one that finished. lavalink-client drops the re-add
	// on a manual skip here but keeps it on the non-transition path; treating
	// both the same is what RepeatQueue is documented to mean.
	//
	// Nothing waiting leaves Current nil, and the TrackPromotedEvent that
	// follows names what the node actually took over with.
	p.advance(ctx)
	p.schedulePreBuffer()
}

// adopt takes the node's word for what is playing. Both of the node's
// "now playing" signals land here: TrackStart and TrackPromoted.
//
// Deliberately stronger than lavalink-client, which only fills Current when it
// is empty. That is safe in JS because nothing can drift; a Go player is driven
// from several goroutines, so without adopting here a divergence would never
// heal and the status would name one track while another is audible. It runs on
// the read loop, so it converges rather than taking the command lock.
func (p *Player) adopt(ctx context.Context, track lavalink.Track) {
	current := p.queue.Current()
	// Only when the node is playing something unexpected: the queue moved under
	// the pre-buffer (an AddNext, a removal, a shuffle between PreBuffer and
	// promotion), so drop that copy from the waiting list rather than leaving it
	// to replay. Matching encodings mean the queue is already in step, and a
	// track legitimately queued twice has to survive.
	if current == nil || current.Encoded != track.Encoded {
		if at := p.queue.Find(func(t lavalink.Track) bool { return t.Encoded == track.Encoded }); at >= 0 {
			p.queue.Remove(ctx, at)
		}
	}
	// SetCurrent de-duplicates, and refreshes UserData when the same track was
	// queued by somebody else.
	p.queue.SetCurrent(ctx, &track)
	p.schedulePreBuffer()
}

// clearErrors forgets the failure window after a track started cleanly.
func (p *Player) clearErrors() {
	p.mu.Lock()
	p.errorAt = nil
	p.mu.Unlock()
}

// trackFailed counts one failed track and reports whether the player gave up.
func (p *Player) trackFailed() bool {
	limit := p.client.cfg.MaxTrackErrors
	if limit < 0 {
		return false
	}
	window := p.client.cfg.TrackErrorWindow
	now := time.Now()

	p.mu.Lock()
	kept := p.errorAt[:0]
	for _, at := range p.errorAt {
		if now.Sub(at) < window {
			kept = append(kept, at)
		}
	}
	p.errorAt = append(kept, now)
	count := len(p.errorAt)
	p.mu.Unlock()

	if count < limit {
		return false
	}
	p.log.Warn("gurulink: giving up after failed tracks",
		slog.Int("errors", count), slog.Duration("within", window))
	p.goCmd(func(ctx context.Context) error { return p.Destroy(ctx, DestroyTrackErrors) })
	return true
}

// voiceClosed reacts to Discord hanging up on the node's voice connection.
func (p *Player) voiceClosed(code int) {
	switch code {
	case CloseCodeDisconnected:
		return

	case CloseCodeSessionInvalid, CloseCodeSessionExpired:
		p.mu.RLock()
		channelID, mute, deaf := p.channelID, p.selfMute, p.selfDeaf
		p.mu.RUnlock()
		if channelID == "" {
			return
		}
		p.client.emit(&PlayerReconnectEvent{Player: p, ChannelID: channelID})
		// Only leaving and rejoining makes Discord hand out a new session.
		go p.background(func(ctx context.Context) error {
			if err := p.Disconnect(ctx); err != nil {
				return err
			}
			return p.Connect(ctx, channelID, mute, deaf)
		})
	}
}

// crossfading reports whether the node should pre-buffer successors.
func (p *Player) crossfading() bool {
	crossfade := p.Crossfade()
	return crossfade != nil && crossfade.Enable
}

// background runs a node call off the read loop, failures as an [ErrorEvent].
func (p *Player) background(f func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := f(ctx); err != nil && !errors.Is(err, ErrPlayerDestroyed) {
		p.client.emit(&ErrorEvent{Node: p.Node(), Err: fmt.Errorf("gurulink: player %s: %w", p.guildID, err)})
	}
}
