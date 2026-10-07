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
	switch e := event.(type) {
	case *TrackStartEvent:
		p.mu.Lock()
		p.errors = 0
		p.mu.Unlock()
		p.stopIdle()
		// [Player.PlayIdentifier] and a resumed session leave us without one.
		if p.queue.Current() == nil {
			p.queue.SetCurrent(ctx, &e.Track)
		}
		p.schedulePreBuffer()

	case *TrackPromotedEvent:
		p.promoted(ctx, e.Track)

	case *TrackEndEvent:
		p.ended(ctx, e)

	case *TrackExceptionEvent:
		// The node follows this with a TrackEnd that moves on, so only count it.
		p.trackFailed()

	case *TrackStuckEvent:
		// A stuck track gets no TrackEnd, so it needs skipping from here.
		if !p.trackFailed() {
			go p.background(func(ctx context.Context) error { return p.next(ctx, lavalink.ReasonLoadFailed) })
		}

	case *WebSocketClosedEvent:
		p.voiceClosed(e.Code)
	}
}

// ended decides what follows a finished track.
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
	// Stopped and replaced mean somebody else is driving (a skip via next(),
	// a fresh play, or a stop): the queue already moved.
	if !e.Reason.StartNext() {
		return
	}
	switch p.Repeat() {
	case RepeatTrack:
		go p.background(func(ctx context.Context) error { return p.play(ctx, e.Track) })
		return
	case RepeatQueue:
		p.queue.Add(ctx, e.Track)
	}
	go p.background(func(ctx context.Context) error { return p.next(ctx, e.Reason) })
}

// endedTransition advances the queue after a crossfade/gapless handoff. The
// node is already playing the successor, so this never calls play(): it only
// retires the outgoing track and makes the head current, then pre-buffers the
// following one.
func (p *Player) endedTransition(ctx context.Context, e *TrackEndEvent) {
	manual := p.takeManualSkip()
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
	if !manual && p.Repeat() == RepeatQueue {
		p.queue.Add(ctx, e.Track)
	}
	if _, ok := p.queue.Advance(ctx); !ok {
		// Nothing was waiting (repeat off, or repeat-track with a dry queue
		// that PreBuffer cleared): the successor the node holds is all there
		// is, so take the node's word rather than clearing to nil and
		// replaying the head later.
		// ponytail: without a promoted payload here we keep the outgoing
		// current; the following TrackPromotedEvent will correct it.
		p.log.Debug("gurulink: transition with nothing queued")
	}
	p.schedulePreBuffer()
}

// promoted is state-sync only — the preceding trackEnd (crossfade/gapless)
// already advanced the queue, and the node is already playing the successor,
// so no play request must be sent. This mirrors lavalink-client's
// trackPromoted, which never touches the queue head.
func (p *Player) promoted(ctx context.Context, track lavalink.Track) {
	// Resumed sessions and PlayIdentifier leave us without one.
	if p.queue.Current() == nil {
		p.queue.SetCurrent(ctx, &track)
		p.schedulePreBuffer()
		return
	}
	// Already advanced by the preceding trackEnd: nothing to do.
	if current := p.queue.Current(); current != nil && current.Encoded == track.Encoded {
		p.schedulePreBuffer()
		return
	}
	// The queue moved under the pre-buffer (an AddNext, a removal, a shuffle
	// between PreBuffer and promotion): the node is playing `track`, so drop
	// that copy from the waiting list instead of leaving it to replay, then
	// take the node's word for what is current.
	if at := p.queue.Find(func(t lavalink.Track) bool { return t.Encoded == track.Encoded }); at >= 0 {
		p.queue.Remove(ctx, at)
	}
	p.queue.SetCurrent(ctx, &track)
	p.schedulePreBuffer()
}

// trackFailed counts one failed track and reports whether the player gave up.
func (p *Player) trackFailed() bool {
	limit := p.client.cfg.MaxTrackErrors
	p.mu.Lock()
	p.errors++
	count := p.errors
	p.mu.Unlock()

	if limit < 0 || count < limit {
		return false
	}
	p.log.Warn("gurulink: giving up after failed tracks", slog.Int("errors", count))
	go p.background(func(ctx context.Context) error { return p.Destroy(ctx, DestroyTrackErrors) })
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
