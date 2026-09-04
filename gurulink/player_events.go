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
	defer p.mu.Unlock()
	if p.idleTimer != nil {
		p.idleTimer.Stop()
	}
	p.idleTimer = time.AfterFunc(timeout, func() {
		// A track that started in the meantime cancelled the timer, but a racing
		// fire still gets here, so check before pulling the plug.
		if p.queue.Current() != nil || p.queue.Len() > 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.Destroy(ctx, DestroyQueueEmpty); err != nil {
			p.log.Warn("gurulink: destroy idle player", slog.Any("err", err))
		}
	})
}

// stopIdle disarms the countdown.
func (p *Player) stopIdle() {
	p.mu.Lock()
	if p.idleTimer != nil {
		p.idleTimer.Stop()
		p.idleTimer = nil
	}
	p.mu.Unlock()
}

// handle reacts to a node event, after the client's listeners have seen it. It
// runs on the node's read loop, so anything that talks back to a node gets its
// own goroutine and its own timeout.
func (p *Player) handle(event Event) {
	switch e := event.(type) {
	case *TrackStartEvent:
		p.mu.Lock()
		p.errors = 0
		p.mu.Unlock()
		p.stopIdle()
		// [Player.PlayIdentifier] and a resumed session leave us without one.
		if p.queue.Current() == nil {
			p.queue.SetCurrent(&e.Track)
		}
		if p.crossfading() {
			go p.background(p.PreBuffer)
		}

	case *TrackPromotedEvent:
		p.promoted(e.Track)

	case *TrackEndEvent:
		p.ended(e)

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
func (p *Player) ended(e *TrackEndEvent) {
	// A crossfade already started the successor: TrackPromotedEvent moves the
	// queue. Stopped, replaced and cleanup mean somebody else is driving.
	if e.Reason.Promoted() || !e.Reason.StartNext() {
		return
	}
	switch p.Repeat() {
	case RepeatTrack:
		go p.background(func(ctx context.Context) error { return p.play(ctx, e.Track) })
		return
	case RepeatQueue:
		p.queue.Add(e.Track)
	}
	go p.background(func(ctx context.Context) error { return p.next(ctx, e.Reason) })
}

// promoted catches the queue up with a node that already switched to the
// pre-buffered successor.
func (p *Player) promoted(track lavalink.Track) {
	if next, ok := p.queue.Peek(); ok && next.Encoded == track.Encoded {
		p.queue.Advance()
	} else {
		// ponytail: the queue moved under the pre-buffer, so take the node's word
		// for what plays instead of trying to reconcile the two.
		p.queue.SetCurrent(&track)
	}
	if p.crossfading() {
		go p.background(p.PreBuffer)
	}
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
		// Kicked, moved out, or the channel is gone: there is nothing to rejoin.
		go p.background(func(ctx context.Context) error { return p.Destroy(ctx, DestroyVoiceClosed) })

	case CloseCodeSessionInvalid, CloseCodeSessionExpired:
		p.mu.RLock()
		channelID, mute, deaf := p.channelID, p.selfMute, p.selfDeaf
		p.mu.RUnlock()
		if channelID == "" {
			return
		}
		// The voice session is dead for good. Leaving and rejoining is what makes
		// Discord hand out a new one.
		go p.background(func(ctx context.Context) error {
			if err := p.client.cfg.SendVoiceUpdate(ctx, p.guildID, nil, false, false); err != nil {
				return err
			}
			return p.Connect(ctx, channelID, mute, deaf)
		})
	}
}

// crossfading reports whether the node should pre-buffer successors.
func (p *Player) crossfading() bool {
	crossfade := p.client.cfg.Crossfade
	return crossfade != nil && crossfade.Enable
}

// background runs a node call off the read loop and reports the failure as an
// [ErrorEvent], since nobody is waiting for it.
func (p *Player) background(f func(ctx context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := f(ctx); err != nil && !errors.Is(err, ErrPlayerDestroyed) {
		p.client.emit(&ErrorEvent{Node: p.Node(), Err: fmt.Errorf("gurulink: player %s: %w", p.guildID, err)})
	}
}
