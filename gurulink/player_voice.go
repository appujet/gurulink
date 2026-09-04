package gurulink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/appujet/gurulink/lavalink"
)

// Connect joins a voice channel, or moves to it. It only asks the Discord
// gateway; the node gets the connection once Discord's two voice updates come
// back through [Client.OnVoiceStateUpdate] and [Client.OnVoiceServerUpdate].
func (p *Player) Connect(ctx context.Context, channelID string, selfMute, selfDeaf bool) error {
	if channelID == "" {
		return errors.New("gurulink: channel id is required")
	}
	p.mu.Lock()
	p.channelID, p.selfMute, p.selfDeaf = channelID, selfMute, selfDeaf
	p.mu.Unlock()
	return p.client.cfg.SendVoiceUpdate(ctx, p.guildID, &channelID, selfMute, selfDeaf)
}

// Disconnect leaves the voice channel but keeps the player and its queue, so
// [Player.Connect] can pick things back up.
func (p *Player) Disconnect(ctx context.Context) error {
	p.mu.Lock()
	p.channelID, p.voice = "", lavalink.VoiceState{}
	p.mu.Unlock()
	return p.client.cfg.SendVoiceUpdate(ctx, p.guildID, nil, false, false)
}

// onVoiceState takes Discord's voice state for the bot. The node needs the session id and the
// channel, and neither changes unless the voice session is rebuilt or the bot is moved.
func (p *Player) onVoiceState(ctx context.Context, channelID, sessionID string) error {
	p.mu.Lock()
	p.channelID = channelID
	before := p.voice
	p.voice.SessionID, p.voice.ChannelID = sessionID, channelID
	voice := p.voice
	p.mu.Unlock()

	if voice == before || !voice.Complete() {
		return nil
	}
	return p.update(ctx, lavalink.PlayerUpdate{Voice: &voice})
}

// onVoiceServer takes Discord's voice server, which is what actually hands the
// connection to a node. A node configured for another region gives the player up
// to one that serves this endpoint.
func (p *Player) onVoiceServer(ctx context.Context, token, endpoint string) error {
	p.mu.Lock()
	p.voice.Token, p.voice.Endpoint = token, endpoint
	voice, node := p.voice, p.node
	p.mu.Unlock()

	if !voice.Complete() {
		return nil
	}
	if !node.serves(endpoint) {
		if target := p.client.bestNode(endpoint); target != nil && target != node {
			return p.MoveNode(ctx, target)
		}
	}
	return p.update(ctx, lavalink.PlayerUpdate{Voice: &voice})
}

// Destroy tears the player down: it leaves the voice channel, drops the node's
// player and forgets the stored queue. The player is unusable afterwards.
func (p *Player) Destroy(ctx context.Context, reason DestroyReason) error {
	p.mu.Lock()
	if p.destroyed {
		p.mu.Unlock()
		return nil
	}
	p.destroyed = true
	if p.idleTimer != nil {
		p.idleTimer.Stop()
		p.idleTimer = nil
	}
	node := p.node
	p.mu.Unlock()

	p.client.forget(p.guildID)
	err := p.client.cfg.SendVoiceUpdate(ctx, p.guildID, nil, false, false)
	// A node that is down or never gave us a session has nothing to destroy.
	if nerr := node.DestroyPlayer(ctx, p.guildID); nerr != nil && !errors.Is(nerr, ErrNoSession) {
		err = errors.Join(err, nerr)
	}
	err = errors.Join(err, p.queue.Delete(ctx))
	p.client.emit(&PlayerDestroyEvent{Player: p, Reason: reason})
	return err
}

// MoveNode rebuilds the player on another node, keeping the queue and the
// position. Use it to drain a node before taking it down.
func (p *Player) MoveNode(ctx context.Context, node *Node) error {
	if node == nil {
		return errors.New("gurulink: nil node")
	}
	p.mu.Lock()
	if p.destroyed {
		p.mu.Unlock()
		return ErrPlayerDestroyed
	}
	from := p.node
	if from == node {
		p.mu.Unlock()
		return nil
	}
	p.node = node
	p.mu.Unlock()

	// Best effort: the usual reason to move is that the old node is gone.
	if err := from.DestroyPlayer(ctx, p.guildID); err != nil && !errors.Is(err, ErrNoSession) {
		p.log.Debug("gurulink: destroy player on old node",
			slog.String("node", from.Name()), slog.Any("err", err))
	}
	if err := p.restore(ctx); err != nil {
		p.mu.Lock()
		p.node = from
		p.mu.Unlock()
		return fmt.Errorf("gurulink: move player to %s: %w", node.Name(), err)
	}
	p.client.emit(&PlayerMoveEvent{Player: p, From: from, To: node})
	return nil
}

// restore rebuilds this player on its node, at the position it had. A node
// calls it after a reconnect it could not resume, and [Player.MoveNode] after a
// move. Without a voice connection there is nothing to rebuild.
func (p *Player) restore(ctx context.Context) error {
	p.mu.RLock()
	voice, volume, paused, filters := p.voice, p.volume, p.paused, p.filters
	p.mu.RUnlock()
	if !voice.Complete() {
		return nil
	}

	update := lavalink.PlayerUpdate{Voice: &voice, Volume: &volume, Paused: &paused, Filters: &filters}
	if current := p.queue.Current(); current != nil {
		position := p.Position()
		update.Track = &lavalink.UpdateTrack{Encoded: lavalink.Value(current.Encoded), UserData: current.UserData}
		update.Position = &position
	}
	return p.update(ctx, update)
}
