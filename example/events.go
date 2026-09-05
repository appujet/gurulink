package main

import (
	"log/slog"
	"slices"

	"github.com/appujet/gurulink/gurulink"
)

// listeners is one handler per event, all 38: a real bot wants a handful.
func listeners() []gurulink.Listener {
	return slices.Concat(nodeListeners(), playbackListeners(), voiceListeners(), pluginListeners())
}

func nodeListeners() []gurulink.Listener {
	return []gurulink.Listener{
		gurulink.On(func(e *gurulink.ConnectEvent) {
			slog.Info("node connected", slog.String("node", e.Node.Name()))
		}),
		gurulink.On(func(e *gurulink.ReadyEvent) {
			slog.Info("node ready", slog.String("node", e.Node.Name()),
				slog.String("session", e.SessionID), slog.Bool("resumed", e.Resumed))
		}),
		gurulink.On(func(e *gurulink.StatsEvent) {
			slog.Debug("node stats", slog.String("node", e.Node.Name()),
				slog.Int("players", e.Players), slog.Int("playing", e.PlayingPlayers),
				slog.Float64("cpu", e.CPU.LavalinkLoad), slog.String("uptime", e.Uptime.String()))
		}),
		gurulink.On(func(e *gurulink.DisconnectEvent) {
			slog.Warn("node disconnected", slog.String("node", e.Node.Name()),
				slog.Int("code", e.Code), slog.String("reason", e.Reason))
		}),
		gurulink.On(func(e *gurulink.ReconnectEvent) {
			slog.Info("node reconnecting", slog.String("node", e.Node.Name()),
				slog.Int("attempt", e.Attempt), slog.String("in", e.Delay.String()))
		}),
		gurulink.On(func(e *gurulink.ResumedEvent) {
			// After a restart these are players this process knows nothing about.
			slog.Info("node resumed our session", slog.String("node", e.Node.Name()),
				slog.Int("players", len(e.Players)))
		}),
		gurulink.On(func(e *gurulink.NodeRemovedEvent) {
			slog.Info("node removed", slog.String("node", e.Node.Name()))
		}),
		gurulink.On(func(e *gurulink.ErrorEvent) {
			slog.Error("gurulink error", slog.String("node", nodeName(e.Node)), slog.Any("err", e.Err))
		}),
	}
}

func nodeName(node *gurulink.Node) string {
	if node == nil {
		return "none"
	}
	return node.Name()
}

func playbackListeners() []gurulink.Listener {
	return []gurulink.Listener{
		gurulink.On(func(e *gurulink.PlayerCreateEvent) {
			slog.Info("player created", slog.String("guild", e.Player.GuildID()))
		}),
		gurulink.On(func(e *gurulink.PlayerDestroyEvent) {
			slog.Info("player destroyed", slog.String("guild", e.Player.GuildID()),
				slog.String("reason", string(e.Reason)))
		}),
		gurulink.On(func(e *gurulink.PlayerUpdateEvent) {
			// One per player every few seconds: only useful for a progress bar.
			slog.Debug("player update", slog.String("guild", e.Player.GuildID()),
				slog.String("position", e.State.Position.String()), slog.Int("ping", e.State.Ping))
		}),
		gurulink.On(func(e *gurulink.TrackStartEvent) {
			slog.Info("track start", slog.String("guild", e.Player.GuildID()),
				slog.String("title", e.Track.Info.Title), slog.String("author", e.Track.Info.Author))
		}),
		gurulink.On(func(e *gurulink.TrackPromotedEvent) {
			slog.Info("crossfaded into next track", slog.String("guild", e.Player.GuildID()),
				slog.String("title", e.Track.Info.Title))
		}),
		gurulink.On(func(e *gurulink.TrackEndEvent) {
			slog.Info("track end", slog.String("guild", e.Player.GuildID()),
				slog.String("title", e.Track.Info.Title), slog.String("reason", string(e.Reason)))
		}),
		gurulink.On(func(e *gurulink.TrackExceptionEvent) {
			slog.Error("track failed", slog.String("guild", e.Player.GuildID()),
				slog.String("title", e.Track.Info.Title), slog.String("message", e.Exception.Message),
				slog.String("severity", string(e.Exception.Severity)))
		}),
		gurulink.On(func(e *gurulink.TrackStuckEvent) {
			slog.Warn("track stuck", slog.String("guild", e.Player.GuildID()),
				slog.String("title", e.Track.Info.Title), slog.String("threshold", e.ThresholdMs.String()))
		}),
		gurulink.On(func(e *gurulink.QueueEndEvent) {
			slog.Info("queue end", slog.String("guild", e.Player.GuildID()),
				slog.String("after", e.Track.Info.Title))
		}),
		gurulink.On(func(e *gurulink.IdleStartEvent) {
			slog.Info("idle, leaving soon", slog.String("guild", e.Player.GuildID()),
				slog.String("in", e.Timeout.String()))
		}),
		gurulink.On(func(e *gurulink.IdleCancelEvent) {
			slog.Info("no longer idle", slog.String("guild", e.Player.GuildID()))
		}),
	}
}

func voiceListeners() []gurulink.Listener {
	return []gurulink.Listener{
		gurulink.On(func(e *gurulink.PlayerChannelMoveEvent) {
			slog.Info("moved channel", slog.String("guild", e.Player.GuildID()),
				slog.String("from", e.From), slog.String("to", e.To))
		}),
		gurulink.On(func(e *gurulink.PlayerNodeMoveEvent) {
			slog.Info("moved node", slog.String("guild", e.Player.GuildID()),
				slog.String("from", e.From.Name()), slog.String("to", e.To.Name()))
		}),
		gurulink.On(func(e *gurulink.PlayerPauseEvent) {
			slog.Info("pause changed", slog.String("guild", e.Player.GuildID()), slog.Bool("paused", e.Paused))
		}),
		gurulink.On(func(e *gurulink.PlayerDisconnectEvent) {
			slog.Info("left voice", slog.String("guild", e.Player.GuildID()), slog.String("channel", e.ChannelID))
		}),
		gurulink.On(func(e *gurulink.PlayerReconnectEvent) {
			slog.Warn("rebuilding voice session", slog.String("guild", e.Player.GuildID()),
				slog.String("channel", e.ChannelID))
		}),
		gurulink.On(func(e *gurulink.WebSocketClosedEvent) {
			slog.Warn("voice socket closed", slog.String("guild", e.Player.GuildID()),
				slog.Int("code", e.Code), slog.String("reason", e.Reason), slog.Bool("remote", e.ByRemote))
		}),
		gurulink.On(func(e *gurulink.PlayerMuteChangeEvent) {
			slog.Info("mute changed", slog.String("guild", e.Player.GuildID()),
				slog.Bool("self", e.SelfMute), slog.Bool("server", e.ServerMute))
		}),
		gurulink.On(func(e *gurulink.PlayerDeafChangeEvent) {
			slog.Info("deaf changed", slog.String("guild", e.Player.GuildID()),
				slog.Bool("self", e.SelfDeaf), slog.Bool("server", e.ServerDeaf))
		}),
		gurulink.On(func(e *gurulink.PlayerSuppressChangeEvent) {
			slog.Info("suppress changed", slog.String("guild", e.Player.GuildID()), slog.Bool("suppress", e.Suppress))
		}),
		gurulink.On(func(e *gurulink.PlayerVoiceJoinEvent) {
			slog.Info("user joined our channel", slog.String("guild", e.Player.GuildID()),
				slog.String("user", e.UserID))
		}),
		gurulink.On(func(e *gurulink.PlayerVoiceLeaveEvent) {
			slog.Info("user left our channel", slog.String("guild", e.Player.GuildID()),
				slog.String("user", e.UserID))
		}),
	}
}

// Plugin events need the node to run that plugin.
func pluginListeners() []gurulink.Listener {
	return []gurulink.Listener{
		gurulink.On(func(e *gurulink.SegmentsLoadedEvent) {
			slog.Info("sponsorblock segments", slog.String("guild", e.Player.GuildID()),
				slog.Int("count", len(e.Segments)))
		}),
		gurulink.On(func(e *gurulink.SegmentSkippedEvent) {
			slog.Info("segment skipped", slog.String("guild", e.Player.GuildID()),
				slog.String("category", e.Segment.Category))
		}),
		gurulink.On(func(e *gurulink.ChaptersLoadedEvent) {
			slog.Info("chapters", slog.String("guild", e.Player.GuildID()), slog.Int("count", len(e.Chapters)))
		}),
		gurulink.On(func(e *gurulink.ChapterStartedEvent) {
			slog.Info("chapter started", slog.String("guild", e.Player.GuildID()),
				slog.String("name", e.Chapter.Name))
		}),
		gurulink.On(func(e *gurulink.LyricsFoundEvent) {
			slog.Info("lyrics found", slog.String("guild", e.Player.GuildID()),
				slog.String("provider", e.Lyrics.Provider), slog.Int("lines", len(e.Lyrics.Lines)))
		}),
		gurulink.On(func(e *gurulink.LyricsNotFoundEvent) {
			slog.Info("no lyrics", slog.String("guild", e.Player.GuildID()))
		}),
		gurulink.On(func(e *gurulink.LyricsLineEvent) {
			slog.Debug("lyrics line", slog.String("guild", e.Player.GuildID()),
				slog.Int("index", e.LineIndex), slog.String("line", e.Line.Line), slog.Bool("skipped", e.Skipped))
		}),
		gurulink.On(func(e *gurulink.UnknownEvent) {
			slog.Warn("event this client cannot name", slog.String("node", nodeName(e.Node)),
				slog.String("guild", e.GuildID), slog.String("type", string(e.Type)),
				slog.String("data", string(e.Data)))
		}),
	}
}
