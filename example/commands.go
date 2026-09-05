package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/appujet/gurulink/gurulink"
	"github.com/appujet/gurulink/lavalink"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
)

func onCommand(link *gurulink.Client, e *events.ApplicationCommandInteractionCreate) {
	reply := func(format string, args ...any) {
		if err := e.CreateMessage(discord.MessageCreate{Content: fmt.Sprintf(format, args...)}); err != nil {
			slog.Error("reply", slog.Any("err", err))
		}
	}
	guildID := e.GuildID().String()

	switch e.Data.CommandName() {
	case "play":
		voice, ok := e.Client().Caches.VoiceState(*e.GuildID(), e.User().ID)
		if !ok || voice.ChannelID == nil {
			reply("join a voice channel first")
			return
		}
		if err := play(link, guildID, voice.ChannelID.String(), e.SlashCommandInteractionData().String("query"), reply); err != nil {
			slog.Error("play", slog.Any("err", err))
			reply("that did not work: %s", err)
		}

	case "skip":
		player := link.Player(guildID)
		if player == nil {
			reply("nothing is playing")
			return
		}
		if err := player.Skip(context.TODO()); err != nil {
			reply("that did not work: %s", err)
			return
		}
		reply("skipped")

	case "stop":
		player := link.Player(guildID)
		if player == nil {
			reply("nothing is playing")
			return
		}
		if err := player.Destroy(context.TODO(), gurulink.DestroyRequested); err != nil {
			reply("that did not work: %s", err)
			return
		}
		reply("stopped")
	}
}

func play(link *gurulink.Client, guildID, channelID, query string, reply func(string, ...any)) error {
	player, err := link.NewPlayer(guildID)
	if err != nil {
		return err
	}
	ctx := context.TODO()
	if err = player.Connect(ctx, channelID, false, true); err != nil {
		return err
	}

	result, err := player.Search(ctx, query, "")
	if err != nil {
		return err
	}
	if result.Exception != nil {
		return errors.New(result.Exception.Message)
	}
	tracks := result.AllTracks()
	if len(tracks) == 0 {
		reply("nothing found for %q", query)
		return nil
	}
	// A search hands back everything it matched; only a link to a playlist queues
	// more than one.
	if result.LoadType == lavalink.LoadTypeSearch {
		tracks = tracks[:1]
	}

	if player.Playing() {
		player.Queue().Add(tracks...)
		reply("queued %d track(s), first up: %s", len(tracks), tracks[0].Info.Title)
		return nil
	}
	player.Queue().Add(tracks[1:]...)
	if err = player.Play(ctx, tracks[0]); err != nil {
		return err
	}
	reply("playing %s by %s", tracks[0].Info.Title, tracks[0].Info.Author)
	return nil
}
