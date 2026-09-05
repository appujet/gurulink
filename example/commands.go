package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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

	// An interaction has its own deadline, so give the calls it makes one.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch e.Data.CommandName() {
	case "play":
		voice, ok := e.Client().Caches.VoiceState(*e.GuildID(), e.User().ID)
		if !ok || voice.ChannelID == nil {
			reply("join a voice channel first")
			return
		}
		if err := play(ctx, link, guildID, voice.ChannelID.String(), e.SlashCommandInteractionData().String("query"), reply); err != nil {
			slog.Error("play", slog.Any("err", err))
			reply("that did not work: %s", err)
		}

	case "skip":
		player := link.Player(guildID)
		if player == nil {
			reply("nothing is playing")
			return
		}
		if err := player.Skip(ctx); err != nil {
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
		if err := player.Destroy(ctx, gurulink.DestroyRequested); err != nil {
			reply("that did not work: %s", err)
			return
		}
		reply("stopped")
	}
}

func play(ctx context.Context, link *gurulink.Client, guildID, channelID, query string, reply func(string, ...any)) error {
	player, err := link.NewPlayer(guildID)
	if err != nil {
		return err
	}
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
	// A search matches many tracks; only a playlist link should queue them all.
	if result.LoadType == lavalink.LoadTypeSearch {
		tracks = tracks[:1]
	}

	if player.Playing() {
		player.Queue().Add(ctx, tracks...)
		reply("queued %d track(s), first up: %s", len(tracks), tracks[0].Info.Title)
		return nil
	}
	player.Queue().Add(ctx, tracks[1:]...)
	if err = player.Play(ctx, tracks[0]); err != nil {
		return err
	}
	reply("playing %s by %s", tracks[0].Info.Title, tracks[0].Info.Author)
	return nil
}
