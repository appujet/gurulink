// A disgo bot that plays music through gurulink and logs every event the client
// can hand out. Set the environment and run it:
//
//	DISCORD_TOKEN=... GUILD_ID=... LAVALINK_ADDRESS=localhost:2333 LAVALINK_PASSWORD=youshallnotpass go run .
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/appujet/gurulink/gurulink"
	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/cache"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/snowflake/v2"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	token := env("DISCORD_TOKEN")
	guildID := snowflake.MustParse(env("GUILD_ID"))

	var link *gurulink.Client
	discordClient, err := disgo.New(token,
		bot.WithGatewayConfigOpts(gateway.WithIntents(gateway.IntentGuilds|gateway.IntentGuildVoiceStates)),
		// gurulink needs no cache; /play uses it to find the caller's channel.
		bot.WithCacheConfigOpts(cache.WithCaches(cache.FlagVoiceStates)),
		bot.WithEventListenerFunc(func(e *events.GuildVoiceStateUpdate) {
			update := gurulink.VoiceStateUpdate{
				GuildID:    e.VoiceState.GuildID.String(),
				UserID:     e.VoiceState.UserID.String(),
				SessionID:  e.VoiceState.SessionID,
				SelfMute:   e.VoiceState.SelfMute,
				SelfDeaf:   e.VoiceState.SelfDeaf,
				ServerMute: e.VoiceState.GuildMute,
				ServerDeaf: e.VoiceState.GuildDeaf,
				Suppress:   e.VoiceState.Suppress,
			}
			if e.VoiceState.ChannelID != nil {
				update.ChannelID = e.VoiceState.ChannelID.String()
			}
			if err := link.OnVoiceStateUpdate(context.TODO(), update); err != nil {
				slog.Error("voice state update", slog.Any("err", err))
			}
		}),
		bot.WithEventListenerFunc(func(e *events.VoiceServerUpdate) {
			if e.Endpoint == nil {
				return // Discord is moving us to another voice server; the next one is real.
			}
			if err := link.OnVoiceServerUpdate(context.TODO(), e.GuildID.String(), e.Token, *e.Endpoint); err != nil {
				slog.Error("voice server update", slog.Any("err", err))
			}
		}),
		bot.WithEventListenerFunc(func(e *events.ApplicationCommandInteractionCreate) {
			onCommand(link, e)
		}),
	)
	if err != nil {
		slog.Error("build discord client", slog.Any("err", err))
		return
	}
	defer discordClient.Close(context.TODO())

	link, err = gurulink.New(gurulink.Config{
		// A bot's user id is its application id, and that one is in the token, so
		// this works before the gateway is even open.
		UserID: discordClient.ApplicationID.String(),
		SendVoiceUpdate: func(ctx context.Context, guildID string, channelID *string, selfMute, selfDeaf bool) error {
			var channel *snowflake.ID
			if channelID != nil {
				id := snowflake.MustParse(*channelID)
				channel = &id
			}
			return discordClient.UpdateVoiceState(ctx, snowflake.MustParse(guildID), channel, selfMute, selfDeaf)
		},
		Listeners:         listeners(),
		EmptyQueueTimeout: 30 * time.Second,
		Resuming:          time.Minute,
		Heartbeat:         30 * time.Second,
	})
	if err != nil {
		slog.Error("build gurulink client", slog.Any("err", err))
		return
	}
	defer link.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err = discordClient.Rest.SetGuildCommands(discordClient.ApplicationID, guildID, commands); err != nil {
		slog.Error("register commands", slog.Any("err", err))
		return
	}
	if err = discordClient.OpenGateway(ctx); err != nil {
		slog.Error("open gateway", slog.Any("err", err))
		return
	}
	if _, err = link.AddNode(ctx, gurulink.NodeConfig{
		Name:     "main",
		Address:  env("LAVALINK_ADDRESS"),
		Password: env("LAVALINK_PASSWORD"),
	}); err != nil {
		slog.Error("add node", slog.Any("err", err))
		return
	}

	slog.Info("running, ctrl-c to stop")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}

var commands = []discord.ApplicationCommandCreate{
	discord.SlashCommandCreate{
		Name:        "play",
		Description: "play a track in your voice channel",
		Options: []discord.ApplicationCommandOption{
			discord.ApplicationCommandOptionString{
				Name:        "query",
				Description: "a search term or a link",
				Required:    true,
			},
		},
	},
	discord.SlashCommandCreate{Name: "skip", Description: "skip the current track"},
	discord.SlashCommandCreate{Name: "stop", Description: "stop and leave the channel"},
}

func env(key string) string {
	value := os.Getenv(key)
	if value == "" {
		slog.Error("missing environment variable", slog.String("key", key))
		os.Exit(1)
	}
	return value
}
