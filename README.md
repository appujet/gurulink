# gurulink

A [Kairo](https://github.com/bongo-devs/Kairo) client for Go: nodes, players, queues,
filters and the Discord voice plumbing that glues them together.

```
go get github.com/appujet/gurulink
```

The module root holds no Go files; every package is a folder under it, so the
client itself is imported as `github.com/appujet/gurulink/gurulink`.

One dependency ([gorilla/websocket](https://github.com/gorilla/websocket)), no
framework. Bring any Discord library: gurulink needs exactly one function from
it, a way to send a voice state update.

## Quickstart

```go
client, err := gurulink.New(gurulink.Config{
	UserID: "1234",
	SendVoiceUpdate: func(ctx context.Context, guildID string, channelID *string, mute, deaf bool) error {
		return shard.UpdateVoiceState(ctx, guildID, channelID, mute, deaf)
	},
	Listeners: []gurulink.Listener{
		gurulink.On(func(e *gurulink.TrackStartEvent) {
			log.Println("now playing", e.Track.Info.Title)
		}),
	},
})

node, err := client.AddNode(ctx, gurulink.NodeConfig{
	Name: "main", Address: "localhost:2333", Password: "youshallnotpass",
})

// Forward Discord's voice events.
client.OnVoiceStateUpdate(ctx, gurulink.VoiceStateUpdate{
	GuildID: guildID, ChannelID: channelID, UserID: userID, SessionID: sessionID,
})
client.OnVoiceServerUpdate(ctx, guildID, token, endpoint)

player, err := client.NewPlayer(guildID)
err = player.Connect(ctx, channelID, false, true)

result, err := player.Search(ctx, "never gonna give you up", "ytmsearch")
err = player.Play(ctx, result.AllTracks()[0])
player.Queue().Add(result.AllTracks()[1:]...)
```

Queued tracks start on their own as the ones before them end.

## What it does

- **Nodes** — websocket + REST, session resuming, heartbeat, backoff reconnect,
  load-based node picking, voice-region affinity, live player migration.
- **Players** — play, pause, seek, volume, repeat, skip, skip-to, back, autoplay,
  idle timeout, consecutive-failure cutoff, voice-close recovery.
- **Queue** — history, insert, move, swap, filter, find, shuffle, and optional
  persistence through a `queue.Store`.
- **Filters** — the ten stock filters, the plugin ones (LavaDSPX,
  lavalink-filter-plugin), and presets: `Nightcore`, `Vaporwave`, `EQBassboost*`,
  `Output*`.
- **Search** — ~70 source shorthands (`spotify`, `yt`, `dz`, `tidal`, …).
- **Plugins** — SponsorBlock, LavaSearch, LavaLyrics, chapters, route planner.
- **Kairo extras** — crossfade with next-track pre-buffering and tape pitch
  ramps.

Every event is delivered to `Listener`s; `gurulink.On[E]` adapts a typed handler.
There are 38 of them: the node lifecycle (connect, ready, stats, disconnect,
reconnect, resumed, removed), playback (track start/promoted/end/exception/stuck,
queue end, idle start/cancel), the voice connection (channel move, node move,
disconnect, reconnect, socket closed, mute/deaf/suppress changes, other users
joining and leaving), the plugins (segments, chapters, lyrics), and
create/destroy/error/unknown.

## Packages

All four sit under `github.com/appujet/gurulink/`:

| Package    | What lives there                                             |
| ---------- | ------------------------------------------------------------ |
| `gurulink` | `Client`, `Node`, `Player`, events, query rules              |
| `lavalink` | the wire protocol: tracks, filters, player state, node stats |
| `queue`    | `Queue`, `Store`, and the change callback                    |
| `store`    | ready-made `Store`s: `Memory` and `File`                     |

`lavalink` imports only the standard library, `queue` imports `lavalink`, and
`store` imports neither — so a bot can speak the wire types, or persist a queue,
without dragging the client along:

```go
client, err := gurulink.New(gurulink.Config{
	QueueStore: store.File{Dir: "queues"},
	// …
})
```

## License

[Apache-2.0](LICENSE). [NOTICE](NOTICE) lists everything that came from
elsewhere and under which license, so a fork inherits no surprises:

- The equalizer tables, the Nightcore/Vaporwave values and the source-alias list
  are ported from [lavalink-client](https://github.com/bongo-devs/lavalink-client)
  (MIT, © 2022 Tomato6966) — data, not code; its notice is reproduced in full.
- [disgolink](https://github.com/disgoorg/disgolink) and
  [disgo](https://github.com/disgoorg/disgo) (both Apache-2.0) were read as
  references for the Go shape of a client. Nothing was copied from either.
- [Lavalink v4](https://lavalink.dev) and the
  [Kairo](https://github.com/bongo-devs/Kairo) fork are implemented from their
  documented protocols.
- [gorilla/websocket](https://github.com/gorilla/websocket) (BSD-2-Clause) is
  the one dependency; its license travels inside the module.
