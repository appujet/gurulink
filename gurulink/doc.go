// Package gurulink is a Lavalink v4 client: nodes, players, queues, filters and
// the Discord voice plumbing that glues them together.
//
//	go get github.com/appujet/gurulink
//	import "github.com/appujet/gurulink/gurulink"
//
// A [Client] owns the nodes and one [Player] per guild. It needs exactly one
// thing from your Discord library, a way to send a voice state update:
//
//	client, err := gurulink.New(gurulink.Config{
//		UserID: "1234",
//		SendVoiceUpdate: func(ctx context.Context, guildID string, channelID *string, mute, deaf bool) error {
//			return shard.UpdateVoiceState(ctx, guildID, channelID, mute, deaf)
//		},
//	})
//	node, err := client.AddNode(ctx, gurulink.NodeConfig{Name: "main", Address: "localhost:2333", Password: "youshallnotpass"})
//
// Forward Discord's two voice events to [Client.OnVoiceStateUpdate] and
// [Client.OnVoiceServerUpdate], then play something:
//
//	player, err := client.NewPlayer(guildID)
//	err = player.Connect(ctx, channelID, false, true)
//	result, err := player.Search(ctx, "never gonna give you up", "ytmsearch")
//	err = player.Play(ctx, result.AllTracks()[0])
//
// Queued tracks start on their own as the ones before them end.
//
// # Packages
//
// This package holds the moving parts: the client and its nodes (client.go,
// config.go, node.go, node_rest.go), players (player.go, player_voice.go,
// player_events.go), events (events.go) and query rules (search.go). The rest
// is split off:
//
//   - [github.com/appujet/gurulink/lavalink] is the wire protocol, the types a
//     node sends and accepts. It imports only the standard library.
//   - [github.com/appujet/gurulink/queue] is the track list a player carries,
//     usable on its own.
//   - [github.com/appujet/gurulink/store] has ready-made queue stores.
package gurulink
