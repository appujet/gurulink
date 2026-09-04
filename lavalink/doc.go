// Package lavalink is the Lavalink v4 wire protocol: the types a node sends and
// accepts, and nothing else. It imports only the standard library, so a bot can
// speak these types without pulling in a client.
//
// Layout: [Duration], [Timestamp] and [Nullable] in json.go, tracks and load
// results in track.go, player state and [PlayerUpdate] in player.go, node stats
// and sessions in node.go, the filter chain and its presets in filters.go, the
// Kairo fork's extras in extras.go, and the gateway enums in op.go.
package lavalink
