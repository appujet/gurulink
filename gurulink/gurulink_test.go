package gurulink

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/appujet/gurulink/lavalink"
)

func testClient(t *testing.T, edit func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		UserID:          "1",
		SendVoiceUpdate: func(context.Context, string, *string, bool, bool) error { return nil },
	}
	if edit != nil {
		edit(&cfg)
	}
	client, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestIdentifier is the trust boundary: user queries turn into node identifiers
// here.
func TestIdentifier(t *testing.T) {
	client := testClient(t, nil)
	for _, tc := range []struct{ query, source, want string }{
		{"never gonna", "", "ytmsearch:never gonna"},
		{"  never gonna  ", "", "ytmsearch:never gonna"},
		{"never gonna", "spotify", "spsearch:never gonna"},
		{"never gonna", "YouTube", "ytsearch:never gonna"},
		{"ytsearch:never gonna", "", "ytsearch:never gonna"},
		{"ytsearch:never gonna", "spotify", "ytsearch:never gonna"},
		{"https://youtu.be/x", "", "https://youtu.be/x"},
		{"https://youtu.be/x", "spotify", "https://youtu.be/x"},
	} {
		got, err := client.identifier(tc.query, tc.source)
		if err != nil {
			t.Fatalf("%q/%q: %v", tc.query, tc.source, err)
		}
		if got != tc.want {
			t.Errorf("%q/%q: got %q, want %q", tc.query, tc.source, got, tc.want)
		}
	}
	if _, err := client.identifier("", ""); !errors.Is(err, ErrEmptyQuery) {
		t.Errorf("empty query: %v", err)
	}
	if _, err := client.identifier("x", "myspace"); err == nil {
		t.Error("an unknown source should fail")
	}
	long := make([]byte, 101)
	for i := range long {
		long[i] = 'a'
	}
	if _, err := client.identifier(string(long), "speak"); !errors.Is(err, ErrSpeakQueryTooLong) {
		t.Errorf("long speak query: %v", err)
	}
}

// TestVoiceStateEvents covers the voice-state fan-out: one Discord update turns
// into the channel, mute, deaf and suppress events, another user's update into
// join/leave, and our own leave must not destroy the player while a kick must.
func TestVoiceStateEvents(t *testing.T) {
	var seen []string
	client := testClient(t, func(c *Config) {
		c.Listeners = []Listener{func(e Event) { seen = append(seen, fmt.Sprintf("%T", e)) }}
	})
	// A player with a session-less node: every event fires, no request is sent.
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")
	client.players["g"] = player

	ctx := context.Background()
	send := func(u VoiceStateUpdate) {
		t.Helper()
		seen = nil
		if err := client.OnVoiceStateUpdate(ctx, u); err != nil {
			t.Fatal(err)
		}
	}
	want := func(events ...string) {
		t.Helper()
		if !slices.Equal(seen, events) {
			t.Errorf("got %v, want %v", seen, events)
		}
	}

	joined := VoiceStateUpdate{GuildID: "g", ChannelID: "c1", SessionID: "s", SelfDeaf: true}
	send(joined)
	want("*gurulink.PlayerChannelMoveEvent", "*gurulink.PlayerDeafChangeEvent")
	send(joined)
	want()

	moderated := joined
	moderated.ServerMute, moderated.Suppress = true, true
	send(moderated)
	want("*gurulink.PlayerMuteChangeEvent", "*gurulink.PlayerSuppressChangeEvent")

	send(VoiceStateUpdate{GuildID: "g", ChannelID: "c1", UserID: "2"})
	want("*gurulink.PlayerVoiceJoinEvent")
	send(VoiceStateUpdate{GuildID: "g", ChannelID: "c2", UserID: "2"})
	want("*gurulink.PlayerVoiceLeaveEvent")

	seen = nil
	if err := player.Disconnect(ctx); err != nil {
		t.Fatal(err)
	}
	want("*gurulink.PlayerDisconnectEvent")
	// Discord echoes the leave we asked for; the player has to survive it.
	send(VoiceStateUpdate{GuildID: "g"})
	want()
	if player.Destroyed() {
		t.Fatal("our own disconnect destroyed the player")
	}

	send(moderated)
	send(VoiceStateUpdate{GuildID: "g"}) // kicked: no request of ours came first
	want("*gurulink.PlayerDisconnectEvent", "*gurulink.PlayerDestroyEvent")
	if !player.Destroyed() {
		t.Error("a kick should destroy the player")
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("a client needs a user id")
	}
	if _, err := New(Config{UserID: "1"}); err == nil {
		t.Error("a client needs SendVoiceUpdate")
	}
}

// TestPlayerOverrides covers the fallback chain of the Kairo settings: a
// player's own beats the client-wide one, and clearing it goes back.
func TestPlayerOverrides(t *testing.T) {
	client := testClient(t, func(c *Config) {
		c.Crossfade = &lavalink.Crossfade{Enable: true}
		c.Tape = &lavalink.Tape{Enable: true}
	})
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")
	ctx := context.Background()
	taping := func() bool { tape := player.Tape(); return tape != nil && tape.Enable }

	if !player.crossfading() || !taping() {
		t.Fatal("the client-wide settings should apply")
	}

	// The node has no session, so the requests fail; the overrides are set anyway.
	_ = player.SetCrossfade(ctx, &lavalink.Crossfade{})
	_ = player.SetTape(ctx, &lavalink.Tape{})
	if player.crossfading() || taping() {
		t.Error("the player's own settings should win")
	}

	_ = player.SetCrossfade(ctx, nil)
	_ = player.SetTape(ctx, nil)
	if !player.crossfading() || !taping() {
		t.Error("clearing the overrides should fall back to the client")
	}
}
