package gurulink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/appujet/gurulink/lavalink"
	"github.com/gorilla/websocket"
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

// testNode gives a client a node whose REST API is an httptest server, so player
// commands land in bodies.
func testNode(t *testing.T, client *Client, bodies chan<- []byte) *Node {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		bodies <- body
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"guildId":"g","volume":100}`)
	}))
	t.Cleanup(server.Close)
	return &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger(), rest: server.URL, sessionID: "s"}
}

// TestIdentifier is the trust boundary: user queries become node identifiers here.
func TestIdentifier(t *testing.T) {
	client := testClient(t, nil)
	for _, tc := range []struct{ query, source, want string }{
		{"never gonna", "", "ytmsearch:never gonna"},
		{"  never gonna  ", "", "ytmsearch:never gonna"},
		{"never gonna", "spotify", "spsearch:never gonna"},
		{"never gonna", "YouTube", "ytsearch:never gonna"},
		{"ytsearch:never gonna", "", "ytsearch:never gonna"},
		{"ytsearch:never gonna", "spotify", "ytsearch:never gonna"},
		{"yt:never gonna", "", "ytsearch:never gonna"},
		{"yt:  never gonna", "spotify", "ytsearch:never gonna"},
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
	if _, err := client.identifier("speak:"+string(long), ""); !errors.Is(err, ErrSpeakQueryTooLong) {
		t.Errorf("long speak query naming its own source: %v", err)
	}
}

// TestVoiceStateEvents covers the voice-state fan-out, and that our own leave
// keeps the player while a kick destroys it.
func TestVoiceStateEvents(t *testing.T) {
	var seen []string
	client := testClient(t, func(c *Config) {
		c.Listeners = []Listener{func(e Event) { seen = append(seen, fmt.Sprintf("%T", e)) }}
	})
	// A session-less node: every event fires, no request is sent.
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

// TestVoiceMoveKeepsSession replays a channel move: the voice state carries a
// new channel but the same session, so it must not patch the node — the token
// it would send is stale, and Lavalink answers that patch with a 500/4006.
// Only the fresh voice server, with its new token, patches.
func TestVoiceMoveKeepsSession(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 4)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	client.players["g"] = player

	ctx := context.Background()
	// Initial join: state alone patches nothing, the server completes it.
	if err := client.OnVoiceStateUpdate(ctx, VoiceStateUpdate{GuildID: "g", ChannelID: "c1", SessionID: "s1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-bodies:
		t.Fatalf("the bare voice state must not patch the node, got %s", body)
	default:
	}
	if err := client.OnVoiceServerUpdate(ctx, "g", "t1", "e1"); err != nil {
		t.Fatal(err)
	}
	body := string(<-bodies)
	for _, want := range []string{`"token":"t1"`, `"endpoint":"e1"`, `"sessionId":"s1"`, `"channelId":"c1"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("the join patch %s is missing %s", body, want)
		}
	}

	// The move: same session, new channel. No patch until the new server.
	if err := client.OnVoiceStateUpdate(ctx, VoiceStateUpdate{GuildID: "g", ChannelID: "c2", SessionID: "s1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-bodies:
		t.Fatalf("a channel move must wait for the fresh voice server, got %s", body)
	default:
	}
	if got := player.ChannelID(); got != "c2" {
		t.Fatalf("the player should track the new channel, got %q", got)
	}

	if err := client.OnVoiceServerUpdate(ctx, "g", "t2", "e1"); err != nil {
		t.Fatal(err)
	}
	body = string(<-bodies)
	for _, want := range []string{`"token":"t2"`, `"sessionId":"s1"`, `"channelId":"c2"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("the move patch %s is missing %s", body, want)
		}
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Error("a client needs a user id")
	}
	if _, err := New(Config{UserID: "1"}); err == nil {
		t.Error("a client needs SendVoiceUpdate")
	}
	send := func(context.Context, string, *string, bool, bool) error { return nil }
	if _, err := New(Config{UserID: "1", SendVoiceUpdate: send, DefaultSource: "myspace"}); err == nil {
		t.Error("an unknown default source should fail here, not on every search")
	}
	client, err := New(Config{UserID: "1", SendVoiceUpdate: send, DefaultSource: "youtube"})
	if err != nil || client.Config().DefaultSource != "ytsearch" {
		t.Errorf("an alias should resolve to its prefix: %v", err)
	}
}

// TestPlayerOverrides covers the Kairo fallback chain: a player's own setting
// beats the client-wide one, and clearing it goes back.
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

// TestSkipCrossfade covers the manual skip: with crossfade on the request asks
// for a transition and the queue waits for TrackEndEvent (crossfade), like
// lavalink-client — not for TrackPromotedEvent, which is state-sync only.
func TestSkipCrossfade(t *testing.T) {
	client := testClient(t, func(c *Config) { c.Crossfade = &lavalink.Crossfade{Enable: true} })
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "playing"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "next"})
	// Drain the debounced PreBuffer from the Add above: the Skip below must be
	// the next body read, not the sync.
	drain := func() {
		for {
			select {
			case <-bodies:
			default:
				return
			}
		}
	}
	// The Add's PreBuffer fires 50ms later; wait it out so the channel is
	// empty before the Skip.
	select {
	case <-bodies:
		// A PreBuffer slipped in early; keep draining.
		drain()
	case <-time.After(100 * time.Millisecond):
		drain()
	}

	if err := player.Skip(ctx); err != nil {
		t.Fatal(err)
	}
	body := string(<-bodies)
	for _, want := range []string{`"transition":true`, `"nextTrack":{"encoded":"next"}`} {
		if !strings.Contains(body, want) {
			t.Errorf("the skip request %s is missing %s", body, want)
		}
	}
	if strings.Contains(body, `"track":`) {
		t.Errorf("the skip request %s replaces the track instead of fading into it", body)
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "playing" {
		t.Errorf("the queue moved before the node ended the track: %v", current)
	}

	// The node ends the outgoing track with reason crossfade: the queue must
	// advance to the successor here, so Now Playing matches what is audible.
	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "playing"}, Reason: lavalink.ReasonCrossfade})
	if current := player.queue.Current(); current == nil || current.Encoded != "next" {
		t.Errorf("the queue did not advance on the crossfade end: %v", current)
	}
	if n := player.queue.Len(); n != 0 {
		t.Errorf("the queue still holds %d tracks after advancing", n)
	}

	// The following promotion is state-sync only and must not advance again.
	player.handle(ctx, &TrackPromotedEvent{Player: player, Track: lavalink.Track{Encoded: "next"}})
	if current := player.queue.Current(); current == nil || current.Encoded != "next" {
		t.Errorf("the promotion moved the queue a second time: %v", current)
	}
	if n := player.queue.Len(); n != 0 {
		t.Errorf("the promotion duplicated the queue: %d tracks", n)
	}

	// Crossfade off: the same skip has to replace the track itself.
	if err := player.SetCrossfade(ctx, &lavalink.Crossfade{}); err != nil {
		t.Fatal(err)
	}
	<-bodies // SetCrossfade's own request.
	player.queue.Add(ctx, lavalink.Track{Encoded: "after"})
	drain()
	// Wait out the Add's debounced sync before the next Skip.
	select {
	case <-bodies:
		drain()
	case <-time.After(100 * time.Millisecond):
		drain()
	}
	if err := player.Skip(ctx); err != nil {
		t.Fatal(err)
	}
	if body := string(<-bodies); !strings.Contains(body, `"track":{"encoded":"after"}`) {
		t.Errorf("the skip request %s does not play the next track", body)
	}
}

// TestTransitionEndAdvances covers the natural crossfade: a track ending with
// reason crossfade/gapless must retire the outgoing track and make the head
// current, without a play request — the node is already playing the successor.
func TestTransitionEndAdvances(t *testing.T) {
	client := testClient(t, func(c *Config) { c.Crossfade = &lavalink.Crossfade{Enable: true} })
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"}, lavalink.Track{Encoded: "three"})

	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonGapless})
	if current := player.queue.Current(); current == nil || current.Encoded != "two" {
		t.Fatalf("the queue did not advance on the gapless end: %v", current)
	}
	if got := player.queue.Tracks(); len(got) != 1 || got[0].Encoded != "three" {
		t.Fatalf("the waiting list is wrong after the transition: %v", got)
	}
	if prev := player.queue.Previous(); len(prev) != 1 || prev[0].Encoded != "one" {
		t.Fatalf("the outgoing track was not retired to history: %v", prev)
	}
}

// TestTransitionEndRepeatTrack covers looping one track through a crossfade:
// without an explicit skip the current track must stay current, not advance
// into the queue (which would break the loop and show the wrong Now Playing).
func TestTransitionEndRepeatTrack(t *testing.T) {
	client := testClient(t, func(c *Config) { c.Crossfade = &lavalink.Crossfade{Enable: true} })
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	player.SetRepeat(RepeatTrack)

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "loop"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "other"})

	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "loop"}, Reason: lavalink.ReasonCrossfade})
	if current := player.queue.Current(); current == nil || current.Encoded != "loop" {
		t.Fatalf("a looped track advanced on its crossfade end: %v", current)
	}
	if n := player.queue.Len(); n != 1 {
		t.Fatalf("a looped track dropped the queue: %d tracks", n)
	}

	// An explicit skip still leaves the loop, like lavalink-client's
	// internal_manualSkipPending.
	if err := player.Skip(ctx); err != nil {
		t.Fatal(err)
	}
	<-bodies // Skip's transition request.
	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "loop"}, Reason: lavalink.ReasonCrossfade})
	if current := player.queue.Current(); current == nil || current.Encoded != "other" {
		t.Fatalf("an explicit skip did not leave the loop: %v", current)
	}
}

// TestPromotedMismatchDropsDuplicate covers the queue moving under the
// pre-buffer: an AddNext between PreBuffer and promotion must not leave the
// promoted track duplicated in the waiting list to replay later.
func TestPromotedMismatchDropsDuplicate(t *testing.T) {
	client := testClient(t, func(c *Config) { c.Crossfade = &lavalink.Crossfade{Enable: true} })
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"}, lavalink.Track{Encoded: "three"})
	// The head moves after the node pre-buffered "two".
	player.queue.AddNext(ctx, lavalink.Track{Encoded: "zero"})

	player.handle(ctx, &TrackPromotedEvent{Player: player, Track: lavalink.Track{Encoded: "two"}})
	if current := player.queue.Current(); current == nil || current.Encoded != "two" {
		t.Fatalf("the promotion did not take the node's word: %v", current)
	}
	for _, got := range player.queue.Tracks() {
		if got.Encoded == "two" {
			t.Fatalf("the promoted track was left in the queue to replay: %v", player.queue.Tracks())
		}
	}
	if got := player.queue.Tracks(); len(got) != 2 || got[0].Encoded != "zero" || got[1].Encoded != "three" {
		t.Fatalf("the waiting list is wrong after the promotion: %v", got)
	}
}

// TestNodeCloseDeadline pins the shutdown budget: in time the node says goodbye,
// out of time it just hangs up.
func TestNodeCloseDeadline(t *testing.T) {
	for _, tc := range []struct {
		name    string
		expired bool
		want    int
	}{
		{"in time", false, websocket.CloseNormalClosure},
		{"out of time", true, websocket.CloseAbnormalClosure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codes := make(chan int, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					codes <- 0
					return
				}
				defer conn.Close()
				_, _, err = conn.ReadMessage()
				code := websocket.CloseAbnormalClosure
				var closeErr *websocket.CloseError
				if errors.As(err, &closeErr) {
					code = closeErr.Code
				}
				codes <- code
			}))
			defer server.Close()

			ctx := context.Background()
			client := testClient(t, nil)
			node, err := client.AddNode(ctx, NodeConfig{Name: "test", Address: strings.TrimPrefix(server.URL, "http://")})
			if err != nil {
				t.Fatal(err)
			}

			closeCtx := ctx
			if tc.expired {
				expired, cancel := context.WithCancel(ctx)
				cancel()
				closeCtx = expired
			}
			node.Close(closeCtx)
			if got := <-codes; got != tc.want {
				t.Errorf("the node closed with code %d, want %d", got, tc.want)
			}
		})
	}
}
