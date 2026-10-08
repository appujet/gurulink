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
	"sync"
	"testing"
	"time"

	"github.com/appujet/gurulink/lavalink"
	"github.com/appujet/gurulink/queue"
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

	// Crossfade off: the same skip stops the track instead of fading, and the
	// queue waits for the TrackEnd that stop produces. Advancing here as well
	// would give the queue two writers, which is what ate a track when a skip
	// landed on a track that was ending anyway.
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
	body = string(<-bodies)
	if !strings.Contains(body, `"track":{"encoded":null}`) {
		t.Errorf("the skip request %s does not stop the track", body)
	}
	if !strings.Contains(body, `"paused":false`) {
		t.Errorf("the skip request %s does not unpause, so a paused player would stay silent", body)
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "next" {
		t.Errorf("the skip advanced the queue itself instead of waiting for the end: %v", current)
	}
	// The stop the skip asked for comes back as TrackEnd (stopped): that is where
	// the queue moves, and the skipped flag is what tells it this was deliberate.
	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "next"}, Reason: lavalink.ReasonStopped})
	if body := string(<-bodies); !strings.Contains(body, `"track":{"encoded":"after"}`) {
		t.Errorf("the end of a skipped track %s does not play the next one", body)
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "after" {
		t.Errorf("the queue did not advance on the skipped track's end: %v", current)
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

// TestSkipWaitsForEnd is the single-writer rule: a skip stops the track and the
// queue moves on the TrackEnd that stop produces, never in the command. A skip
// that advanced locally would race the end of the track it is skipping and the
// two together would consume two tracks for one press.
func TestSkipWaitsForEnd(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"}, lavalink.Track{Encoded: "three"})

	if err := player.Skip(ctx); err != nil {
		t.Fatal(err)
	}
	if body := string(<-bodies); !strings.Contains(body, `"track":{"encoded":null}`) {
		t.Errorf("the skip request %s does not stop the track", body)
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "one" {
		t.Fatalf("the skip advanced the queue instead of waiting for the end: %v", current)
	}

	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonStopped})
	if body := string(<-bodies); !strings.Contains(body, `"track":{"encoded":"two"}`) {
		t.Errorf("the skipped track's end %s does not play the next one", body)
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "two" {
		t.Fatalf("the queue did not advance on the skipped track's end: %v", current)
	}

	// The natural end of the track that was skipped arrives late. It names a
	// track that is no longer current, so it must do nothing: advancing here is
	// what used to eat "three".
	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonFinished})
	select {
	case body := <-bodies:
		t.Errorf("a stale end sent a request: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "two" {
		t.Errorf("a stale end moved the queue: %v", current)
	}
	if got := player.queue.Tracks(); len(got) != 1 || got[0].Encoded != "three" {
		t.Errorf("a stale end consumed a queued track: %v", got)
	}
}

// TestStopDoesNotAdvance pins the other half of the stopped reason: a stop and a
// skip look identical on the wire, so the flag the command leaves behind is the
// only thing that tells them apart.
func TestStopDoesNotAdvance(t *testing.T) {
	// Events reach listeners once the command lock is free, so wait for the
	// event rather than reading a counter the listener writes from elsewhere.
	ended := make(chan *QueueEndEvent, 4)
	client := testClient(t, func(c *Config) {
		c.Listeners = []Listener{On(func(e *QueueEndEvent) { ended <- e })}
	})
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"})

	if err := player.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	<-bodies // Stop's own null-track request.

	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonStopped})
	if body := string(<-bodies); !strings.Contains(body, `"track":{"encoded":null}`) {
		t.Errorf("a stopped player played something: %s", body)
	}
	select {
	case e := <-ended:
		if e.Reason != lavalink.ReasonStopped {
			t.Errorf("the queue ended with reason %q, want stopped", e.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the queue end never fired")
	}
	if current := player.queue.Current(); current != nil {
		t.Errorf("a stopped player still has a current track: %v", current)
	}
	select {
	case <-ended:
		t.Error("the queue end fired more than once")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestBareStopIgnored covers a stop nobody asked for: with no flag set the queue
// must stay put rather than treat it as a skip.
func TestBareStopIgnored(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"})

	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonStopped})
	select {
	case body := <-bodies:
		t.Errorf("an unasked-for stop sent a request: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "one" {
		t.Errorf("an unasked-for stop moved the queue: %v", current)
	}
}

// TestSkipKeepsRepeatQueue covers the re-add living in the one advance primitive
// rather than in a single caller: a skipped track has to stay in the rotation,
// exactly like one that finished.
func TestSkipKeepsRepeatQueue(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	player.SetRepeat(RepeatQueue)

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"})

	if err := player.Skip(ctx); err != nil {
		t.Fatal(err)
	}
	<-bodies
	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonStopped})
	<-bodies

	if current := player.queue.Current(); current == nil || current.Encoded != "two" {
		t.Fatalf("the skip did not advance: %v", current)
	}
	if got := player.queue.Tracks(); len(got) != 1 || got[0].Encoded != "one" {
		t.Errorf("the skipped track fell out of the repeat-queue rotation: %v", got)
	}
}

// TestSkipLeavesRepeatTrack covers the skip flag beating the loop, the
// non-crossfade twin of TestTransitionEndRepeatTrack.
func TestSkipLeavesRepeatTrack(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	player.SetRepeat(RepeatTrack)

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "loop"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "other"})

	// A natural end loops.
	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "loop"}, Reason: lavalink.ReasonFinished})
	<-bodies
	if current := player.queue.Current(); current == nil || current.Encoded != "loop" {
		t.Fatalf("a natural end broke the loop: %v", current)
	}

	// An explicit skip leaves it.
	if err := player.Skip(ctx); err != nil {
		t.Fatal(err)
	}
	<-bodies
	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "loop"}, Reason: lavalink.ReasonStopped})
	<-bodies
	if current := player.queue.Current(); current == nil || current.Encoded != "other" {
		t.Errorf("an explicit skip did not leave the loop: %v", current)
	}
}

// TestStaleStateIgnored is the seek bug: the websocket and REST carry no
// ordering between them, so a playerUpdate the node stamped before a seek can be
// read after the seek's reply. Taking it would roll the position back and the
// seek would look ignored even though the audio moved.
func TestStaleStateIgnored(t *testing.T) {
	client := testClient(t, nil)
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")

	now := time.Now()
	fresh := lavalink.PlayerState{
		Time:     lavalink.Timestamp{Time: now},
		Position: 60 * lavalink.Second,
	}
	if !player.setState(fresh) {
		t.Fatal("the first frame should apply")
	}

	stale := lavalink.PlayerState{
		Time:     lavalink.Timestamp{Time: now.Add(-2 * time.Second)},
		Position: 5 * lavalink.Second,
	}
	if player.setState(stale) {
		t.Error("a frame the node stamped earlier should not apply")
	}
	if got := player.State().Position; got != 60*lavalink.Second {
		t.Errorf("a stale frame moved the position to %s, want 1m0s", got)
	}

	// A later frame still gets through, so the gate does not freeze the clock.
	newer := lavalink.PlayerState{
		Time:     lavalink.Timestamp{Time: now.Add(2 * time.Second)},
		Position: 62 * lavalink.Second,
	}
	if !player.setState(newer) {
		t.Error("a newer frame should apply")
	}
	if got := player.State().Position; got != 62*lavalink.Second {
		t.Errorf("the position is %s after a newer frame, want 1m2s", got)
	}

	// A node that sends no timestamp gets the benefit of the doubt rather than
	// having every frame rejected.
	if !player.setState(lavalink.PlayerState{Position: 7 * lavalink.Second}) {
		t.Error("an unstamped frame should apply")
	}
}

// TestSeekMovesPositionBeforeReply covers lavalink-client writing lastPosition
// before its request: without it Position() keeps interpolating the old timeline
// for a whole round trip.
func TestSeekMovesPositionBeforeReply(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 8)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{
		Encoded: "one",
		Info:    lavalink.TrackInfo{Length: 5 * lavalink.Minute, IsSeekable: true},
	})

	if err := player.Seek(ctx, 90*lavalink.Second); err != nil {
		t.Fatal(err)
	}
	if body := string(<-bodies); !strings.Contains(body, `"position":90000`) {
		t.Errorf("the seek request %s does not carry the position", body)
	}
	if got := player.State().Position; got != 90*lavalink.Second {
		t.Errorf("the local position is %s after a seek, want 1m30s", got)
	}

	// And a frame from before the seek cannot undo it.
	player.setState(lavalink.PlayerState{
		Time:     lavalink.Timestamp{Time: time.Now().Add(-time.Second)},
		Position: 3 * lavalink.Second,
	})
	if got := player.Position(); got < 90*lavalink.Second {
		t.Errorf("a frame from before the seek rolled the position back to %s", got)
	}
}

// TestSeekGuards covers the three checks lavalink-client makes before it sends
// anything, so an impossible seek never becomes a REST error.
func TestSeekGuards(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 8)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	ctx := context.Background()

	// Nothing playing: a no-op, not a request.
	if err := player.Seek(ctx, lavalink.Second); err != nil {
		t.Errorf("seeking with nothing playing: %v", err)
	}
	select {
	case body := <-bodies:
		t.Errorf("seeking with nothing playing sent %s", body)
	case <-time.After(50 * time.Millisecond):
	}

	player.queue.SetCurrent(ctx, &lavalink.Track{
		Encoded: "live",
		Info:    lavalink.TrackInfo{IsStream: true},
	})
	if err := player.Seek(ctx, lavalink.Second); !errors.Is(err, ErrNotSeekable) {
		t.Errorf("seeking a stream: %v, want ErrNotSeekable", err)
	}

	player.queue.SetCurrent(ctx, &lavalink.Track{
		Encoded: "fixed",
		Info:    lavalink.TrackInfo{Length: 10 * lavalink.Second, IsSeekable: false},
	})
	if err := player.Seek(ctx, lavalink.Second); !errors.Is(err, ErrNotSeekable) {
		t.Errorf("seeking an unseekable track: %v, want ErrNotSeekable", err)
	}

	// Past the end clamps to the end; below zero clamps to zero.
	player.queue.SetCurrent(ctx, &lavalink.Track{
		Encoded: "ok",
		Info:    lavalink.TrackInfo{Length: 10 * lavalink.Second, IsSeekable: true},
	})
	if err := player.Seek(ctx, lavalink.Minute); err != nil {
		t.Fatal(err)
	}
	if body := string(<-bodies); !strings.Contains(body, `"position":10000`) {
		t.Errorf("a seek past the end sent %s, want it clamped to the length", body)
	}
	if err := player.Seek(ctx, -lavalink.Minute); err != nil {
		t.Fatal(err)
	}
	if body := string(<-bodies); !strings.Contains(body, `"position":0`) {
		t.Errorf("a negative seek sent %s, want it clamped to zero", body)
	}
}

// TestPromotionRestartsPosition covers the timeline following the audio across a
// crossfade: without it Position() reports the outgoing track's elapsed time
// against the incoming one and a Now Playing progress bar is simply wrong.
func TestPromotionRestartsPosition(t *testing.T) {
	client := testClient(t, func(c *Config) { c.Crossfade = &lavalink.Crossfade{Enable: true} })
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"})
	player.setState(lavalink.PlayerState{
		Time:     lavalink.Timestamp{Time: time.Now()},
		Position: 3 * lavalink.Minute,
	})

	player.handle(ctx, &TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonCrossfade})
	// Back to the start of the successor, and still running: the node keeps the
	// successor active through a transition end, so audio really is advancing.
	if got := player.Position(); got > lavalink.Second {
		t.Errorf("the position is %s after a crossfade handoff, want it near 0", got)
	}

	player.handle(ctx, &TrackPromotedEvent{Player: player, Track: lavalink.Track{Encoded: "two"}})
	if got := player.Position(); got > lavalink.Second {
		t.Errorf("the position is %s after a promotion, want it near 0", got)
	}
}

// TestPromotionKeepsDoubleQueued covers the de-duplication only firing when the
// node is playing something unexpected: the same track queued twice is a normal
// thing to do, and the second copy must survive the first one's promotion.
func TestPromotionKeepsDoubleQueued(t *testing.T) {
	client := testClient(t, func(c *Config) { c.Crossfade = &lavalink.Crossfade{Enable: true} })
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "same"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "same"}, lavalink.Track{Encoded: "other"})

	player.handle(ctx, &TrackPromotedEvent{Player: player, Track: lavalink.Track{Encoded: "same"}})
	if got := player.queue.Tracks(); len(got) != 2 || got[0].Encoded != "same" {
		t.Errorf("the promotion ate the queued copy of the playing track: %v", got)
	}
}

// TestTrackStartAdoptsNodeTrack is a deliberate step past lavalink-client, which
// only fills Current when it is empty. A Go player is driven from several
// goroutines, so the node's word has to be able to correct a divergence —
// otherwise the status names one track while another is audible, forever.
func TestTrackStartAdoptsNodeTrack(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "stale"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "real"}, lavalink.Track{Encoded: "later"})

	player.handle(ctx, &TrackStartEvent{Player: player, Track: lavalink.Track{Encoded: "real"}})
	if current := player.queue.Current(); current == nil || current.Encoded != "real" {
		t.Fatalf("the node's word did not correct the current track: %v", current)
	}
	for _, got := range player.queue.Tracks() {
		if got.Encoded == "real" {
			t.Errorf("the started track was left queued to replay: %v", player.queue.Tracks())
		}
	}
}

// TestNodeChangingSilencesTrackEvents covers the gate around a node move: the
// old node's events describe a player that is about to be destroyed, and letting
// one advance the queue drives it against the new node mid-rebuild.
func TestNodeChangingSilencesTrackEvents(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"})

	player.mu.Lock()
	player.nodeChanging = true
	player.mu.Unlock()

	for _, event := range []Event{
		&TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonFinished},
		&TrackStartEvent{Player: player, Track: lavalink.Track{Encoded: "elsewhere"}},
		&TrackPromotedEvent{Player: player, Track: lavalink.Track{Encoded: "elsewhere"}},
		&TrackStuckEvent{Player: player, Track: lavalink.Track{Encoded: "one"}},
	} {
		player.handle(ctx, event)
	}
	select {
	case body := <-bodies:
		t.Errorf("an event during a node move sent a request: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "one" {
		t.Errorf("an event during a node move moved the queue: %v", current)
	}
	if n := player.queue.Len(); n != 1 {
		t.Errorf("an event during a node move consumed the queue: %d tracks", n)
	}
}

// TestTrackErrorWindow covers failures expiring: a long-lived player that hits
// scattered errors over hours must not be torn down as if it had a burst.
func TestTrackErrorWindow(t *testing.T) {
	client := testClient(t, func(c *Config) {
		c.MaxTrackErrors = 3
		c.TrackErrorWindow = 50 * time.Millisecond
	})
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")

	if player.trackFailed() || player.trackFailed() {
		t.Fatal("two failures should be under the limit")
	}
	// Let them age out, then two more must still be under the limit.
	time.Sleep(80 * time.Millisecond)
	if player.trackFailed() || player.trackFailed() {
		t.Fatal("failures outside the window should be forgotten")
	}
	// A third inside the window trips it.
	if !player.trackFailed() {
		t.Error("three failures inside the window should give up")
	}
}

// TestTrackStartClearsErrorWindow covers a clean start wiping the count, so a
// recovered player is not destroyed by old failures.
func TestTrackStartClearsErrorWindow(t *testing.T) {
	client := testClient(t, func(c *Config) { c.MaxTrackErrors = 2 })
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	if player.trackFailed() {
		t.Fatal("one failure should be under the limit")
	}
	player.handle(context.Background(), &TrackStartEvent{Player: player, Track: lavalink.Track{Encoded: "one"}})
	if player.trackFailed() {
		t.Error("a clean start should have cleared the failure count")
	}
}

// TestConcurrentSkipAndEnd is the race the command lock and the staleness guard
// exist for: a user skip landing at the same moment the track ends. Both used to
// advance the queue, so one press consumed two tracks — the extra one went
// straight to the history without ever being audible. The invariant is one
// advance per end, however the two interleave. Run under -race.
func TestConcurrentSkipAndEnd(t *testing.T) {
	const queued, rounds = 40, 20

	client := testClient(t, nil)
	bodies := make(chan []byte, 512)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	go func() {
		for range bodies {
		}
	}()

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "t0"})
	for i := 1; i <= queued; i++ {
		player.queue.Add(ctx, lavalink.Track{Encoded: fmt.Sprintf("t%d", i)})
	}

	for round := 0; round < rounds; round++ {
		before := player.queue.Current()
		if before == nil {
			t.Fatalf("round %d: the player lost its current track", round)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = player.Skip(ctx)
		}()
		go func() {
			defer wg.Done()
			player.handle(ctx, &TrackEndEvent{Player: player, Track: *before, Reason: lavalink.ReasonFinished})
		}()
		wg.Wait()

		// The advance runs off the event loop, so wait for it to land rather
		// than guessing. Anything still pending after this would show up as a
		// miscount below.
		for waited := 0; waited < 200; waited++ {
			if current := player.queue.Current(); current == nil || current.Encoded != before.Encoded {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// One track consumed per end. Two writers would have eaten them in pairs and
	// run the queue dry around round 20.
	if got := player.queue.Len(); got != queued-rounds {
		t.Errorf("%d tracks left after %d skips, want %d: the queue advanced more than once per end",
			got, rounds, queued-rounds)
	}
	if got := len(player.queue.Previous()); got != rounds {
		t.Errorf("%d tracks in the history after %d skips, want %d", got, rounds, rounds)
	}
	current := player.queue.Current()
	if current == nil {
		t.Fatal("the player lost its current track entirely")
	}
	if want := fmt.Sprintf("t%d", rounds); current.Encoded != want {
		t.Errorf("playing %s after %d skips, want %s", current.Encoded, rounds, want)
	}
	// Nothing may be both playing and still waiting.
	seen := map[string]bool{current.Encoded: true}
	for _, track := range player.queue.Tracks() {
		if seen[track.Encoded] {
			t.Fatalf("%s is both playing and queued after the race", track.Encoded)
		}
		seen[track.Encoded] = true
	}
}

// TestAutoplayCooldown covers the retry guard: a source that is down used to be
// asked again on every single track end. lavalink-client holds it off with
// internal_autoplay_failed_at for the same reason.
func TestAutoplayCooldown(t *testing.T) {
	var calls int
	var addTracks bool
	client := testClient(t, func(c *Config) {
		c.AutoplayCooldown = 60 * time.Millisecond
		c.Autoplay = func(ctx context.Context, p *Player) error {
			calls++
			if addTracks {
				p.Queue().Add(ctx, lavalink.Track{Encoded: "found"})
			}
			return nil
		}
	})
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")
	ctx := context.Background()

	if player.autoplay(ctx) {
		t.Error("an autoplay that added nothing should report false")
	}
	if calls != 1 {
		t.Fatalf("autoplay ran %d times, want 1", calls)
	}
	// Straight away again: held off, so a dead source is not hammered.
	if player.autoplay(ctx) || calls != 1 {
		t.Errorf("autoplay ran %d times inside the cooldown, want 1", calls)
	}

	time.Sleep(80 * time.Millisecond)
	addTracks = true
	if !player.autoplay(ctx) {
		t.Error("an autoplay that added a track should report true")
	}
	if calls != 2 {
		t.Fatalf("autoplay ran %d times after the cooldown, want 2", calls)
	}
	// A successful call clears the hold-off, so the next one is free to run.
	if !player.autoplay(ctx) || calls != 3 {
		t.Errorf("autoplay ran %d times after a success, want 3", calls)
	}
}

// TestAutoplayNotReentered covers the in-progress guard: without it a callback
// that itself triggers a queue change can be re-entered.
func TestAutoplayNotReentered(t *testing.T) {
	var calls int
	client := testClient(t, nil)
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")
	client.cfg.Autoplay = func(ctx context.Context, p *Player) error {
		calls++
		if calls < 3 {
			p.autoplay(ctx) // re-entry must be refused
		}
		return nil
	}

	player.autoplay(context.Background())
	if calls != 1 {
		t.Errorf("autoplay was re-entered: ran %d times, want 1", calls)
	}
}

// TestDestroyedPlayerIgnoresTrackEvents covers events arriving after a teardown:
// every command they could start fails anyway, and a queue edit would still
// reach Config.OnQueueChange after the destroy event went out.
func TestDestroyedPlayerIgnoresTrackEvents(t *testing.T) {
	var changes int
	client := testClient(t, func(c *Config) {
		c.OnQueueChange = func(context.Context, string, queue.Change, []lavalink.Track) { changes++ }
	})
	bodies := make(chan []byte, 16)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	client.players["g"] = player

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"})
	if err := player.Destroy(ctx, DestroyRequested); err != nil {
		t.Fatal(err)
	}
	drained := changes
	// Destroy's own DELETE lands in bodies with an empty body; drain it so the
	// check below only sees requests the events caused.
	for len(bodies) > 0 {
		<-bodies
	}

	for _, event := range []Event{
		&TrackEndEvent{Player: player, Track: lavalink.Track{Encoded: "one"}, Reason: lavalink.ReasonFinished},
		&TrackStartEvent{Player: player, Track: lavalink.Track{Encoded: "two"}},
		&TrackPromotedEvent{Player: player, Track: lavalink.Track{Encoded: "two"}},
		&TrackStuckEvent{Player: player, Track: lavalink.Track{Encoded: "one"}},
	} {
		player.handle(ctx, event)
	}
	if changes != drained {
		t.Errorf("a destroyed player reported %d queue changes after the teardown", changes-drained)
	}
	select {
	case body := <-bodies:
		t.Errorf("a destroyed player sent a request: %s", body)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestStrandedPlayersStopPlaying covers a node drop: its players are silent, so
// the player must stop claiming a track is running. lavalink-client clears
// player.playing in close() for the same reason.
func TestStrandedPlayersStopPlaying(t *testing.T) {
	client := testClient(t, nil)
	node := testNode(t, client, make(chan []byte, 16))
	player := newPlayer(client, node, "g")
	client.players["g"] = player

	player.setStarted(true)
	if !player.started() {
		t.Fatal("setup: expected a started player")
	}
	node.stranded()
	if player.started() {
		t.Error("a player whose node dropped still claims to be playing")
	}
}

// TestStrandedPlayersStayPutWithoutANode covers the freeze policy: with nothing
// to move to, a player is left in place for the reconnect rather than destroyed.
func TestStrandedPlayersStayPutWithoutANode(t *testing.T) {
	client := testClient(t, func(c *Config) { c.AutoMove = true })
	node := testNode(t, client, make(chan []byte, 16))
	player := newPlayer(client, node, "g")
	client.players["g"] = player
	player.queue.SetCurrent(context.Background(), &lavalink.Track{Encoded: "one"})

	node.stranded()

	if player.Destroyed() {
		t.Error("a stranded player with nowhere to go was destroyed instead of frozen")
	}
	if player.Node() != node {
		t.Error("a stranded player was moved to a node that cannot take it")
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "one" {
		t.Errorf("a stranded player lost its queue: %v", current)
	}
}

// TestCallbacksCanCallBackIn is the deadlock guard. Commands serialise on a
// lock, and they are what trigger events and queue-change callbacks, so user
// code calling a command from inside one used to wait on a lock its own
// goroutine was holding — forever. A QueueEndEvent handler that starts playing
// again is the obvious case and the most common bot pattern there is.
//
// Each case signals only after its nested command RETURNS, so a deadlock shows
// up as a timeout rather than a leaked goroutine nobody waits on.
func TestCallbacksCanCallBackIn(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arm installs the callback. It resolves the player lazily through get,
		// because OnQueueChange has to be set before the player is built, and
		// calls done once its nested command returns.
		arm     func(client *Client, get func() *Player, done func())
		trigger func(player *Player)
	}{
		{
			// next() emits QueueEndEvent while it holds the lock.
			name: "queue end listener plays",
			arm: func(client *Client, get func() *Player, done func()) {
				client.AddListener(On(func(e *QueueEndEvent) {
					_ = e.Player.Play(context.Background(), lavalink.Track{Encoded: "rescue"})
					done()
				}))
			},
			trigger: func(player *Player) {
				ctx := context.Background()
				player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "last"})
				player.handle(ctx, &TrackEndEvent{
					Player: player,
					Track:  lavalink.Track{Encoded: "last"},
					Reason: lavalink.ReasonFinished,
				})
			},
		},
		{
			// play() emits IdleCancelEvent while it holds the lock.
			name: "idle listener stops",
			arm: func(client *Client, get func() *Player, done func()) {
				client.AddListener(On(func(e *IdleCancelEvent) {
					_ = e.Player.Stop(context.Background())
					done()
				}))
			},
			trigger: func(player *Player) {
				player.startIdle()
				_ = player.Play(context.Background(), lavalink.Track{Encoded: "one"})
			},
		},
		{
			// Every queue edit inside a command runs OnQueueChange.
			name: "queue change handler skips",
			arm: func(client *Client, get func() *Player, done func()) {
				client.cfg.OnQueueChange = func(context.Context, string, queue.Change, []lavalink.Track) {
					if player := get(); player != nil {
						_ = player.Skip(context.Background())
						done()
					}
				}
			},
			trigger: func(player *Player) {
				ctx := context.Background()
				player.queue.Add(ctx, lavalink.Track{Encoded: "two"})
				_ = player.Play(ctx, lavalink.Track{Encoded: "one"})
			},
		},
		{
			// Autoplay runs inline, so it relies on the context token instead.
			name: "autoplay plays with its own ctx",
			arm: func(client *Client, get func() *Player, done func()) {
				client.cfg.Autoplay = func(ctx context.Context, p *Player) error {
					// The ctx it was handed marks the running command, so this
					// re-enters rather than waiting on the lock.
					err := p.Play(ctx, lavalink.Track{Encoded: "auto"})
					done()
					return err
				}
			},
			trigger: func(player *Player) {
				ctx := context.Background()
				player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "last"})
				player.handle(ctx, &TrackEndEvent{
					Player: player,
					Track:  lavalink.Track{Encoded: "last"},
					Reason: lavalink.ReasonFinished,
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bodies := make(chan []byte, 256)
			go func() {
				for range bodies {
				}
			}()
			client := testClient(t, func(c *Config) { c.EmptyQueueTimeout = time.Hour })
			node := testNode(t, client, bodies)

			reached := make(chan struct{})
			var once sync.Once
			done := func() { once.Do(func() { close(reached) }) }

			var mu sync.Mutex
			var target *Player
			tc.arm(client, func() *Player {
				mu.Lock()
				defer mu.Unlock()
				return target
			}, done)

			player := newPlayer(client, node, "g")
			mu.Lock()
			target = player
			mu.Unlock()
			client.players["g"] = player

			go tc.trigger(player)

			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("deadlocked: a callback called a command and it never returned")
			}
		})
	}
}

// TestCrossfadeMirrorsNode covers the node's word winning over the local cache.
// A Kairo node reports the transition settings actually in effect, with its own
// defaults filled in; trusting the cache instead is what let a player claim
// crossfade was on while the node ran its own configuration.
func TestCrossfadeMirrorsNode(t *testing.T) {
	client := testClient(t, func(c *Config) {
		c.Crossfade = &lavalink.Crossfade{Enable: true, DurationMs: 1000}
	})
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")

	// What the node says is in effect, including a field we never sent.
	player.absorb(lavalink.PlayerInfo{
		Volume: 100,
		Crossfade: lavalink.Value(lavalink.Crossfade{
			Enable: true, DurationMs: 3000, ManualDurationMs: 3500, Curve: lavalink.CrossfadeSCurve,
		}),
	})
	got := player.Crossfade()
	if got == nil || got.DurationMs != 3000 || got.Curve != lavalink.CrossfadeSCurve {
		t.Fatalf("the node's settings were not mirrored: %+v", got)
	}
	if !player.crossfading() {
		t.Error("the mirrored settings should still count as crossfading")
	}

	// Explicitly null means no transitions, and must not fall back to the
	// client-wide setting.
	player.absorb(lavalink.PlayerInfo{Volume: 100, Crossfade: lavalink.Null[lavalink.Crossfade]()})
	if player.crossfading() {
		t.Error("a node reporting no transitions should turn crossfading off, not fall back")
	}
}

// TestCrossfadeUntouchedByStockNode covers the other side: stock Lavalink never
// sends the field, so the local override has to survive every reply.
func TestCrossfadeUntouchedByStockNode(t *testing.T) {
	client := testClient(t, nil)
	player := newPlayer(client, &Node{cfg: NodeConfig{Name: "test"}, client: client, log: client.Logger()}, "g")

	player.mu.Lock()
	player.crossfade = &lavalink.Crossfade{Enable: true, DurationMs: 1500}
	player.mu.Unlock()

	// A reply with no crossfade field at all.
	player.absorb(lavalink.PlayerInfo{Volume: 100})
	got := player.Crossfade()
	if got == nil || !got.Enable || got.DurationMs != 1500 {
		t.Errorf("a stock node's reply clobbered the override: %+v", got)
	}
}

// TestCrossfadeSeekSkipSequence is the Reever report, exactly:
// crossfade on, play, seek, skip, seek, skip twice.
//
// A crossfade skip hands over by playhead — the node promotes the successor
// when the outgoing track reaches the end of the fade window, and the marker it
// uses lives on that track's timeline. Two things used to destroy it: a Seek,
// which moves the playhead away from the marker, and the pre-buffer sync, whose
// crossfade settings make the node re-arm and throw the marker away. Either left
// the node holding a successor it could never promote, after which it ignores
// every new transition and only promotes the stuck one — so the next skip did
// nothing visible and the one after it finally advanced.
func TestCrossfadeSeekSkipSequence(t *testing.T) {
	client := testClient(t, func(c *Config) {
		c.Crossfade = &lavalink.Crossfade{Enable: true, DurationMs: 3000, ManualDurationMs: 3500}
	})
	bodies := make(chan []byte, 64)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	seekable := func(encoded string) lavalink.Track {
		return lavalink.Track{
			Encoded: encoded,
			Info:    lavalink.TrackInfo{Length: 4 * lavalink.Minute, IsSeekable: true},
		}
	}
	drain := func() {
		for {
			select {
			case <-bodies:
			case <-time.After(80 * time.Millisecond):
				return
			}
		}
	}
	nextBody := func() string {
		t.Helper()
		select {
		case b := <-bodies:
			return string(b)
		case <-time.After(2 * time.Second):
			t.Fatal("no request reached the node")
			return ""
		}
	}

	ctx := context.Background()

	// 1. Play, with two more queued so a skip has somewhere to go.
	if err := player.Play(ctx, seekable("one")); err != nil {
		t.Fatal(err)
	}
	player.queue.Add(ctx, seekable("two"), seekable("three"))
	drain()

	// 2. Seek works.
	if err := player.Seek(ctx, 30*lavalink.Second); err != nil {
		t.Fatalf("the first seek failed: %v", err)
	}
	if body := nextBody(); !strings.Contains(body, `"position":30000`) {
		t.Errorf("the first seek sent %s", body)
	}

	// 3. Skip arms the hand-over and leaves the queue where it is.
	if err := player.Skip(ctx); err != nil {
		t.Fatalf("skip: %v", err)
	}
	body := nextBody()
	if !strings.Contains(body, `"transition":true`) || !strings.Contains(body, `"nextTrack":{"encoded":"two"}`) {
		t.Fatalf("the skip did not arm a transition: %s", body)
	}
	if !player.Transitioning() {
		t.Fatal("the player should report the hand-over in flight")
	}

	// 4. Seek during the hand-over. It must refuse rather than move the playhead
	//    the node is promoting on — that is the bug.
	if err := player.Seek(ctx, 10*lavalink.Second); !errors.Is(err, ErrTransitioning) {
		t.Errorf("seeking mid-hand-over returned %v, want ErrTransitioning", err)
	}
	if err := player.SetEndTime(ctx, 20*lavalink.Second); !errors.Is(err, ErrTransitioning) {
		t.Errorf("setting an end time mid-hand-over returned %v, want ErrTransitioning", err)
	}
	select {
	case body := <-bodies:
		t.Fatalf("a request reached the node during the hand-over: %s", body)
	case <-time.After(150 * time.Millisecond):
	}

	// 4b. The debounced pre-buffer must stay silent too: its crossfade settings
	//     are what made the node re-arm and drop the promotion marker.
	player.queue.Add(ctx, seekable("four"))
	select {
	case body := <-bodies:
		t.Fatalf("a queue edit pre-buffered during the hand-over: %s", body)
	case <-time.After(250 * time.Millisecond):
	}

	// 5. The node finishes the hand-over. ONE skip is all it took, so the queue
	//    advances here and the window closes.
	player.handle(ctx, &TrackEndEvent{
		Player: player,
		Track:  lavalink.Track{Encoded: "one"},
		Reason: lavalink.ReasonCrossfade,
	})
	if player.Transitioning() {
		t.Error("the hand-over should be over once the node promoted it")
	}
	if current := player.queue.Current(); current == nil || current.Encoded != "two" {
		t.Fatalf("the queue did not advance on the promotion: %v", current)
	}

	// Seeking works again straight away, against the track now playing.
	if err := player.Seek(ctx, 45*lavalink.Second); err != nil {
		t.Fatalf("the seek after the hand-over failed: %v", err)
	}
	var seen string
	for i := 0; i < 8 && !strings.Contains(seen, `"position":45000`); i++ {
		seen = nextBody()
	}
	if !strings.Contains(seen, `"position":45000`) {
		t.Errorf("the seek after the hand-over sent %s", seen)
	}
	drain()

	// And a single skip advances — no need to press it twice.
	if err := player.Skip(ctx); err != nil {
		t.Fatalf("the second skip: %v", err)
	}
	if body := nextBody(); !strings.Contains(body, `"transition":true`) {
		t.Fatalf("the second skip did not arm a transition: %s", body)
	}
	player.handle(ctx, &TrackEndEvent{
		Player: player,
		Track:  lavalink.Track{Encoded: "two"},
		Reason: lavalink.ReasonCrossfade,
	})
	if current := player.queue.Current(); current == nil || current.Encoded != "three" {
		t.Fatalf("one skip should advance to three, got %v", current)
	}
}

// TestTransitioningClearedByNode is the backstop: a hand-over that never
// produced a promoted TrackEnd must not leave seeking refused forever.
func TestTransitioningClearedByNode(t *testing.T) {
	client := testClient(t, func(c *Config) { c.Crossfade = &lavalink.Crossfade{Enable: true} })
	bodies := make(chan []byte, 32)
	player := newPlayer(client, testNode(t, client, bodies), "g")

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{Encoded: "one"})
	player.queue.Add(ctx, lavalink.Track{Encoded: "two"})

	if err := player.Skip(ctx); err != nil {
		t.Fatal(err)
	}
	if !player.Transitioning() {
		t.Fatal("setup: expected a hand-over in flight")
	}
	// Only a TrackStart arrives, with no promoted TrackEnd before it.
	player.handle(ctx, &TrackStartEvent{Player: player, Track: lavalink.Track{Encoded: "two"}})
	if player.Transitioning() {
		t.Error("a track starting should clear the hand-over flag")
	}
}

// TestSeekReplyNotRejectedByPrecision pins a bug a real node found and the fake
// could not: the wire carries unix millis, so a reply the node generated after a
// seek truncates to an earlier instant than a nanosecond-precision local stamp.
// The freshness gate then rejected it and threw away the volume, pause and
// filters it carried — the player went on reporting the wrong pause state.
func TestSeekReplyNotRejectedByPrecision(t *testing.T) {
	client := testClient(t, nil)
	bodies := make(chan []byte, 8)
	player := newPlayer(client, testNode(t, client, bodies), "g")
	go func() {
		for range bodies {
		}
	}()

	ctx := context.Background()
	player.queue.SetCurrent(ctx, &lavalink.Track{
		Encoded: "one",
		Info:    lavalink.TrackInfo{Length: 5 * lavalink.Minute, IsSeekable: true},
	})
	if err := player.Seek(ctx, 30*lavalink.Second); err != nil {
		t.Fatal(err)
	}

	// The local stamp has to be a whole millisecond, or anything the node sends
	// inside that millisecond looks older than it is.
	stamp := player.State().Time.Time
	if stamp.Truncate(time.Millisecond) != stamp {
		t.Errorf("the local seek stamp %v is finer than the wire's millisecond", stamp)
	}

	// A reply stamped in the very same millisecond must still apply.
	same := lavalink.PlayerState{
		Time:     lavalink.Timestamp{Time: time.UnixMilli(stamp.UnixMilli())},
		Position: 30 * lavalink.Second,
		Ping:     42,
	}
	player.absorb(lavalink.PlayerInfo{State: same, Volume: 77, Paused: true})
	if !player.Paused() {
		t.Error("a reply from the same millisecond as the seek was rejected, losing the pause")
	}
	if got := player.Volume(); got != 77 {
		t.Errorf("volume is %d after that reply, want 77", got)
	}
}
