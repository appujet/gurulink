//go:build integration

package gurulink

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/appujet/gurulink/lavalink"
)

// These run against a real Lavalink node rather than the httptest fake, which
// answers every request with the same canned body. Only a real node can say
// whether the JSON gurulink produces is actually accepted — the omitzero and
// Nullable encoding, the noReplace query parameter, a null track, a filter
// payload — and only a real node stamps PlayerState.Time, which the freshness
// gate depends on.
//
// Actual audio needs a Discord voice connection, so what is covered here is the
// wire contract and state sync, not decoding.
//
//	docker run -d --rm --name ll --network host \
//	  -v $PWD/application.yml:/opt/Lavalink/application.yml \
//	  -v $PWD:/audio:ro ghcr.io/lavalink-devs/lavalink:4
//	LAVALINK_TRACK=/audio/tone.mp3 go test -tags integration ./gurulink/ -run TestReal -v

func realNode(t *testing.T) (*Client, *Node) {
	t.Helper()
	address := os.Getenv("LAVALINK_ADDRESS")
	if address == "" {
		address = "127.0.0.1:2333"
	}
	password := os.Getenv("LAVALINK_PASSWORD")
	if password == "" {
		password = "youshallnotpass"
	}

	ready := make(chan *ReadyEvent, 1)
	client := testClient(t, func(c *Config) {
		c.Listeners = []Listener{On(func(e *ReadyEvent) {
			select {
			case ready <- e:
			default:
			}
		})}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	node, err := client.AddNode(ctx, NodeConfig{Name: "real", Address: address, Password: password})
	if err != nil {
		t.Skipf("no Lavalink at %s: %v", address, err)
	}
	t.Cleanup(func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client.Close(shutdown)
	})

	select {
	case e := <-ready:
		if e.SessionID == "" {
			t.Fatal("the node handshook without a session id")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the node never sent its ready handshake")
	}
	if !node.Available() {
		t.Fatal("the node is not available after ready")
	}
	return client, node
}

func realTrack(t *testing.T, ctx context.Context, node *Node) lavalink.Track {
	t.Helper()
	identifier := os.Getenv("LAVALINK_TRACK")
	if identifier == "" {
		identifier = "/audio/tone.mp3"
	}
	result, err := node.LoadTracks(ctx, identifier)
	if err != nil {
		t.Fatalf("load %s: %v", identifier, err)
	}
	tracks := result.AllTracks()
	if len(tracks) == 0 {
		t.Skipf("the node loaded no track for %q (load type %s)", identifier, result.LoadType)
	}
	return tracks[0]
}

// TestRealCommandsAreAccepted puts every player command gurulink can send past a
// real node. The fake answers anything; this catches a body the node rejects.
func TestRealCommandsAreAccepted(t *testing.T) {
	client, node := realNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	track := realTrack(t, ctx, node)
	if !track.Info.IsSeekable || track.Info.Length <= 0 {
		t.Fatalf("the test track is not seekable or has no length: %+v", track.Info)
	}

	player, err := client.NewPlayer("1234567890")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		done, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = player.Destroy(done, DestroyRequested)
	})

	if err := player.Play(ctx, track); err != nil {
		t.Fatalf("play: %v", err)
	}
	info, err := node.FetchPlayer(ctx, player.GuildID())
	if err != nil {
		t.Fatalf("fetch player: %v", err)
	}
	if info.Track == nil || info.Track.Encoded != track.Encoded {
		t.Fatalf("the node is holding %v, want the track we played", info.Track)
	}
	// The gate's ordering key has to actually come back from a real node.
	if info.State.Time.IsZero() {
		t.Fatal("the node sent no state timestamp, so the freshness gate has no ordering key")
	}

	if err := player.SetVolume(ctx, 55); err != nil {
		t.Fatalf("set volume: %v", err)
	}
	if got := player.Volume(); got != 55 {
		t.Errorf("volume is %d after the node replied, want 55", got)
	}

	// A seek past the end is clamped against the node's own reported length.
	if err := player.Seek(ctx, track.Info.Length*4); err != nil {
		t.Fatalf("seek past the end: %v", err)
	}
	if got := player.State().Position; got > track.Info.Length {
		t.Errorf("the position is %s, past the track length %s", got, track.Info.Length)
	}

	filters := lavalink.Filters{Volume: func() *float32 { v := float32(0.8); return &v }()}
	if err := player.SetFilters(ctx, filters); err != nil {
		t.Fatalf("set filters: %v", err)
	}
	if err := player.ClearFilters(ctx); err != nil {
		t.Fatalf("clear filters: %v", err)
	}

	if err := player.Pause(ctx, true); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if !player.Paused() {
		t.Error("the node did not report the pause")
	}
	if err := player.Resume(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// noReplace is a query parameter, not part of the body.
	if err := player.Update(ctx, lavalink.PlayerUpdate{
		Track:     &lavalink.UpdateTrack{Encoded: lavalink.Value(track.Encoded)},
		NoReplace: true,
	}); err != nil {
		t.Fatalf("noReplace update: %v", err)
	}

	// A null track is how both Stop and Skip ask for silence.
	if err := player.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	info, err = node.FetchPlayer(ctx, player.GuildID())
	if err != nil {
		t.Fatalf("fetch player after stop: %v", err)
	}
	if info.Track != nil {
		t.Errorf("the node is still holding %v after a stop", info.Track)
	}
}

// TestRealSeekGuardsUseNodeMetadata checks the guards against what the node
// actually reports, rather than hand-written TrackInfo.
func TestRealSeekGuardsUseNodeMetadata(t *testing.T) {
	client, node := realNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	player, err := client.NewPlayer("1234567891")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		done, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = player.Destroy(done, DestroyRequested)
	})

	// Nothing playing: a no-op that never reaches the node.
	if err := player.Seek(ctx, lavalink.Second); err != nil {
		t.Errorf("seeking with nothing playing: %v", err)
	}

	track := realTrack(t, ctx, node)
	if err := player.Play(ctx, track); err != nil {
		t.Fatal(err)
	}
	// Mid-track, and the local timeline moves before the reply lands.
	target := track.Info.Length / 2
	if err := player.Seek(ctx, target); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if got := player.State().Position; got != target {
		t.Errorf("the local position is %s after seeking to %s", got, target)
	}
}

// TestRealSessionResumes covers the handshake path a reconnect depends on.
func TestRealSessionResumes(t *testing.T) {
	client, node := realNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	on, seconds := true, 60
	session, err := node.UpdateSession(ctx, lavalink.SessionUpdate{Resuming: &on, Timeout: &seconds})
	if err != nil {
		t.Fatalf("enable resuming: %v", err)
	}
	if !session.Resuming || session.Timeout != seconds {
		t.Errorf("the node reported resuming=%v timeout=%d, want true/%d", session.Resuming, session.Timeout, seconds)
	}
	if _, err := node.PlayerInfos(ctx); err != nil {
		t.Errorf("list players: %v", err)
	}
	if _, err := node.Info(ctx); err != nil {
		t.Errorf("node info: %v", err)
	}
	_ = client
}

// TestRealKairoFields puts the four fields stock Lavalink ignores past a real
// node: crossfade, tape, nextTrack and transition, in both their value and
// explicit-null forms. Nothing but a real server validates that encoding.
func TestRealKairoFields(t *testing.T) {
	client, node := realNode(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	track := realTrack(t, ctx, node)
	player, err := client.NewPlayer("1234567892")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		done, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = player.Destroy(done, DestroyRequested)
	})
	if err := player.Play(ctx, track); err != nil {
		t.Fatal(err)
	}

	if err := player.SetCrossfade(ctx, &lavalink.Crossfade{Enable: true, DurationMs: 3000}); err != nil {
		t.Fatalf("set crossfade: %v", err)
	}
	// A Kairo node answers with the settings in effect, its own defaults filled
	// in, and absorb has to take them over the local cache.
	if got := player.Crossfade(); got == nil || !got.Enable {
		t.Errorf("crossfade is %+v after the node replied", got)
	} else if got.DurationMs != 3000 {
		t.Logf("the node adjusted durationMs to %d (it reports what is in effect)", got.DurationMs)
	}
	if !player.crossfading() {
		t.Error("the player should be crossfading after the node accepted it")
	}

	if err := player.SetTape(ctx, &lavalink.Tape{Enable: true, DurationMs: 400}); err != nil {
		t.Fatalf("set tape: %v", err)
	}
	// A tape ramps the pause, so this exercises the pause payload carrying it.
	if err := player.Pause(ctx, true); err != nil {
		t.Fatalf("pause with a tape: %v", err)
	}
	if err := player.Resume(ctx); err != nil {
		t.Fatalf("resume with a tape: %v", err)
	}

	if err := player.Update(ctx, lavalink.PlayerUpdate{
		NextTrack:  lavalink.Value(lavalink.UpdateTrack{Encoded: lavalink.Value(track.Encoded)}),
		Transition: true,
	}); err != nil {
		t.Fatalf("nextTrack with a transition: %v", err)
	}
	// PreBuffer is the path that normally sends it.
	player.queue.Add(ctx, track)
	if err := player.PreBuffer(ctx); err != nil {
		t.Fatalf("pre-buffer: %v", err)
	}

	// The explicit-null forms, which is how crossfade gets turned off.
	if err := player.Update(ctx, lavalink.PlayerUpdate{
		NextTrack: lavalink.Null[lavalink.UpdateTrack](),
		Crossfade: lavalink.Null[lavalink.Crossfade](),
		Tape:      lavalink.Null[lavalink.Tape](),
	}); err != nil {
		t.Fatalf("null transition fields: %v", err)
	}
	if err := player.SetCrossfade(ctx, &lavalink.Crossfade{}); err != nil {
		t.Fatalf("disable crossfade: %v", err)
	}
	if player.crossfading() {
		t.Error("crossfading should be off after the node accepted the disable")
	}
}
