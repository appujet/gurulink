package queue

import (
	"context"
	"testing"

	"github.com/appujet/gurulink/lavalink"
	"github.com/appujet/gurulink/store"
)

func track(encoded string, length lavalink.Duration) lavalink.Track {
	return lavalink.Track{Encoded: encoded, Info: lavalink.TrackInfo{Identifier: encoded, Length: length}}
}

func encodedOf(tracks []lavalink.Track) string {
	var out string
	for _, t := range tracks {
		out += t.Encoded
	}
	return out
}

// TestQueue walks a queue the way a player does: fill, advance, go back.
func TestQueue(t *testing.T) {
	ctx := context.Background()
	q := New("1", Config{})
	q.Add(ctx, track("a", lavalink.Second), track("b", 2*lavalink.Second))
	q.AddNext(ctx, track("first", 0))

	if got := q.Len(); got != 3 {
		t.Fatalf("got %d tracks, want 3", got)
	}
	if got := q.Duration(); got != 3*lavalink.Second {
		t.Errorf("got duration %s, want 3s", got)
	}
	if next, _ := q.Peek(); next.Encoded != "first" {
		t.Errorf("AddNext should jump the queue, got %q", next.Encoded)
	}

	for _, want := range []string{"first", "a", "b"} {
		got, ok := q.Advance(ctx)
		if !ok {
			t.Fatalf("Advance ran dry before %q", want)
		}
		if got.Encoded != want {
			t.Fatalf("got %q, want %q", got.Encoded, want)
		}
		if cur := q.Current(); cur == nil || cur.Encoded != want {
			t.Fatalf("Current is not %q", want)
		}
	}
	if _, ok := q.Advance(ctx); ok {
		t.Error("Advance on an empty queue should report false")
	}
	if q.Current() != nil {
		t.Error("a dry queue clears Current")
	}
	if got := q.Previous(); len(got) != 3 || got[0].Encoded != "b" {
		t.Errorf("Previous should be newest first, got %v", got)
	}

	// Back replays the last track and the playing one goes to the front.
	if got, ok := q.Back(ctx); !ok || got.Encoded != "b" {
		t.Errorf("Back gave %q, %v", got.Encoded, ok)
	}
	if cur := q.Current(); cur == nil || cur.Encoded != "b" {
		t.Error("Back should make the played track current")
	}
	if _, ok := q.Back(ctx); !ok {
		t.Error("Back should still have history")
	}
	if got := q.Len(); got != 1 {
		t.Errorf("the interrupted track goes back to the queue, got %d", got)
	}
}

// TestEdits covers the index maths, which is where an off-by-one hides.
func TestEdits(t *testing.T) {
	ctx := context.Background()
	q := New("1", Config{})
	q.Add(ctx, track("a", 0), track("b", 0), track("c", 0), track("d", 0))

	if err := q.Move(ctx, 0, 3); err != nil {
		t.Fatal(err)
	}
	if got := encodedOf(q.Tracks()); got != "bcda" {
		t.Errorf("after Move got %q, want bcda", got)
	}
	if err := q.Swap(ctx, 0, 3); err != nil {
		t.Fatal(err)
	}
	if got := encodedOf(q.Tracks()); got != "acdb" {
		t.Errorf("after Swap got %q, want acdb", got)
	}
	for _, err := range []error{q.Move(ctx, 0, 9), q.Move(ctx, -1, 0), q.Swap(ctx, 0, 9)} {
		if err == nil {
			t.Error("out of range edits should fail")
		}
	}

	removed, ok := q.RemoveRange(ctx, 1, 3)
	if !ok || encodedOf(removed) != "cd" {
		t.Errorf("RemoveRange gave %q, %v", encodedOf(removed), ok)
	}
	if _, ok := q.RemoveRange(ctx, 1, 1); ok {
		t.Error("an empty range is not a removal")
	}
	if _, ok := q.Remove(ctx, 5); ok {
		t.Error("removing past the end should fail")
	}

	q.Add(ctx, track("keep", 0))
	q.Filter(ctx, func(t lavalink.Track) bool { return t.Encoded == "keep" })
	if got := encodedOf(q.Tracks()); got != "keep" {
		t.Errorf("after Filter got %q", got)
	}
	if got := q.Find(func(t lavalink.Track) bool { return t.Encoded == "keep" }); got != 0 {
		t.Errorf("Find gave %d, want 0", got)
	}

	q.Add(ctx, track("x", 0), track("y", 0))
	q.Shuffle(ctx)
	if got := q.Len(); got != 3 {
		t.Errorf("Shuffle changed the queue length to %d", got)
	}
	q.Clear(ctx)
	if got := q.Len(); got != 0 {
		t.Errorf("Clear left %d tracks", got)
	}

	// Every edit reports a change, Swap included; a no-op edit stays quiet, since
	// every change is a full store write.
	var last Change
	var changes int
	q = New("1", Config{OnChange: func(_ context.Context, _ string, c Change, _ []lavalink.Track) {
		last, changes = c, changes+1
	}})
	q.Add(ctx, track("a", 0), track("b", 0))

	changes = 0
	if err := q.Swap(ctx, 0, 1); err != nil || changes != 1 || last != Shuffled {
		t.Errorf("Swap should report one shuffle, got %d %q: %v", changes, last, err)
	}
	if err := q.Move(ctx, 0, 1); err != nil || last != Shuffled {
		t.Errorf("Move should report a shuffle, got %q: %v", last, err)
	}

	changes = 0
	q.Clear(ctx)
	q.Clear(ctx)
	q.Shuffle(ctx)
	if changes != 1 {
		t.Errorf("only the first Clear changed anything, got %d changes", changes)
	}

	q.Add(ctx, track("a", 0))
	playing, _ := q.Advance(ctx)
	changes = 0
	q.SetCurrent(ctx, &playing)
	if changes != 0 {
		t.Error("setting the track Advance already made current should stay quiet")
	}
	q.SetCurrent(ctx, nil)
	if changes != 1 || last != Current {
		t.Errorf("clearing the playing track should report %q, got %d %q", Current, changes, last)
	}

	// Callers pass tracks out of events listeners still hold, so the queue must not
	// keep the caller's pointer.
	mine := track("x", 0)
	q.SetCurrent(ctx, &mine)
	mine.Encoded = "mutated"
	if cur := q.Current(); cur == nil || cur.Encoded != "x" {
		t.Errorf("SetCurrent kept the caller's track: %v", cur)
	}
}

// TestStore round trips the memory store, which also checks that store satisfies
// [Store] without importing it.
func TestStore(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Store: &store.Memory{}}
	q := New("1", cfg)
	q.Add(ctx, track("a", lavalink.Second), track("b", lavalink.Second))
	q.Advance(ctx)

	loaded := New("1", cfg)
	if err := loaded.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if cur := loaded.Current(); cur == nil || cur.Encoded != "a" {
		t.Error("the playing track should persist")
	}
	if got := encodedOf(loaded.Tracks()); got != "b" {
		t.Errorf("got %q queued, want b", got)
	}

	empty := New("2", cfg)
	if err := empty.Load(ctx); err != nil {
		t.Errorf("a guild the store never saw is not an error: %v", err)
	}
	if empty.Len() != 0 {
		t.Error("nothing to load should leave the queue alone")
	}

	if err := q.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := New("1", cfg).Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := New("1", Config{}).Delete(ctx); err != nil {
		t.Errorf("Delete without a store does nothing: %v", err)
	}
}
