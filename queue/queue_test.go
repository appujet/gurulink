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

// TestQueue walks a queue the way a player does: fill it, advance through it,
// then go back.
func TestQueue(t *testing.T) {
	q := New("1", Config{})
	q.Add(track("a", lavalink.Second), track("b", 2*lavalink.Second))
	q.AddNext(track("first", 0))

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
		got, ok := q.Advance()
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
	if _, ok := q.Advance(); ok {
		t.Error("Advance on an empty queue should report false")
	}
	if q.Current() != nil {
		t.Error("a dry queue clears Current")
	}
	if got := q.Previous(); len(got) != 3 || got[0].Encoded != "b" {
		t.Errorf("Previous should be newest first, got %v", got)
	}

	// Back replays the last track; the one playing goes to the front. Nothing
	// plays here, so the queue stays empty.
	if got, ok := q.Back(); !ok || got.Encoded != "b" {
		t.Errorf("Back gave %q, %v", got.Encoded, ok)
	}
	if cur := q.Current(); cur == nil || cur.Encoded != "b" {
		t.Error("Back should make the played track current")
	}
	if _, ok := q.Back(); !ok {
		t.Error("Back should still have history")
	}
	if got := q.Len(); got != 1 {
		t.Errorf("the interrupted track goes back to the queue, got %d", got)
	}
}

// TestEdits covers the index maths, which is where an off-by-one hides.
func TestEdits(t *testing.T) {
	q := New("1", Config{})
	q.Add(track("a", 0), track("b", 0), track("c", 0), track("d", 0))

	if err := q.Move(0, 3); err != nil {
		t.Fatal(err)
	}
	if got := encodedOf(q.Tracks()); got != "bcda" {
		t.Errorf("after Move got %q, want bcda", got)
	}
	if err := q.Swap(0, 3); err != nil {
		t.Fatal(err)
	}
	if got := encodedOf(q.Tracks()); got != "acdb" {
		t.Errorf("after Swap got %q, want acdb", got)
	}
	for _, err := range []error{q.Move(0, 9), q.Move(-1, 0), q.Swap(0, 9)} {
		if err == nil {
			t.Error("out of range edits should fail")
		}
	}

	removed, ok := q.RemoveRange(1, 3)
	if !ok || encodedOf(removed) != "cd" {
		t.Errorf("RemoveRange gave %q, %v", encodedOf(removed), ok)
	}
	if _, ok := q.RemoveRange(1, 1); ok {
		t.Error("an empty range is not a removal")
	}
	if _, ok := q.Remove(5); ok {
		t.Error("removing past the end should fail")
	}

	q.Add(track("keep", 0))
	q.Filter(func(t lavalink.Track) bool { return t.Encoded == "keep" })
	if got := encodedOf(q.Tracks()); got != "keep" {
		t.Errorf("after Filter got %q", got)
	}
	if got := q.Find(func(t lavalink.Track) bool { return t.Encoded == "keep" }); got != 0 {
		t.Errorf("Find gave %d, want 0", got)
	}

	q.Add(track("x", 0), track("y", 0))
	q.Shuffle()
	if got := q.Len(); got != 3 {
		t.Errorf("Shuffle changed the queue length to %d", got)
	}
	q.Clear()
	if got := q.Len(); got != 0 {
		t.Errorf("Clear left %d tracks", got)
	}
}

// TestStore round trips through the ready-made memory store, which also checks
// that store satisfies [Store] without importing it.
func TestStore(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Store: &store.Memory{}}
	q := New("1", cfg)
	q.Add(track("a", lavalink.Second), track("b", lavalink.Second))
	q.Advance()

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
