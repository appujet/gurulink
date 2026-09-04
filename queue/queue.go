// Package queue is a player's track list: what plays now, what played before
// and what comes next, with optional persistence through a [Store].
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/appujet/gurulink/lavalink"
)

// Change says what happened to a queue.
type Change string

const (
	Added    Change = "added"
	Removed  Change = "removed"
	Shuffled Change = "shuffled"
)

// Store persists queues so they outlive a restart. Get returns nil data
// for a guild it never saw. Implementations must be safe for concurrent use.
type Store interface {
	Get(ctx context.Context, guildID string) ([]byte, error)
	Set(ctx context.Context, guildID string, data []byte) error
	Delete(ctx context.Context, guildID string) error
}

// Config configures a [Queue]. Every field is optional.
type Config struct {
	// Store persists the queue. Without one nothing is written.
	Store Store
	// Logger reports store failures. Defaults to [slog.Default].
	Logger *slog.Logger
	// OnChange is called after every change, outside the lock.
	OnChange func(guildID string, change Change, tracks []lavalink.Track)
	// HistoryLimit is how many played tracks to remember. Defaults to 25.
	HistoryLimit int
}

// Queue is a player's track list. Every method is safe for concurrent use.
//
// A change persists to [Config.Store] and calls [Config.OnChange] before
// returning, both outside the lock.
type Queue struct {
	guildID  string
	store    Store
	log      *slog.Logger
	onChange func(guildID string, change Change, tracks []lavalink.Track)
	limit    int

	mu       sync.RWMutex
	current  *lavalink.Track
	previous []lavalink.Track
	tracks   []lavalink.Track
}

// New builds an empty queue for a guild. Fill it from the store with
// [Queue.Load].
func New(guildID string, cfg Config) *Queue {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HistoryLimit <= 0 {
		cfg.HistoryLimit = 25
	}
	return &Queue{
		guildID:  guildID,
		store:    cfg.Store,
		log:      cfg.Logger,
		onChange: cfg.OnChange,
		limit:    cfg.HistoryLimit,
	}
}

// Current returns the playing track, or nil when nothing plays.
func (q *Queue) Current() *lavalink.Track {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.current == nil {
		return nil
	}
	t := *q.current
	return &t
}

// Tracks returns a copy of the tracks waiting to play.
func (q *Queue) Tracks() []lavalink.Track {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return slices.Clone(q.tracks)
}

// Previous returns a copy of the played tracks, newest first.
func (q *Queue) Previous() []lavalink.Track {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return slices.Clone(q.previous)
}

// Len is how many tracks are waiting.
func (q *Queue) Len() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.tracks)
}

// Duration is the total play time of the waiting tracks.
func (q *Queue) Duration() lavalink.Duration {
	q.mu.RLock()
	defer q.mu.RUnlock()
	var total lavalink.Duration
	for _, t := range q.tracks {
		total += t.Info.Length
	}
	return total
}

// Add appends tracks to the end of the queue.
func (q *Queue) Add(tracks ...lavalink.Track) {
	if len(tracks) == 0 {
		return
	}
	q.mu.Lock()
	q.tracks = append(q.tracks, tracks...)
	q.mu.Unlock()
	q.changed(Added, tracks)
}

// AddNext puts tracks at the front, so they play before everything queued.
func (q *Queue) AddNext(tracks ...lavalink.Track) {
	q.Insert(0, tracks...)
}

// Insert puts tracks at index i, clamped to the queue bounds.
func (q *Queue) Insert(i int, tracks ...lavalink.Track) {
	if len(tracks) == 0 {
		return
	}
	q.mu.Lock()
	i = min(max(i, 0), len(q.tracks))
	q.tracks = slices.Insert(q.tracks, i, tracks...)
	q.mu.Unlock()
	q.changed(Added, tracks)
}

// Remove drops the track at index i and returns it.
func (q *Queue) Remove(i int) (lavalink.Track, bool) {
	removed, ok := q.RemoveRange(i, i+1)
	if !ok {
		return lavalink.Track{}, false
	}
	return removed[0], true
}

// RemoveRange drops tracks in [start, end) and returns them.
func (q *Queue) RemoveRange(start, end int) ([]lavalink.Track, bool) {
	q.mu.Lock()
	if start < 0 || end > len(q.tracks) || start >= end {
		q.mu.Unlock()
		return nil, false
	}
	removed := slices.Clone(q.tracks[start:end])
	q.tracks = slices.Delete(q.tracks, start, end)
	q.mu.Unlock()
	q.changed(Removed, removed)
	return removed, true
}

// Clear drops every waiting track, leaving the playing one alone.
func (q *Queue) Clear() {
	q.mu.Lock()
	removed := q.tracks
	q.tracks = nil
	q.mu.Unlock()
	q.changed(Removed, removed)
}

// Shuffle reorders the waiting tracks.
func (q *Queue) Shuffle() {
	q.mu.Lock()
	rand.Shuffle(len(q.tracks), func(i, j int) { q.tracks[i], q.tracks[j] = q.tracks[j], q.tracks[i] })
	q.mu.Unlock()
	q.changed(Shuffled, nil)
}

// Move moves the track at from to index to.
func (q *Queue) Move(from, to int) error {
	q.mu.Lock()
	if from < 0 || from >= len(q.tracks) || to < 0 || to >= len(q.tracks) {
		q.mu.Unlock()
		return fmt.Errorf("queue: move %d->%d out of range (%d tracks)", from, to, len(q.tracks))
	}
	track := q.tracks[from]
	q.tracks = slices.Insert(slices.Delete(q.tracks, from, from+1), to, track)
	q.mu.Unlock()
	q.changed(Added, nil)
	return nil
}

// Swap exchanges two waiting tracks.
func (q *Queue) Swap(i, j int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if i < 0 || i >= len(q.tracks) || j < 0 || j >= len(q.tracks) {
		return fmt.Errorf("queue: swap %d<->%d out of range (%d tracks)", i, j, len(q.tracks))
	}
	q.tracks[i], q.tracks[j] = q.tracks[j], q.tracks[i]
	return nil
}

// Filter keeps only the waiting tracks keep returns true for.
func (q *Queue) Filter(keep func(lavalink.Track) bool) {
	q.mu.Lock()
	kept := make([]lavalink.Track, 0, len(q.tracks))
	var removed []lavalink.Track
	for _, t := range q.tracks {
		if keep(t) {
			kept = append(kept, t)
		} else {
			removed = append(removed, t)
		}
	}
	q.tracks = kept
	q.mu.Unlock()
	if len(removed) > 0 {
		q.changed(Removed, removed)
	}
}

// Find returns the index of the first waiting track match returns true for, or
// -1. Pair it with [Queue.Remove] to drop a track a user picked out.
func (q *Queue) Find(match func(lavalink.Track) bool) int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return slices.IndexFunc(q.tracks, match)
}

// Peek returns the next track without removing it.
func (q *Queue) Peek() (lavalink.Track, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if len(q.tracks) == 0 {
		return lavalink.Track{}, false
	}
	return q.tracks[0], true
}

// Advance retires the playing track and makes the next one current. It reports
// false when nothing was queued, which also clears Current.
func (q *Queue) Advance() (lavalink.Track, bool) {
	q.mu.Lock()
	q.retire()
	if len(q.tracks) == 0 {
		q.current = nil
		q.mu.Unlock()
		q.changed(Removed, nil)
		return lavalink.Track{}, false
	}
	next := q.tracks[0]
	q.tracks = slices.Delete(q.tracks, 0, 1)
	q.current = &next
	q.mu.Unlock()
	q.changed(Removed, []lavalink.Track{next})
	return next, true
}

// Back makes the last played track current again and puts the one playing back
// at the front of the queue.
func (q *Queue) Back() (lavalink.Track, bool) {
	q.mu.Lock()
	if len(q.previous) == 0 {
		q.mu.Unlock()
		return lavalink.Track{}, false
	}
	prev := q.previous[0]
	q.previous = slices.Delete(q.previous, 0, 1)
	if q.current != nil {
		q.tracks = slices.Insert(q.tracks, 0, *q.current)
	}
	q.current = &prev
	q.mu.Unlock()
	q.changed(Added, nil)
	return prev, true
}

// SetCurrent replaces the playing track without touching the history.
func (q *Queue) SetCurrent(t *lavalink.Track) {
	q.mu.Lock()
	q.current = t
	q.mu.Unlock()
	q.changed(Added, nil)
}

// retire moves the playing track into the history. Caller holds the lock.
func (q *Queue) retire() {
	if q.current == nil {
		return
	}
	q.previous = slices.Insert(q.previous, 0, *q.current)
	if len(q.previous) > q.limit {
		q.previous = q.previous[:q.limit]
	}
}

// state is what a [Store] holds per guild.
type state struct {
	Current  *lavalink.Track  `json:"current,omitempty"`
	Previous []lavalink.Track `json:"previous,omitempty"`
	Tracks   []lavalink.Track `json:"tracks,omitempty"`
}

// Save writes the queue to [Config.Store]; without one it does nothing.
func (q *Queue) Save(ctx context.Context) error {
	if q.store == nil {
		return nil
	}
	q.mu.RLock()
	data, err := json.Marshal(state{Current: q.current, Previous: q.previous, Tracks: q.tracks})
	q.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("marshal queue: %w", err)
	}
	return q.store.Set(ctx, q.guildID, data)
}

// Load replaces the queue with whatever the store holds. A guild the store
// never saw is not an error.
func (q *Queue) Load(ctx context.Context) error {
	if q.store == nil {
		return nil
	}
	data, err := q.store.Get(ctx, q.guildID)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("unmarshal queue: %w", err)
	}
	q.mu.Lock()
	q.current, q.previous, q.tracks = s.Current, s.Previous, s.Tracks
	q.mu.Unlock()
	return nil
}

// Delete drops the stored copy of the queue, leaving the queue in memory alone.
// Without a store it does nothing.
func (q *Queue) Delete(ctx context.Context) error {
	if q.store == nil {
		return nil
	}
	return q.store.Delete(ctx, q.guildID)
}

// changed persists the queue and tells [Config.OnChange]. Call it with no
// lock held.
//
// ponytail: saves on every change, blocking. Wrap the store in a debouncing one
// if that turns out to be too much write traffic.
func (q *Queue) changed(change Change, tracks []lavalink.Track) {
	if q.onChange != nil {
		q.onChange(q.guildID, change, tracks)
	}
	if q.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := q.Save(ctx); err != nil {
		q.log.Error("queue: save", slog.String("guild_id", q.guildID), slog.Any("err", err))
	}
}
