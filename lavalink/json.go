package lavalink

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Duration is a millisecond count, the unit Lavalink speaks on the wire.
type Duration int64

const (
	Millisecond Duration = 1
	Second      Duration = 1000 * Millisecond
	Minute      Duration = 60 * Second
	Hour        Duration = 60 * Minute
)

// Milliseconds returns d as a plain millisecond count.
func (d Duration) Milliseconds() int64 { return int64(d) }

// Std converts d to a [time.Duration].
func (d Duration) Std() time.Duration { return time.Duration(d) * time.Millisecond }

func (d Duration) String() string { return d.Std().String() }

// Timestamp is a unix-millisecond timestamp.
type Timestamp struct{ time.Time }

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return strconv.AppendInt(nil, t.UnixMilli(), 10), nil
}

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
	ms, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp %q: %w", data, err)
	}
	t.Time = time.UnixMilli(ms)
	return nil
}

// Nullable tells an absent field ("leave it alone") from a null one ("clear it").
// Tag the field `json:",omitzero"` so the zero value is left out.
type Nullable[T any] struct {
	set bool
	val *T
}

// Value returns a Nullable carrying v.
func Value[T any](v T) Nullable[T] { return Nullable[T]{set: true, val: &v} }

// Null returns a Nullable that marshals to JSON null.
func Null[T any]() Nullable[T] { return Nullable[T]{set: true} }

// Get returns the value and whether it is present and non-null.
func (n Nullable[T]) Get() (T, bool) {
	if n.val == nil {
		var zero T
		return zero, false
	}
	return *n.val, true
}

// IsZero is encoding/json's omitzero hook.
func (n Nullable[T]) IsZero() bool { return !n.set }

func (n Nullable[T]) MarshalJSON() ([]byte, error) { return json.Marshal(n.val) }

func (n *Nullable[T]) UnmarshalJSON(data []byte) error {
	n.set = true
	if string(data) == "null" {
		n.val = nil
		return nil
	}
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	n.val = &v
	return nil
}
