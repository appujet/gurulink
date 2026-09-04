// Package store has ready-made queue stores. It moves bytes and nothing else,
// so it imports nothing of gurulink; anything here satisfies queue.Store
// structurally.
package store

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Memory keeps queues in a map: they survive a node reconnect, but not a
// restart. Safe for concurrent use.
type Memory struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func (m *Memory) Get(_ context.Context, guildID string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.data[guildID], nil
}

func (m *Memory) Set(_ context.Context, guildID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	m.data[guildID] = data
	return nil
}

func (m *Memory) Delete(_ context.Context, guildID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, guildID)
	return nil
}

// File keeps one file per guild under Dir, so queues outlive a restart. Writes
// are atomic; concurrent writes for one guild are the caller's problem, and
// gurulink only ever has one goroutine saving a given queue.
//
// ponytail: a file per guild, rewritten whole. Point [Memory] or your own
// database store at it if you outgrow that.
type File struct {
	Dir string
}

// guildPath is Dir/<guildID>.json, rejecting anything that is not a snowflake
// so a crafted guild id cannot escape Dir.
func (f File) guildPath(guildID string) (string, error) {
	if guildID == "" {
		return "", errors.New("store: empty guild id")
	}
	for _, r := range guildID {
		if r < '0' || r > '9' {
			return "", errors.New("store: guild id must be numeric")
		}
	}
	return filepath.Join(f.Dir, guildID+".json"), nil
}

func (f File) Get(_ context.Context, guildID string) ([]byte, error) {
	path, err := f.guildPath(guildID)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return data, err
}

func (f File) Set(_ context.Context, guildID string, data []byte) error {
	path, err := f.guildPath(guildID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(f.Dir, 0o755); err != nil {
		return err
	}
	// Write beside the target and rename, so a crash never leaves half a queue.
	temp := path + ".tmp"
	if err := os.WriteFile(temp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		_ = os.Remove(temp)
		return err
	}
	return nil
}

func (f File) Delete(_ context.Context, guildID string) error {
	path, err := f.guildPath(guildID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}
