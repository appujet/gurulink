package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFile(t *testing.T) {
	ctx := context.Background()
	f := File{Dir: filepath.Join(t.TempDir(), "queues")}

	if data, err := f.Get(ctx, "123"); err != nil || data != nil {
		t.Errorf("a guild with no file should read as nothing: %q, %v", data, err)
	}
	if err := f.Set(ctx, "123", []byte(`{"tracks":[]}`)); err != nil {
		t.Fatal(err)
	}
	data, err := f.Get(ctx, "123")
	if err != nil || string(data) != `{"tracks":[]}` {
		t.Fatalf("got %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(f.Dir, "123.json.tmp")); err == nil {
		t.Error("the temp file should be renamed away")
	}
	if err := f.Delete(ctx, "123"); err != nil {
		t.Fatal(err)
	}
	if err := f.Delete(ctx, "123"); err != nil {
		t.Errorf("deleting twice is not an error: %v", err)
	}
}

// TestFileRejectsPaths is the trust boundary: guild ids come from Discord, so a
// crafted one must not reach outside Dir.
func TestFileRejectsPaths(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	f := File{Dir: filepath.Join(dir, "queues")}

	for _, id := range []string{"", "..", "../escaped", "/etc/passwd", "12/34", "a123", "12 3"} {
		if err := f.Set(ctx, id, []byte("x")); err == nil {
			t.Errorf("Set(%q) should be rejected", id)
		}
		if _, err := f.Get(ctx, id); err == nil {
			t.Errorf("Get(%q) should be rejected", id)
		}
		if err := f.Delete(ctx, id); err == nil {
			t.Errorf("Delete(%q) should be rejected", id)
		}
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Errorf("nothing should have been written: %v, %v", entries, err)
	}
}

func TestMemory(t *testing.T) {
	ctx := context.Background()
	var m Memory

	if data, err := m.Get(ctx, "123"); err != nil || data != nil {
		t.Errorf("an unknown guild reads as nothing: %q, %v", data, err)
	}
	if err := m.Set(ctx, "123", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if data, _ := m.Get(ctx, "123"); string(data) != "x" {
		t.Errorf("got %q, want x", data)
	}
	if err := m.Delete(ctx, "123"); err != nil {
		t.Fatal(err)
	}
	if data, _ := m.Get(ctx, "123"); data != nil {
		t.Errorf("deleted guild still reads %q", data)
	}
}
