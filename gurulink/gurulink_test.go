package gurulink

import (
	"context"
	"errors"
	"testing"
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
// here, and the link rules have to hold.
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

func TestLinkRules(t *testing.T) {
	blocked := testClient(t, func(c *Config) { c.BlockedLinks = []string{"Rickroll", "pornhub.com"} })
	for _, query := range []string{"https://pornhub.com/x", "a rickroll please"} {
		if _, err := blocked.identifier(query, ""); !errors.Is(err, ErrLinkBlocked) {
			t.Errorf("%q should be blocked, got %v", query, err)
		}
	}
	if _, err := blocked.identifier("https://youtu.be/x", ""); err != nil {
		t.Errorf("an unblocked link should pass: %v", err)
	}

	allowed := testClient(t, func(c *Config) { c.AllowedLinks = []string{"youtube.com", "youtu.be"} })
	if _, err := allowed.identifier("https://youtu.be/x", ""); err != nil {
		t.Errorf("an allowed link should pass: %v", err)
	}
	if _, err := allowed.identifier("https://example.com/x", ""); !errors.Is(err, ErrLinkNotAllowed) {
		t.Errorf("a link outside the allow list should fail, got %v", err)
	}
	if _, err := allowed.identifier("never gonna", ""); err != nil {
		t.Errorf("the allow list is for links only: %v", err)
	}

	none := testClient(t, func(c *Config) { c.DisallowLinks = true })
	if _, err := none.identifier("https://youtu.be/x", ""); !errors.Is(err, ErrLinksDisallowed) {
		t.Errorf("links should be disallowed, got %v", err)
	}
	if _, err := none.identifier("never gonna", ""); err != nil {
		t.Errorf("searches still work: %v", err)
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
