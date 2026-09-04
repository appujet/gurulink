package gurulink

import (
	"errors"
	"fmt"
	"strings"
)

// DefaultSource is the search prefix used for queries that name none.
const DefaultSource = "ytmsearch"

// Query rejection reasons, all reported by [Client.Search].
var (
	ErrEmptyQuery        = errors.New("gurulink: empty query")
	ErrLinksDisallowed   = errors.New("gurulink: link queries are disallowed")
	ErrLinkBlocked       = errors.New("gurulink: query matches a blocked link")
	ErrLinkNotAllowed    = errors.New("gurulink: query matches no allowed link")
	ErrSpeakQueryTooLong = errors.New("gurulink: speak queries are limited to 100 characters")
)

// IsLink reports whether q is an http(s) URL rather than search terms.
func IsLink(q string) bool {
	q = strings.ToLower(strings.TrimSpace(q))
	return strings.HasPrefix(q, "http://") || strings.HasPrefix(q, "https://")
}

// Source resolves a shorthand such as "yt", "spotify" or "ytmsearch" to the
// search prefix a node expects. It reports false for anything unknown.
func Source(name string) (string, bool) {
	src, ok := sourceAliases[strings.ToLower(strings.TrimSpace(name))]
	return src, ok
}

// SourceOf returns the prefix a query already carries, as in
// "ytsearch:never gonna", or "" when it carries none.
func SourceOf(query string) string {
	prefix, _, ok := strings.Cut(query, ":")
	if !ok {
		return ""
	}
	src, known := Source(prefix)
	if !known {
		return ""
	}
	return src
}

// identifier turns a user query into a /loadtracks identifier and checks it
// against the client's link rules. A URL is passed through untouched, anything
// else gets the source prefix it asked for, or the configured default.
func (c *Client) identifier(query, source string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", ErrEmptyQuery
	}
	if err := c.checkLinks(query); err != nil {
		return "", err
	}
	if IsLink(query) {
		return query, nil
	}
	if SourceOf(query) != "" {
		return query, nil
	}
	src := c.cfg.DefaultSource
	if source != "" {
		resolved, ok := Source(source)
		if !ok {
			return "", fmt.Errorf("gurulink: unknown search source %q", source)
		}
		src = resolved
	}
	if src == "speak" && len(query) > 100 {
		return "", ErrSpeakQueryTooLong
	}
	return src + ":" + query, nil
}

// checkLinks applies the blocked/allowed link rules. Queries are user input, so
// this runs before anything reaches a node.
func (c *Client) checkLinks(query string) error {
	if containsAny(query, c.cfg.BlockedLinks) {
		return ErrLinkBlocked
	}
	if !IsLink(query) {
		return nil
	}
	if c.cfg.DisallowLinks {
		return ErrLinksDisallowed
	}
	if len(c.cfg.AllowedLinks) > 0 && !containsAny(query, c.cfg.AllowedLinks) {
		return ErrLinkNotAllowed
	}
	return nil
}

// containsAny reports whether s contains any of subs, ignoring case.
//
// ponytail: substring match, not regexp. Add a regexp variant of the config
// fields if plain domains and words stop being enough.
func containsAny(s string, subs []string) bool {
	s = strings.ToLower(s)
	for _, sub := range subs {
		if sub != "" && strings.Contains(s, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

// sourceAliases maps every shorthand lavalink-client accepts (MIT; see NOTICE)
// to the search prefix a node understands. A prefix always maps to itself, so
// users can pass either. Whether a node actually has the source is the node's
// problem.
var sourceAliases = map[string]string{
	"ytmsearch":    "ytmsearch",
	"ytm":          "ytmsearch",
	"youtubemusic": "ytmsearch",
	"musicyoutube": "ytmsearch",
	"ytsearch":     "ytsearch",
	"yt":           "ytsearch",
	"youtube":      "ytsearch",
	"scsearch":     "scsearch",
	"sc":           "scsearch",
	"soundcloud":   "scsearch",
	"amsearch":     "amsearch",
	"am":           "amsearch",
	"apple":        "amsearch",
	"applemusic":   "amsearch",
	"musicapple":   "amsearch",
	"spsearch":     "spsearch",
	"sp":           "spsearch",
	"spotify":      "spsearch",
	"spotify.com":  "spsearch",
	"spotifycom":   "spsearch",
	"sprec":        "sprec",
	"spsuggestion": "sprec",
	"dzsearch":     "dzsearch",
	"dz":           "dzsearch",
	"deezer":       "dzsearch",
	"dzisrc":       "dzisrc",
	"dzrec":        "dzrec",
	"ymsearch":     "ymsearch",
	"yandex":       "ymsearch",
	"yandexmusic":  "ymsearch",
	"ymrec":        "ymrec",
	"vksearch":     "vksearch",
	"vk":           "vksearch",
	"vkmusic":      "vksearch",
	"vkrec":        "vkrec",
	"qbsearch":     "qbsearch",
	"qobuz":        "qbsearch",
	"qbisrc":       "qbisrc",
	"qbrec":        "qbrec",
	"pdsearch":     "pdsearch",
	"pd":           "pdsearch",
	"pandora":      "pdsearch",
	"pandoramusic": "pdsearch",
	"tdsearch":     "tdsearch",
	"td":           "tdsearch",
	"tidal":        "tdsearch",
	"music":        "tdsearch",
	"tdrec":        "tdrec",
	"jssearch":     "jssearch",
	"js":           "jssearch",
	"jiosaavn":     "jssearch",
	"jsrec":        "jsrec",
	"bcsearch":     "bcsearch",
	"bc":           "bcsearch",
	"bandcamp":     "bcsearch",
	"phsearch":     "phsearch",
	"pornhub":      "phsearch",
	"porn":         "phsearch",
	"amzsearch":    "amzsearch",
	"admsearch":    "admsearch",
	"gnsearch":     "gnsearch",
	"szsearch":     "szsearch",
	"speak":        "speak",
	"tts":          "tts",
	"ftts":         "ftts",
	"flowery":      "ftts",
	"flowerytts":   "ftts",
	"flowery.tts":  "ftts",
	"local":        "local",
	"http":         "http",
	"https":        "https",
	"link":         "link",
	"uri":          "uri",
}
