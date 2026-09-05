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
	ErrSpeakQueryTooLong = errors.New("gurulink: speak queries are limited to 100 characters")
)

// IsLink reports whether q is an http(s) URL rather than search terms.
func IsLink(q string) bool {
	q = strings.ToLower(strings.TrimSpace(q))
	return strings.HasPrefix(q, "http://") || strings.HasPrefix(q, "https://")
}

// Source resolves a shorthand such as "yt" or "spotify" to a node's search
// prefix, false for anything unknown.
func Source(name string) (string, bool) {
	src, ok := sourceAliases[strings.ToLower(strings.TrimSpace(name))]
	return src, ok
}

// SourceOf is the prefix a query carries, as in "yt:never gonna", else "".
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

// identifier turns a query into a /loadtracks identifier. A URL passes through;
// otherwise the query's own prefix wins, then source, then the default.
func (c *Client) identifier(query, source string) (string, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return "", ErrEmptyQuery
	}
	if IsLink(query) {
		return query, nil
	}

	src, terms := c.cfg.DefaultSource, query
	// The query's own prefix is an alias too: a node only knows "ytsearch:x".
	if own := SourceOf(query); own != "" {
		_, terms, _ = strings.Cut(query, ":")
		src, terms = own, strings.TrimSpace(terms)
	} else if source != "" {
		resolved, ok := Source(source)
		if !ok {
			return "", fmt.Errorf("gurulink: unknown search source %q", source)
		}
		src = resolved
	}
	if src == "speak" && len(terms) > 100 {
		return "", ErrSpeakQueryTooLong
	}
	return src + ":" + terms, nil
}

// sourceAliases maps every shorthand lavalink-client accepts (MIT; see NOTICE) to
// a node's search prefix. A prefix maps to itself, so either works.
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
