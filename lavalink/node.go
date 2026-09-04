package lavalink

import "fmt"

// Stats is a node's load report, sent every 60s and on /stats.
type Stats struct {
	Players        int         `json:"players"`
	PlayingPlayers int         `json:"playingPlayers"`
	Uptime         Duration    `json:"uptime"`
	Memory         Memory      `json:"memory"`
	CPU            CPU         `json:"cpu"`
	FrameStats     *FrameStats `json:"frameStats"`
}

type Memory struct {
	Free       int64 `json:"free"`
	Used       int64 `json:"used"`
	Allocated  int64 `json:"allocated"`
	Reservable int64 `json:"reservable"`
}

type CPU struct {
	Cores        int     `json:"cores"`
	SystemLoad   float64 `json:"systemLoad"`
	LavalinkLoad float64 `json:"lavalinkLoad"`
}

type FrameStats struct {
	Sent    int `json:"sent"`
	Nulled  int `json:"nulled"`
	Deficit int `json:"deficit"`
}

// Info is a node's /info response.
type Info struct {
	Version        Version      `json:"version"`
	BuildTime      Timestamp    `json:"buildTime"`
	Git            Git          `json:"git"`
	JVM            string       `json:"jvm"`
	Lavaplayer     string       `json:"lavaplayer"`
	SourceManagers []string     `json:"sourceManagers"`
	Filters        []string     `json:"filters"`
	Plugins        []PluginInfo `json:"plugins"`
}

type Version struct {
	Semver     string `json:"semver"`
	Major      int    `json:"major"`
	Minor      int    `json:"minor"`
	Patch      int    `json:"patch"`
	PreRelease string `json:"preRelease,omitempty"`
}

type Git struct {
	Branch     string    `json:"branch"`
	Commit     string    `json:"commit"`
	CommitTime Timestamp `json:"commitTime"`
}

type PluginInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Session is a node session's resuming state.
type Session struct {
	Resuming bool `json:"resuming"`
	Timeout  int  `json:"timeout"`
}

// SessionUpdate patches a session. Nil fields are left untouched.
type SessionUpdate struct {
	Resuming *bool `json:"resuming,omitzero"`
	Timeout  *int  `json:"timeout,omitzero"`
}

// RESTError is a non-2xx response from a node.
type RESTError struct {
	Timestamp   Timestamp `json:"timestamp"`
	Status      int       `json:"status"`
	StatusError string    `json:"error"`
	Trace       string    `json:"trace,omitempty"`
	Message     string    `json:"message"`
	Path        string    `json:"path"`
}

func (e RESTError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s: %d %s: %s", e.Path, e.Status, e.StatusError, e.Message)
	}
	return fmt.Sprintf("%s: %d %s", e.Path, e.Status, e.StatusError)
}
