package lavalink

import (
	"encoding/json"
	"testing"
)

// TestNullable pins the tri-state the wire format needs: an untouched field is
// absent, Null clears it on the node, Value sets it.
func TestNullable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		update PlayerUpdate
		want   string
	}{
		{"absent", PlayerUpdate{}, `{}`},
		{"null track", PlayerUpdate{Track: &UpdateTrack{Encoded: Null[string]()}}, `{"track":{"encoded":null}}`},
		{"track", PlayerUpdate{Track: &UpdateTrack{Encoded: Value("abc")}}, `{"track":{"encoded":"abc"}}`},
		{"clear next", PlayerUpdate{NextTrack: Null[UpdateTrack]()}, `{"nextTrack":null}`},
	} {
		data, err := json.Marshal(tc.update)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(data) != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, data, tc.want)
		}
	}

	var update PlayerUpdate
	if err := json.Unmarshal([]byte(`{"nextTrack":null}`), &update); err != nil {
		t.Fatal(err)
	}
	if update.NextTrack.IsZero() {
		t.Error("decoded null nextTrack should be present")
	}
	if _, ok := update.NextTrack.Get(); ok {
		t.Error("decoded null nextTrack should hold no value")
	}
}

// TestEqualizer checks the band/gain shape a node expects, since the Go side is
// a plain array.
func TestEqualizer(t *testing.T) {
	data, err := json.Marshal(Filters{Equalizer: &Equalizer{0: 0.25, 14: -0.25}})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"equalizer":[{"band":0,"gain":0.25},{"band":1,"gain":0}`
	if string(data[:len(want)]) != want {
		t.Errorf("got %s", data)
	}

	var back Filters
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if *back.Equalizer != (Equalizer{0: 0.25, 14: -0.25}) {
		t.Errorf("round trip lost bands: %v", *back.Equalizer)
	}
	if err := json.Unmarshal([]byte(`[{"band":99,"gain":1}]`), &back.Equalizer); err == nil {
		t.Error("band 99 should be rejected")
	}
	if (Filters{}).Active() {
		t.Error("empty filters are not active")
	}
	if !back.Active() {
		t.Error("filters with an equalizer are active")
	}
}

// TestLoadResult covers all five load types, since each one puts its payload in
// a different field.
func TestLoadResult(t *testing.T) {
	for _, tc := range []struct {
		body   string
		want   LoadType
		tracks int
	}{
		{`{"loadType":"track","data":{"encoded":"a","info":{}}}`, LoadTypeTrack, 1},
		{`{"loadType":"playlist","data":{"info":{"name":"p"},"tracks":[{"encoded":"a"},{"encoded":"b"}]}}`, LoadTypePlaylist, 2},
		{`{"loadType":"search","data":[{"encoded":"a"},{"encoded":"b"},{"encoded":"c"}]}`, LoadTypeSearch, 3},
		{`{"loadType":"empty","data":{}}`, LoadTypeEmpty, 0},
		{`{"loadType":"error","data":{"message":"nope","severity":"fault"}}`, LoadTypeError, 0},
	} {
		var result LoadResult
		if err := json.Unmarshal([]byte(tc.body), &result); err != nil {
			t.Fatalf("%s: %v", tc.want, err)
		}
		if result.LoadType != tc.want {
			t.Errorf("got load type %s, want %s", result.LoadType, tc.want)
		}
		if got := len(result.AllTracks()); got != tc.tracks {
			t.Errorf("%s: got %d tracks, want %d", tc.want, got, tc.tracks)
		}
	}

	var result LoadResult
	if err := json.Unmarshal([]byte(`{"loadType":"error","data":{"message":"nope","severity":"fault"}}`), &result); err != nil {
		t.Fatal(err)
	}
	if result.Exception.Error() != "fault: nope" {
		t.Errorf("got %q", result.Exception.Error())
	}
	if err := json.Unmarshal([]byte(`{"loadType":"whatever"}`), &result); err == nil {
		t.Error("unknown load type should be rejected")
	}
}

// A node rejects the whole player update when any of the four voice fields is missing, so an
// incomplete state must never be sent — channel id included.
func TestVoiceStateComplete(t *testing.T) {
	full := VoiceState{Token: "t", Endpoint: "e", SessionID: "s", ChannelID: "c"}
	if !full.Complete() {
		t.Error("a full voice state is not complete")
	}
	for name, state := range map[string]VoiceState{
		"no token":      {Endpoint: "e", SessionID: "s", ChannelID: "c"},
		"no endpoint":   {Token: "t", SessionID: "s", ChannelID: "c"},
		"no session id": {Token: "t", Endpoint: "e", ChannelID: "c"},
		"no channel id": {Token: "t", Endpoint: "e", SessionID: "s"},
	} {
		if state.Complete() {
			t.Errorf("%s: reported complete", name)
		}
	}

	raw, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"token":"t","endpoint":"e","sessionId":"s","channelId":"c"}`; string(raw) != want {
		t.Errorf("got %s, want %s", raw, want)
	}
}
