package lavalink

import (
	"encoding/json"
	"fmt"
)

// Filters is the audio filter chain a node applies to a player. A nil field is
// left out of the payload, which turns that filter off on the node; send a
// whole zero Filters to clear everything.
type Filters struct {
	Volume     *float32    `json:"volume,omitzero"`
	Equalizer  *Equalizer  `json:"equalizer,omitzero"`
	Karaoke    *Karaoke    `json:"karaoke,omitzero"`
	Timescale  *Timescale  `json:"timescale,omitzero"`
	Tremolo    *Oscillator `json:"tremolo,omitzero"`
	Vibrato    *Oscillator `json:"vibrato,omitzero"`
	Rotation   *Rotation   `json:"rotation,omitzero"`
	Distortion *Distortion `json:"distortion,omitzero"`
	ChannelMix *ChannelMix `json:"channelMix,omitzero"`
	LowPass    *LowPass    `json:"lowPass,omitzero"`

	// PluginFilters holds filters a node plugin registered, keyed by the name
	// the plugin uses. See the Plugin* constants for the keys this package
	// knows; values are marshalled as given, so any plugin works.
	PluginFilters map[string]any `json:"pluginFilters,omitempty"`
}

// Active reports whether any filter is set, that is, whether the node is doing
// DSP for this player. Seeking with filters on needs the node nudged twice, so
// this is what that workaround keys off.
func (f Filters) Active() bool {
	return f.Volume != nil || f.Equalizer != nil || f.Karaoke != nil ||
		f.Timescale != nil || f.Tremolo != nil || f.Vibrato != nil ||
		f.Rotation != nil || f.Distortion != nil || f.ChannelMix != nil ||
		f.LowPass != nil || len(f.PluginFilters) > 0
}

// Equalizer is the gain of each of the 15 bands, -0.25 (mute) to 1.0 (double
// volume). Band 0 is 25Hz, band 14 is 20kHz.
type Equalizer [15]float32

type eqBand struct {
	Band int     `json:"band"`
	Gain float32 `json:"gain"`
}

func (e Equalizer) MarshalJSON() ([]byte, error) {
	bands := make([]eqBand, len(e))
	for band, gain := range e {
		bands[band] = eqBand{Band: band, Gain: gain}
	}
	return json.Marshal(bands)
}

func (e *Equalizer) UnmarshalJSON(data []byte) error {
	var bands []eqBand
	if err := json.Unmarshal(data, &bands); err != nil {
		return err
	}
	*e = Equalizer{}
	for _, b := range bands {
		if b.Band < 0 || b.Band >= len(e) {
			return fmt.Errorf("equalizer band %d out of range", b.Band)
		}
		e[b.Band] = b.Gain
	}
	return nil
}

// Karaoke tries to remove the center channel, where vocals usually sit.
type Karaoke struct {
	Level       float32 `json:"level,omitzero"`
	MonoLevel   float32 `json:"monoLevel,omitzero"`
	FilterBand  float32 `json:"filterBand,omitzero"`
	FilterWidth float32 `json:"filterWidth,omitzero"`
}

// Timescale changes speed, pitch and rate independently. 1.0 is untouched;
// see [Nightcore] and [Vaporwave] for the two everyone asks for.
type Timescale struct {
	Speed float32 `json:"speed,omitzero"`
	Pitch float32 `json:"pitch,omitzero"`
	Rate  float32 `json:"rate,omitzero"`
}

// Oscillator is the shape of both tremolo (volume wobble) and vibrato (pitch
// wobble): Frequency in Hz, Depth from 0 to 1.
type Oscillator struct {
	Frequency float32 `json:"frequency,omitzero"`
	Depth     float32 `json:"depth,omitzero"`
}

// Rotation pans the audio around the listener, the "8D audio" effect.
type Rotation struct {
	RotationHz float32 `json:"rotationHz,omitzero"`
}

// Distortion applies trigonometric waveshaping.
type Distortion struct {
	SinOffset float32 `json:"sinOffset,omitzero"`
	SinScale  float32 `json:"sinScale,omitzero"`
	CosOffset float32 `json:"cosOffset,omitzero"`
	CosScale  float32 `json:"cosScale,omitzero"`
	TanOffset float32 `json:"tanOffset,omitzero"`
	TanScale  float32 `json:"tanScale,omitzero"`
	Offset    float32 `json:"offset,omitzero"`
	Scale     float32 `json:"scale,omitzero"`
}

// ChannelMix remixes the stereo channels into each other. Zero is meaningful
// here, so every field is always sent; use one of the Output* presets.
type ChannelMix struct {
	LeftToLeft   float32 `json:"leftToLeft"`
	LeftToRight  float32 `json:"leftToRight"`
	RightToLeft  float32 `json:"rightToLeft"`
	RightToRight float32 `json:"rightToRight"`
}

// LowPass suppresses higher frequencies. Smoothing of 1.0 or below is a no-op.
type LowPass struct {
	Smoothing float32 `json:"smoothing,omitzero"`
}

// Keys for [Filters.PluginFilters], with the value type each one expects.
const (
	// PluginLavaFilter is lavalink-filter-plugin; value [LavaFilter].
	PluginLavaFilter = "lavalink-filter-plugin"
	// PluginHighPass is LavaDSPX high-pass; value [PassFilter].
	PluginHighPass = "high-pass"
	// PluginLowPass is LavaDSPX low-pass; value [PassFilter]. Not the same as
	// the stock [LowPass] filter.
	PluginLowPass = "low-pass"
	// PluginNormalization is LavaDSPX normalization; value [Normalization].
	PluginNormalization = "normalization"
	// PluginEcho is LavaDSPX echo; value [DSPXEcho].
	PluginEcho = "echo"
)

// LavaFilter is the lavalink-filter-plugin payload.
type LavaFilter struct {
	Echo   *Echo   `json:"echo,omitzero"`
	Reverb *Reverb `json:"reverb,omitzero"`
}

// Echo repeats the audio; Delay is in seconds, Decay from 0 to 1.
type Echo struct {
	Delay float32 `json:"delay,omitzero"`
	Decay float32 `json:"decay,omitzero"`
}

// Reverb needs one gain per delay.
type Reverb struct {
	Delays []float32 `json:"delays,omitempty"`
	Gains  []float32 `json:"gains,omitempty"`
}

// PassFilter is the LavaDSPX high-pass and low-pass payload. BoostFactor
// changes the output volume, 1.0 leaves it alone.
type PassFilter struct {
	CutoffFrequency int     `json:"cutoffFrequency,omitzero"`
	BoostFactor     float32 `json:"boostFactor,omitzero"`
}

// Normalization attenuates peaks above MaxAmplitude (0 mutes, 1 is untouched).
type Normalization struct {
	MaxAmplitude float32 `json:"maxAmplitude,omitzero"`
	Adaptive     bool    `json:"adaptive,omitzero"`
}

// DSPXEcho is the LavaDSPX echo payload; EchoLength is in seconds and Decay of
// 1.0 means no decay. [Echo] is the lavalink-filter-plugin one.
type DSPXEcho struct {
	EchoLength float32 `json:"echoLength,omitzero"`
	Decay      float32 `json:"decay,omitzero"`
}

// Timescale presets.
var (
	// Nightcore speeds the track up and pitches it higher.
	Nightcore = Timescale{Speed: 1.2899995, Pitch: 1.2899995, Rate: 0.93659995}
	// Vaporwave slows the track down and pitches it lower.
	Vaporwave = Timescale{Speed: 0.85, Pitch: 0.8, Rate: 1}
)

// ChannelMix presets. Left and Right fold that channel into both outputs.
var (
	OutputStereo = ChannelMix{LeftToLeft: 1, RightToRight: 1}
	OutputMono   = ChannelMix{LeftToLeft: 0.5, LeftToRight: 0.5, RightToLeft: 0.5, RightToRight: 0.5}
	OutputLeft   = ChannelMix{LeftToLeft: 1, RightToLeft: 1}
	OutputRight  = ChannelMix{LeftToRight: 1, RightToRight: 1}
)

// Equalizer presets, ported from lavalink-client's EQList (MIT; see NOTICE).
var (
	EQBassboostEarrape = Equalizer{0.225, 0.25125, 0.25125, 0.15, -0.1875, 0.05625, -0.16875, 0.08625, 0.13125, 0.16875, 0.20625, -0.225, 0.20625, -0.1875, -0.28125}
	EQBassboostHigh    = Equalizer{0.15, 0.1675, 0.1675, 0.1, -0.125, 0.0375, -0.1125, 0.0575, 0.0875, 0.1125, 0.1375, -0.15, 0.1375, -0.125, -0.1875}
	EQBassboostMedium  = Equalizer{0.1125, 0.125625, 0.125625, 0.075, -0.09375, 0.028125, -0.084375, 0.043125, 0.065625, 0.084375, 0.103125, -0.1125, 0.103125, -0.09375, -0.140625}
	EQBassboostLow     = Equalizer{0.075, 0.08375, 0.08375, 0.05, -0.0625, 0.01875, -0.05625, 0.02875, 0.04375, 0.05625, 0.06875, -0.075, 0.06875, -0.0625, -0.09375}
	EQBetterMusic      = Equalizer{0.25, 0.025, 0.0125, 0, 0, -0.0125, -0.025, -0.0175, 0, 0, 0.0125, 0.025, 0.25, 0.125, 0.125}
	EQFullSound        = Equalizer{0.625, 0.275, 0.2625, 0.25, 0.25, 0.2375, 0.225, 0.2325, 0.25, 0.25, 0.2625, 0.275, 0.625, 0.375, 0.375}
	EQRock             = Equalizer{0.3, 0.25, 0.2, 0.1, 0.05, -0.05, -0.15, -0.2, -0.1, -0.05, 0.05, 0.1, 0.2, 0.25, 0.3}
	EQClassic          = Equalizer{0.375, 0.35, 0.125, 0, 0, 0.125, 0.55, 0.05, 0.125, 0.25, 0.2, 0.25, 0.3, 0.25, 0.3}
	EQPop              = Equalizer{0.2635, 0.22141, -0.21141, -0.1851, -0.155, 0.21141, 0.22456, 0.237, 0.237, 0.237, -0.05, -0.116, 0.192}
	EQElectronic       = Equalizer{0.375, 0.35, 0.125, 0, 0, -0.125, -0.125, 0, 0.25, 0.125, 0.15, 0.2, 0.25, 0.35, 0.4}
	EQGaming           = Equalizer{0.35, 0.3, 0.25, 0.2, 0.15, 0.1, 0.05, 0, -0.05, -0.1, -0.15, -0.2, -0.25, -0.3, -0.35}
)
