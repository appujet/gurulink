module github.com/appujet/gurulink/example

go 1.25

require (
	github.com/appujet/gurulink v0.0.0
	github.com/disgoorg/disgo v0.19.6
	github.com/disgoorg/snowflake/v2 v2.0.3
)

require (
	github.com/disgoorg/godave v0.1.0 // indirect
	github.com/disgoorg/json/v2 v2.0.0 // indirect
	github.com/disgoorg/omit v1.0.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/sasha-s/go-csync v0.0.0-20240107134140-fcbab37b09ad // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
)

// The example lives in the repo it demonstrates; drop this in your own bot.
replace github.com/appujet/gurulink => ../
