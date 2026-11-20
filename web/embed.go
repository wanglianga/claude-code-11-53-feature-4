package web

import (
	"embed"
	"io/fs"
	"net/http"
)

// Assets exposes the vendored Swagger UI distribution. It is embedded into the
// binary so the container runs without network access to a CDN.
//
//go:embed swagger-ui
var assets embed.FS

func Assets() http.FileSystem {
	sub, err := fs.Sub(assets, "swagger-ui")
	if err != nil {
		panic(err)
	}
	return http.FS(sub)
}
