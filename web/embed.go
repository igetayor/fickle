package web

import "embed"

// FS embeds the static web assets for zero-dependency distribution.
//
//go:embed index.html style.css app.js
var FS embed.FS
