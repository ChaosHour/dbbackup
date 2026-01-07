// Package installer provides systemd service installation for dbbackup
package installer

import (
	"embed"
)

// Templates contains embedded systemd unit files
//
//go:embed templates/*.service templates/*.timer
var Templates embed.FS
