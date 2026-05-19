package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// zap would work fine too; choosing zerolog mainly because i personally prefer syntax.
// dont need da sugar styel logging here either
func New() zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339

	return zerolog.New(os.Stdout).
		With().
		Timestamp().
		Logger()
}
