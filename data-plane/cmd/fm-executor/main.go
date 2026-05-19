package main

import (
	"github.com/supesu/faultmesh/data-plane/pkg/logging"
	"github.com/supesu/faultmesh/data-plane/pkg/signals"
	"github.com/supesu/faultmesh/data-plane/pkg/version"
)

func main() {
	logger := logging.New()

	logger.Info().
		Str("Verison", version.Version).
		Str("Git Commit", version.GitCommit).
		Str("Built", version.BuildDate).
		Msg("fm-ctl")

	// pause until shutdown.
	ctx, stop := signals.Context()
	defer stop()
	<-ctx.Done()
}
