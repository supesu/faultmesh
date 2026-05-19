package main

import (
	"github.com/supesu/faultmesh/data-plane/pkg/logging"
	"github.com/supesu/faultmesh/data-plane/pkg/signals"
	"github.com/supesu/faultmesh/data-plane/pkg/version"
)

func main() {
	logger := logging.New()

	logger.Info().
		Str("Version", version.Version).
		Str("GitCommit", version.GitCommit).
		Str("Built", version.BuildDate).
		Msg("fm-operator")

	// pause unti lshutdown
	ctx, stop := signals.Context()
	defer stop()
	<-ctx.Done()
}
