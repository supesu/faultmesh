package main

import (
	"os"

	"github.com/supesu/faultmesh/data-plane/cmd/fm-ctl/cmd"
	"github.com/supesu/faultmesh/data-plane/pkg/logging"
)

func main() {
	logger := logging.New()
	if err := cmd.NewRoot(logger).Execute(); err != nil {
		os.Exit(1)
	}
}
