package cmd

import (
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	"github.com/supesu/faultmesh/data-plane/pkg/version"
)

type GlobalFlags struct {
	ControlAddr    string
	TLSCert        string
	TLSKey         string
	TLSCA          string
	TLSServerName  string
	SPIFFESocket   string
	ServerSPIFFEID string
}

func NewRoot(logger zerolog.Logger) *cobra.Command {
	g := &GlobalFlags{}
	root := &cobra.Command{
		Use:           "fm-ctl",
		Short:         "faultmesh control-plane CLI",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: false,
	}

	root.PersistentFlags().StringVar(&g.ControlAddr, "control-addr", "", "control plane address (host:port)")
	root.PersistentFlags().StringVar(&g.TLSCert, "tls-cert", "", "client SVID cert path (file mode mTLS)")
	root.PersistentFlags().StringVar(&g.TLSKey, "tls-key", "", "client SVID key path")
	root.PersistentFlags().StringVar(&g.TLSCA, "tls-ca", "", "trust bundle CA path")
	root.PersistentFlags().StringVar(&g.TLSServerName, "tls-server-name", "", "expected control-plane DNS name in server cert")
	root.PersistentFlags().StringVar(&g.SPIFFESocket, "spiffe-socket", "", "SPIFFE workload API socket path (enables rotating SVIDs)")
	root.PersistentFlags().StringVar(&g.ServerSPIFFEID, "server-spiffe-id", "", "expected control-plane SPIFFE ID")

	root.AddCommand(newTailCmd(g, logger))
	return root
}
