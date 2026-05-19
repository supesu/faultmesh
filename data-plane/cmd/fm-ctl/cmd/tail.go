package cmd

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/supesu/faultmesh/data-plane/internal/grpcdial"
	controlv1 "github.com/supesu/faultmesh/data-plane/pkg/genproto/faultmesh/v1"
	"github.com/supesu/faultmesh/data-plane/pkg/signals"
)

func newTailCmd(g *GlobalFlags, logger zerolog.Logger) *cobra.Command {
	tail := &cobra.Command{
		Use:   "tail",
		Short: "Tail server-streamed control-plane resources",
	}
	tail.AddCommand(newTailEventsCmd(g, logger))
	return tail
}

func newTailEventsCmd(g *GlobalFlags, logger zerolog.Logger) *cobra.Command {
	var (
		tenant  string
		agentID string
	)
	c := &cobra.Command{
		Use:   "events",
		Short: "Stream normalized events from DebugService.TailEvents as JSON lines",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := validateAddr(g.ControlAddr); err != nil {
				return err
			}

			ctx, stop := signals.Context()
			defer stop()

			var dialOpts []grpc.DialOption
			var closer func() error
			switch {
			case g.SPIFFESocket != "" && g.ServerSPIFFEID != "":
				creds, cl, err := grpcdial.WorkloadAPICreds(ctx, g.SPIFFESocket, g.ServerSPIFFEID)
				if err != nil {
					return fmt.Errorf("workload API creds: %w", err)
				}
				dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
				closer = cl
			case g.TLSCert != "":
				creds, err := grpcdial.LoadFileCreds(g.TLSCert, g.TLSKey, g.TLSCA, g.TLSServerName)
				if err != nil {
					return fmt.Errorf("file mTLS creds: %w", err)
				}
				dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
			default:
				return errors.New("either --spiffe-socket + --server-spiffe-id or --tls-cert + --tls-key + --tls-ca are required")
			}

			conn, err := grpc.NewClient(g.ControlAddr, dialOpts...)
			if err != nil {
				return fmt.Errorf("dial %s: %w", g.ControlAddr, err)
			}
			defer conn.Close()
			if closer != nil {
				defer func() { _ = closer() }()
			}

			client := controlv1.NewDebugServiceClient(conn)
			stream, err := client.TailEvents(ctx, &controlv1.TailRequest{
				Tenant:  tenant,
				AgentId: agentID,
			})
			if err != nil {
				return fmt.Errorf("TailEvents: %w", err)
			}

			logger.Info().
				Str("addr", g.ControlAddr).
				Str("tenant", tenant).
				Str("agent_id", agentID).
				Msg("streaming events")

			marshaler := protojson.MarshalOptions{UseProtoNames: true}
			for {
				ev, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					if ctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("recv: %w", err)
				}
				b, mErr := marshaler.Marshal(ev)
				if mErr != nil {
					logger.Warn().Err(mErr).Msg("skip event: marshal failed")
					continue
				}
				if _, wErr := fmt.Fprintln(os.Stdout, string(b)); wErr != nil {
					return wErr
				}
			}
		},
	}
	c.Flags().StringVar(&tenant, "tenant", "", "tenant filter (empty matches everything)")
	c.Flags().StringVar(&agentID, "agent-id", "", "agent ID filter (empty matches everything)")
	return c
}

func validateAddr(addr string) error {
	if addr == "" {
		return errors.New("--control-addr is required")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("--control-addr must be host:port: %w", err)
	}
	if host == "" {
		return errors.New("--control-addr host is empty")
	}
	if p, perr := strconv.Atoi(port); perr != nil || p <= 0 || p > 65535 {
		return errors.New("--control-addr port must be in 1..65535")
	}
	return nil
}
