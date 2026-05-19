package grpcdial

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"google.golang.org/grpc/credentials"
)

var ErrEmptyCA = errors.New("tls-ca file contained no PEM certificates")

func LoadFileCreds(certPath, keyPath, caPath, serverName string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, ErrEmptyCA
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   serverName,
		MinVersion:   tls.VersionTLS12,
	}), nil
}

func WorkloadAPICreds(ctx context.Context, socketPath, serverSPIFFEID string) (credentials.TransportCredentials, func() error, error) {
	if !strings.HasPrefix(socketPath, "unix://") {
		socketPath = "unix://" + socketPath
	}
	id, err := spiffeid.FromString(serverSPIFFEID)
	if err != nil {
		return nil, nil, err
	}
	source, err := workloadapi.NewX509Source(ctx,
		workloadapi.WithClientOptions(workloadapi.WithAddr(socketPath)),
	)
	if err != nil {
		return nil, nil, err
	}
	tlsCfg := tlsconfig.MTLSClientConfig(source, source, tlsconfig.AuthorizeID(id))
	return credentials.NewTLS(tlsCfg), source.Close, nil
}
