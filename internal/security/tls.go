package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

func LoadServerTLS(certFile, keyFile, clientCAFile string, requireClientCertificate bool) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, errors.New("server certificate and key are required")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}
	if clientCAFile != "" {
		pem, err := os.ReadFile(clientCAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("client CA file contains no valid certificates")
		}
		config.ClientCAs = pool
		config.ClientAuth = tls.VerifyClientCertIfGiven
	}
	if requireClientCertificate {
		if config.ClientCAs == nil {
			return nil, errors.New("mutual TLS requires a client CA file")
		}
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return config, nil
}

func LoadClientTLS(caFile, serverName string) (*tls.Config, error) {
	return LoadClientTLSWithCertificate(caFile, serverName, "", "")
}

func LoadClientTLSWithCertificate(caFile, serverName, clientCertFile, clientKeyFile string) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if (clientCertFile == "") != (clientKeyFile == "") {
		return nil, errors.New("client certificate and key must be configured together")
	}
	if clientCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	if caFile == "" {
		return config, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("server CA file contains no valid certificates")
	}
	config.RootCAs = pool
	return config, nil
}
