package client

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/ARIN00-web/messlessmq/internal/network"

	"github.com/ARIN00-web/messlessmq/internal/protocol"
)

func testClient(t *testing.T) *Client {
	t.Helper()
	c, err := New(Config{Bootstrap: []string{"127.0.0.1:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func testMetadata() protocol.MetadataResponse {
	return protocol.MetadataResponse{Version: 1, Brokers: []protocol.BrokerMetadata{{ID: "b1", Address: "a1"}, {ID: "b2", Address: "a2"}}, Partitions: []protocol.PartitionMetadata{{Topic: "orders", PartitionID: 0, Replicas: []string{"b1"}, Leader: "b1", LeaderEpoch: 1}, {Topic: "orders", PartitionID: 1, Replicas: []string{"b2"}, Leader: "b2", LeaderEpoch: 1}}}
}

func TestUnkeyedRoutingRoundRobinsPartitions(t *testing.T) {
	c := testClient(t)
	metadata := testMetadata()
	first, _, err := c.selectRoute(metadata, "orders", nil)
	if err != nil || first != 0 {
		t.Fatalf("first route = %d, %v", first, err)
	}
	second, _, err := c.selectRoute(metadata, "orders", nil)
	if err != nil || second != 1 {
		t.Fatalf("second route = %d, %v", second, err)
	}
}

func TestKeyedRoutingIsStable(t *testing.T) {
	c := testClient(t)
	metadata := testMetadata()
	first, address, err := c.selectRoute(metadata, "orders", []byte("customer-7"))
	if err != nil {
		t.Fatal(err)
	}
	second, secondAddress, err := c.selectRoute(metadata, "orders", []byte("customer-7"))
	if err != nil || first != second || address != secondAddress {
		t.Fatalf("unstable keyed route: %d/%s vs %d/%s (%v)", first, address, second, secondAddress, err)
	}
}

func TestResponseRetryClassificationExcludesUncertainOutcome(t *testing.T) {
	if !isStaleRoute(&ResponseError{Code: protocol.ErrorNotPartitionLeader}) {
		t.Fatal("not-leader should refresh metadata")
	}
	if isStaleRoute(&ResponseError{Code: protocol.ErrorCommitUncertain}) {
		t.Fatal("uncertain append must not be retried")
	}
}

func TestClientUsesTLSAndAuthenticatesEachConnection(t *testing.T) {
	cert, roots := testTLSCertificate(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()
		for i := 0; i < 2; i++ {
			frame, readErr := network.ReadFrame(conn)
			if readErr != nil {
				serverErr <- readErr
				return
			}
			request, decodeErr := protocol.DecodeRequest(frame)
			if decodeErr != nil {
				serverErr <- decodeErr
				return
			}
			var payload []byte
			if i == 0 {
				if request.APIKey != protocol.APIAUTH {
					serverErr <- fmt.Errorf("first request API = %d, want AUTH", request.APIKey)
					return
				}
				auth, authErr := protocol.DecodeAuthRequest(request.Payload)
				if authErr != nil || auth.Token != "test-secret" {
					serverErr <- fmt.Errorf("unexpected auth request: token=%q err=%v", auth.Token, authErr)
					return
				}
				payload, decodeErr = protocol.EncodeAuthResponse(protocol.AuthResponse{PrincipalID: "test-client"})
			} else {
				if request.APIKey != protocol.APIMETADATA {
					serverErr <- fmt.Errorf("second request API = %d, want METADATA", request.APIKey)
					return
				}
				metadataRequest, requestErr := protocol.DecodeMetadataRequest(request.Payload)
				if requestErr != nil || metadataRequest.Topic != "orders" {
					serverErr <- fmt.Errorf("metadata request should preserve topic scope: topic=%q err=%v", metadataRequest.Topic, requestErr)
					return
				}
				payload, decodeErr = protocol.EncodeMetadataResponse(protocol.MetadataResponse{Version: 7})
			}
			if decodeErr != nil {
				serverErr <- decodeErr
				return
			}
			response, encodeErr := protocol.EncodeResponse(protocol.Response{APIKey: request.APIKey, APIVersion: request.APIVersion, CorrelationID: request.CorrelationID, Payload: payload})
			if encodeErr != nil {
				serverErr <- encodeErr
				return
			}
			if writeErr := network.WriteFrame(conn, response); writeErr != nil {
				serverErr <- writeErr
				return
			}
		}
		serverErr <- nil
	}()

	client, err := New(Config{
		Bootstrap: []string{listener.Addr().String()},
		APIKey:    "test-secret",
		TLSConfig: &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := client.Metadata(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Version != 7 {
		t.Fatalf("metadata version = %d, want 7", metadata.Version)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func testTLSCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return certificate, roots
}

func TestResolveAddressWithBootstrap(t *testing.T) {
	// Loopback bootstrap and loopback advertised: untouched
	got := ResolveAddressWithBootstrap("127.0.0.1:19092", []string{"127.0.0.1:19092"}, "broker-1")
	if got != "127.0.0.1:19092" {
		t.Fatalf("got %q, want 127.0.0.1:19092", got)
	}

	// External bootstrap host rewrites loopback advertised host
	got = ResolveAddressWithBootstrap("127.0.0.1:19093", []string{"192.168.1.50:19092"}, "broker-2")
	if got != "192.168.1.50:19093" {
		t.Fatalf("got %q, want 192.168.1.50:19093", got)
	}

	// In-container client dialing via port 9092 rewrites to container name
	got = ResolveAddressWithBootstrap("127.0.0.1:19093", []string{"broker-1:9092"}, "broker-2")
	if got != "broker-2:9092" {
		t.Fatalf("got %q, want broker-2:9092", got)
	}

	// Non-loopback advertised address is preserved
	got = ResolveAddressWithBootstrap("k8s-broker-2:9092", []string{"127.0.0.1:19092"}, "broker-2")
	if got != "k8s-broker-2:9092" {
		t.Fatalf("got %q, want k8s-broker-2:9092", got)
	}
}

func TestCustomEndpointResolver(t *testing.T) {
	c, err := New(Config{
		Bootstrap: []string{"127.0.0.1:19092"},
		EndpointResolver: func(brokerID string, advertisedAddr string) string {
			return fmt.Sprintf("custom-%s:9999", brokerID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	metadata := protocol.MetadataResponse{
		Brokers: []protocol.BrokerMetadata{{ID: "b1", Address: "127.0.0.1:19092"}},
		Partitions: []protocol.PartitionMetadata{
			{Topic: "orders", PartitionID: 0, Leader: "b1"},
		},
	}
	_, addr, err := c.routeForPartition(metadata, "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if addr != "custom-b1:9999" {
		t.Fatalf("got %q, want custom-b1:9999", addr)
	}
}
