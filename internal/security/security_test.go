package security

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestKeyStoreCreateAuthenticateExpireAndRevoke(t *testing.T) {
	store := NewKeyStore()
	token, err := store.Create(Principal{ID: "producer-1", Roles: []string{"writer", "writer"}}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := store.Authenticate(token)
	if err != nil || principal.ID != "producer-1" || len(principal.Roles) != 1 {
		t.Fatalf("principal=%+v err=%v", principal, err)
	}
	principal.Roles[0] = "mutated"
	principal, err = store.Authenticate(token)
	if err != nil || principal.Roles[0] != "writer" {
		t.Fatal("principal roles were not defensively copied")
	}
	if !store.Revoke(token) {
		t.Fatal("revoke did not remove key")
	}
	if _, err := store.Authenticate(token); err != ErrInvalidCredential {
		t.Fatalf("expected invalid credential, got %v", err)
	}
	if err := store.Add("expiring", Principal{ID: "p"}, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthenticateAt("expiring", time.Unix(100, 0)); err != ErrExpiredCredential {
		t.Fatalf("expected expired, got %v", err)
	}
}

func TestAuthenticationAndAuthorizationMiddleware(t *testing.T) {
	store := NewKeyStore()
	token, err := store.Create(Principal{ID: "alice", Roles: []string{"producer"}}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	acl := NewACLAuthorizer()
	if err := acl.Grant("producer", "orders", "write"); err != nil {
		t.Fatal(err)
	}
	handler := Protect(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), store, acl, "orders", "write")
	request := httptest.NewRequest("POST", "/orders", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 401 {
		t.Fatalf("unauthenticated status=%d", response.Code)
	}
	request = httptest.NewRequest("POST", "/orders", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 204 {
		t.Fatalf("authorized status=%d body=%s", response.Code, response.Body.String())
	}
	request = httptest.NewRequest("POST", "/orders", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	Protect(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }), store, acl, "orders", "delete").ServeHTTP(response, request)
	if response.Code != 403 {
		t.Fatalf("unauthorized action status=%d", response.Code)
	}
}

func TestParseBearerAndWildcardACL(t *testing.T) {
	if token, err := ParseBearer("bEaReR token"); err != nil || token != "token" {
		t.Fatalf("token=%q err=%v", token, err)
	}
	if _, err := ParseBearer("Basic token"); err == nil {
		t.Fatal("accepted Basic authentication")
	}
	acl := NewACLAuthorizer()
	if err := acl.Grant("admin", "*", "*"); err != nil {
		t.Fatal(err)
	}
	if !acl.Allowed(Principal{Roles: []string{"admin"}}, "orders", "delete") {
		t.Fatal("wildcard grant not applied")
	}
	if acl.Allowed(Principal{Roles: []string{"unknown"}}, "orders", "read") {
		t.Fatal("authorization did not deny by default")
	}
}

func TestRateLimiterTokenBucketAndCapacity(t *testing.T) {
	limiter, err := NewRateLimiter(2, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	limiter.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		ok, _, err := limiter.Allow("principal")
		if err != nil || !ok {
			t.Fatalf("first tokens: ok=%v err=%v", ok, err)
		}
	}
	ok, wait, err := limiter.Allow("principal")
	if err != nil || ok || wait <= 0 {
		t.Fatalf("expected throttling, ok=%v wait=%v err=%v", ok, wait, err)
	}
	if _, _, err := limiter.Allow("other"); err != ErrRateLimiterCapacity {
		t.Fatalf("expected capacity error, got %v", err)
	}
	now = now.Add(time.Second)
	if ok, _, err := limiter.Allow("principal"); err != nil || !ok {
		t.Fatalf("refill failed: ok=%v err=%v", ok, err)
	}
	limiter.Forget("principal")
	if limiter.KeyCount() != 0 {
		t.Fatalf("key count=%d", limiter.KeyCount())
	}
}

func TestTLSConfigurationAndMutualTLSValidation(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile := writeTestCertificate(t, dir)
	server, err := LoadServerTLS(certFile, keyFile, caFile, true)
	if err != nil {
		t.Fatal(err)
	}
	if server.MinVersion != 0x0303 || server.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("unexpected TLS server config: %+v", server)
	}
	client, err := LoadClientTLSWithCertificate(caFile, "localhost", certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if client.RootCAs == nil || client.ServerName != "localhost" || len(client.Certificates) != 1 {
		t.Fatal("client trust configuration missing")
	}
	if _, err := LoadServerTLS(certFile, keyFile, "", true); err == nil {
		t.Fatal("mTLS accepted without client CA")
	}
	if _, err := LoadClientTLS(filepath.Join(dir, "missing.pem"), "localhost"); err == nil {
		t.Fatal("client accepted missing CA file")
	}
}

func writeTestCertificate(t *testing.T, dir string) (string, string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER := x509.MarshalPKCS1PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER})
	certFile, keyFile, caFile := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key"), filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, caFile
}

func TestRateLimitMiddleware(t *testing.T) {
	limiter, err := NewRateLimiter(1, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	handler := RateLimitMiddleware(limiter, func(r *http.Request) string { return "verified-principal" }, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	for i := 0; i < 2; i++ {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/", nil))
		if i == 0 && response.Code != 204 {
			t.Fatalf("first request status=%d", response.Code)
		}
		if i == 1 && (response.Code != http.StatusTooManyRequests || response.Header().Get("Retry-After") == "") {
			t.Fatalf("second request status=%d retry-after=%q", response.Code, response.Header().Get("Retry-After"))
		}
	}
}
