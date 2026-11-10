package security

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidCredential = errors.New("invalid credential")
	ErrExpiredCredential = errors.New("credential expired")
)

type Principal struct {
	ID    string   `json:"id"`
	Roles []string `json:"roles"`
}

type credential struct {
	principal Principal
	expiresAt time.Time
}

type KeyStore struct {
	mu   sync.RWMutex
	keys map[[32]byte]credential
}

func NewKeyStore() *KeyStore { return &KeyStore{keys: make(map[[32]byte]credential)} }

func (s *KeyStore) Add(token string, principal Principal, expiresAt time.Time) error {
	if s == nil || strings.TrimSpace(token) == "" || strings.TrimSpace(principal.ID) == "" {
		return errors.New("token and principal ID are required")
	}
	principal.Roles = cleanRoles(principal.Roles)
	digest := sha256.Sum256([]byte(token))
	s.mu.Lock()
	s.keys[digest] = credential{principal: clonePrincipal(principal), expiresAt: expiresAt}
	s.mu.Unlock()
	return nil
}

func (s *KeyStore) Create(principal Principal, ttl time.Duration) (string, error) {
	if ttl < 0 {
		return "", errors.New("credential TTL cannot be negative")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(secret)
	var expires time.Time
	if ttl > 0 {
		expires = time.Now().Add(ttl)
	}
	if err := s.Add(token, principal, expires); err != nil {
		return "", err
	}
	return token, nil
}

func (s *KeyStore) Revoke(token string) bool {
	if s == nil || token == "" {
		return false
	}
	digest := sha256.Sum256([]byte(token))
	s.mu.Lock()
	_, exists := s.keys[digest]
	delete(s.keys, digest)
	s.mu.Unlock()
	return exists
}

func (s *KeyStore) Authenticate(token string) (Principal, error) {
	return s.AuthenticateAt(token, time.Now())
}

func (s *KeyStore) AuthenticateAt(token string, now time.Time) (Principal, error) {
	if s == nil || token == "" {
		return Principal{}, ErrInvalidCredential
	}
	digest := sha256.Sum256([]byte(token))
	s.mu.RLock()
	entry, exists := s.keys[digest]
	s.mu.RUnlock()
	if !exists {
		return Principal{}, ErrInvalidCredential
	}
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		return Principal{}, ErrExpiredCredential
	}
	return clonePrincipal(entry.principal), nil
}

func ParseBearer(header string) (string, error) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", ErrInvalidCredential
	}
	return parts[1], nil
}

func cleanRoles(roles []string) []string {
	seen := make(map[string]struct{}, len(roles))
	clean := make([]string, 0, len(roles))
	for _, role := range roles {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		clean = append(clean, role)
	}
	return clean
}

func clonePrincipal(p Principal) Principal {
	p.Roles = append([]string(nil), p.Roles...)
	return p
}

func (s *KeyStore) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := ParseBearer(r.Header.Get("Authorization"))
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		principal, err := s.Authenticate(token)
		if err != nil {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
	})
}
