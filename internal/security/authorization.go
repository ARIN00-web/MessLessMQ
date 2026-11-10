package security

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
)

type Permission struct {
	Resource string
	Action   string
}

type Authorizer interface {
	Allowed(Principal, string, string) bool
}

type ACLAuthorizer struct {
	mu          sync.RWMutex
	permissions map[string][]Permission
}

func NewACLAuthorizer() *ACLAuthorizer {
	return &ACLAuthorizer{permissions: make(map[string][]Permission)}
}

func (a *ACLAuthorizer) Grant(role, resource, action string) error {
	role, resource, action = strings.TrimSpace(role), strings.TrimSpace(resource), strings.TrimSpace(action)
	if a == nil || role == "" || resource == "" || action == "" {
		return errors.New("role, resource, and action are required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, current := range a.permissions[role] {
		if current.Resource == resource && current.Action == action {
			return nil
		}
	}
	a.permissions[role] = append(a.permissions[role], Permission{Resource: resource, Action: action})
	return nil
}

func (a *ACLAuthorizer) Revoke(role, resource, action string) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	permissions := a.permissions[role]
	for i, p := range permissions {
		if p.Resource == resource && p.Action == action {
			a.permissions[role] = append(permissions[:i], permissions[i+1:]...)
			return true
		}
	}
	return false
}

func (a *ACLAuthorizer) Allowed(principal Principal, resource, action string) bool {
	if a == nil || resource == "" || action == "" {
		return false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, role := range principal.Roles {
		for _, p := range a.permissions[role] {
			if (p.Resource == resource || p.Resource == "*") && (p.Action == action || p.Action == "*") {
				return true
			}
		}
	}
	return false
}

type principalContextKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, clonePrincipal(principal))
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return clonePrincipal(principal), ok
}

func RequirePermission(authorizer Authorizer, resource, action string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if authorizer == nil || !authorizer.Allowed(principal, resource, action) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func Protect(next http.Handler, store *KeyStore, authorizer Authorizer, resource, action string) http.Handler {
	if store == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "authentication unavailable", http.StatusServiceUnavailable)
		})
	}
	return store.Middleware(RequirePermission(authorizer, resource, action, next))
}
