package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
)

const operatorActor = "local-operator"

type Principal struct {
	TenantID string
	Actor    string
}

type principalContextKey struct{}

func principalForRequest(r *http.Request, tenantID string) *http.Request {
	principal := Principal{TenantID: tenantID, Actor: operatorActor}
	ctx := context.WithValue(r.Context(), principalContextKey{}, principal)
	return r.WithContext(ctx)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

func authorizedRequest(r *http.Request, expectedToken string) bool {
	values := r.Header.Values("Authorization")
	return len(values) == 1 && authorizedBearer(values[0], expectedToken)
}

func authorizedBearer(header, expectedToken string) bool {
	if expectedToken == "" {
		return false
	}
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || token == "" || strings.ContainsAny(token, " \t\r\n") {
		return false
	}
	if len(token) != len(expectedToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) == 1
}
