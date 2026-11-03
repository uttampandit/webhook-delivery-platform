//go:build integration

package config

import (
	"errors"
	"net"
	"net/url"
	"strconv"
)

// NewIntegrationTestRegistry returns the fixed registry with receiver URLs
// replaced by loopback-only test endpoints. The integration build tag keeps
// this constructor out of normal application binaries.
func NewIntegrationTestRegistry(mode RunMode, fulfillmentURL, notificationURL string) (Registry, error) {
	registry, err := RegistryForMode(mode)
	if err != nil {
		return Registry{}, err
	}
	if !validLoopbackHTTPURL(fulfillmentURL) || !validLoopbackHTTPURL(notificationURL) {
		return Registry{}, errors.New("integration receiver URLs must be loopback HTTP URLs with an explicit port")
	}
	registry.destinations[0].URL = fulfillmentURL
	registry.destinations[1].URL = notificationURL
	return registry, nil
}

func validLoopbackHTTPURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return false
	}
	address := net.ParseIP(parsed.Hostname())
	if address == nil || !address.IsLoopback() {
		return false
	}
	port, err := strconv.Atoi(parsed.Port())
	return err == nil && port >= 1 && port <= 65535 && parsed.Path != ""
}
