//go:build integration

package config

import "testing"

func TestIntegrationTestRegistryAllowsOnlyLoopbackReceiverURLs(t *testing.T) {
	registry, err := NewIntegrationTestRegistry(RunModeCompose,
		"http://127.0.0.1:49123/webhook", "http://[::1]:49124/webhook")
	if err != nil {
		t.Fatal("create loopback integration registry:", err)
	}
	destinations := registry.Destinations()
	if destinations[0].URL != "http://127.0.0.1:49123/webhook" || destinations[1].URL != "http://[::1]:49124/webhook" {
		t.Fatalf("integration URLs were not injected: %+v", destinations)
	}
	if _, err := NewIntegrationTestRegistry(RunModeCompose,
		"http://example.invalid/webhook", "http://127.0.0.1:49124/webhook"); err == nil {
		t.Fatal("integration registry accepted a non-loopback receiver URL")
	}
}
