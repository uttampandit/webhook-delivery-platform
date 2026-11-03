package main

import (
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
)

func TestHTTPServerUsesBoundedTimeoutsAndModeBinding(t *testing.T) {
	cases := []struct {
		name    string
		mode    string
		address string
	}{
		{name: "host binds loopback", mode: "host", address: "127.0.0.1:8080"},
		{name: "compose binds container port", mode: "compose", address: ":8080"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			settings, err := config.LoadFrom(config.ServiceAPI, func(key string) (string, bool) {
				values := map[string]string{
					"DATABASE_URL": "postgres://user:pass@127.0.0.1:5432/webhook_dev?sslmode=disable",
					"RUN_MODE":     test.mode,
					"API_TOKEN":    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				}
				value, ok := values[key]
				return value, ok
			})
			if err != nil {
				t.Fatal("load API settings:", err)
			}
			server := newHTTPServer(settings, nil)
			if server.Addr != test.address {
				t.Fatalf("server address = %q, want %q", server.Addr, test.address)
			}
			if server.ReadHeaderTimeout != 5*time.Second || server.ReadTimeout != 10*time.Second ||
				server.WriteTimeout != 15*time.Second || server.IdleTimeout != 60*time.Second {
				t.Fatalf("server timeouts = (%s, %s, %s, %s), want (5s, 10s, 15s, 60s)",
					server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout)
			}
		})
	}
}
