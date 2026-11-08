package main

import (
	"strings"
	"testing"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/receiver"
	"github.com/uttampandit/webhook-delivery-platform/internal/signing"
)

func TestListenerBindings(t *testing.T) {
	for _, mode := range []string{"host", "compose"} {
		for _, code := range []string{"fulfillment", "notification"} {
			values := map[string]string{"DATABASE_URL": "postgres://test:public@localhost/test", "RUN_MODE": mode, "RECEIVER_CODE": code, "DEMO_CONTROL_TOKEN": strings.Repeat("ab", 32), "FULFILLMENT_SIGNING_KEY_V1": strings.Repeat("cd", 32), "NOTIFICATION_SIGNING_KEY_V1": strings.Repeat("ef", 32)}
			s, err := config.LoadFrom(config.ServiceReceiver, func(k string) (string, bool) { v, ok := values[k]; return v, ok })
			if err != nil {
				t.Fatal(err)
			}
			w, c := newServers(s, receiver.New(nil, signing.Auth{}, "", time.Second))
			wantW, wantC := ":8081", ":9091"
			if mode == "host" {
				wantW, wantC = "127.0.0.1:8082", "127.0.0.1:9092"
				if code == "notification" {
					wantW, wantC = "127.0.0.1:8083", "127.0.0.1:9093"
				}
			}
			if w.Addr != wantW || c.Addr != wantC || w.ReadTimeout == 0 || c.WriteTimeout == 0 {
				t.Fatal("invalid listener setup")
			}
		}
	}
}
