package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseOptionsIncludesExplicitBoundedConfiguration(t *testing.T) {
	got, err := parseOptions([]string{"--events=80", "--producer-concurrency=2", "--payload-bytes=2048", "--timeout=3m", "--run-id=task26_run"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Events != 80 || got.ProducerConcurrency != 2 || got.PayloadBytes != 2048 || got.Timeout != 3*time.Minute || got.RunID != "task26_run" {
		t.Fatalf("parsed options = %+v", got)
	}
	if !reflect.DeepEqual(got.Command, []string{"--events=80", "--producer-concurrency=2", "--payload-bytes=2048", "--timeout=3m", "--run-id=task26_run"}) {
		t.Fatalf("command args = %#v", got.Command)
	}
}

func TestParseOptionsRejectsUnknownDuplicateAndOverCapFlags(t *testing.T) {
	for _, args := range [][]string{
		{"--token=secret"},
		{"--events=1", "--events=2"},
		{"--events=10001"},
		{"--producer-concurrency=33"},
		{"--payload-bytes=32769"},
		{"--timeout=31m"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Fatalf("accepted invalid args %q", args)
		}
	}
}
