//go:build crash && !darwin && !linux

package crash_test

import "testing"

func TestCrashHarnessRequiresLinuxOrMacOS(t *testing.T) {
	t.Fatal("actual SIGKILL crash tests require a Linux or macOS process host")
}
