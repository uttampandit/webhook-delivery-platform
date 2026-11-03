package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

var generatedSecretLine = regexp.MustCompile(`(?m)^(POSTGRES_PASSWORD|API_TOKEN|DEMO_CONTROL_TOKEN|FULFILLMENT_SIGNING_KEY_V1|NOTIFICATION_SIGNING_KEY_V1)=([0-9a-f]{64})$`)

func TestInitEnvCommandCreatesPrivateRandomFileAndRefusesOverwrite(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate init-env test source")
	}
	projectRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "../.."))
	script := filepath.Join(projectRoot, "scripts", "init-env.sh")
	path := filepath.Join(t.TempDir(), "local.env")

	command := exec.Command("sh", script, path)
	command.Dir = projectRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("initialize environment file: %v; output=%s", err, output)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal("stat initialized file:", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("environment file mode = %04o, want 0600", info.Mode().Perm())
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("read generated test file:", err)
	}
	lines := generatedSecretLine.FindAllStringSubmatch(string(contents), -1)
	if len(lines) != 5 {
		t.Fatalf("found %d generated secret fields, want 5", len(lines))
	}
	seen := make(map[string]bool, len(lines))
	for _, line := range lines {
		seen[line[2]] = true
		if strings.Contains(string(output), line[2]) {
			t.Fatalf("helper printed generated secret %s", line[1])
		}
	}
	if len(seen) != 5 {
		t.Fatal("generated secrets were not independent values")
	}

	command = exec.Command("sh", script, path)
	command.Dir = projectRoot
	output, err = command.CombinedOutput()
	if err == nil {
		t.Fatal("helper overwrote an existing environment file")
	}
	if strings.Contains(string(output), "Created private environment file") {
		t.Fatalf("overwrite attempt reported success: %s", output)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("read environment file after overwrite attempt:", err)
	}
	if string(after) != string(contents) {
		t.Fatal("overwrite attempt changed the existing environment file")
	}
}
