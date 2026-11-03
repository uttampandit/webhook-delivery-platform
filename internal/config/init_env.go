package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// CreateEnvFile writes a local environment file with fresh cryptographic
// secrets. A hard-link publish makes the final creation atomic and refuses to
// replace any existing file or symlink.
func CreateEnvFile(path string) (err error) {
	if path == "" {
		return errors.New("environment file path is required")
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return errors.New("cannot resolve environment file path")
	}
	directory := filepath.Dir(absolutePath)
	name := filepath.Base(absolutePath)
	temporary, err := os.CreateTemp(directory, "."+name+".tmp-")
	if err != nil {
		return errors.New("cannot create a private temporary environment file")
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return errors.New("cannot set private environment file permissions")
	}
	if err := writeEnvironment(temporary); err != nil {
		_ = temporary.Close()
		return errors.New("cannot write generated environment file")
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errors.New("cannot sync generated environment file")
	}
	if err := temporary.Close(); err != nil {
		return errors.New("cannot close generated environment file")
	}
	if err := os.Link(temporaryPath, absolutePath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("refusing to overwrite an existing environment file")
		}
		return errors.New("cannot install generated environment file")
	}
	return nil
}

func writeEnvironment(file *os.File) error {
	postgresPassword, err := randomHexSecret()
	if err != nil {
		return err
	}
	apiToken, err := randomHexSecret()
	if err != nil {
		return err
	}
	demoControlToken, err := randomHexSecret()
	if err != nil {
		return err
	}
	fulfillmentKey, err := randomHexSecret()
	if err != nil {
		return err
	}
	notificationKey, err := randomHexSecret()
	if err != nil {
		return err
	}
	contents := fmt.Sprintf(`POSTGRES_USER=webhook
POSTGRES_PASSWORD=%s
DATABASE_URL=postgres://webhook:%s@127.0.0.1:5432/webhook_dev?sslmode=disable
TEST_DATABASE_ADMIN_URL=postgres://webhook:%s@127.0.0.1:5432/postgres?sslmode=disable
RUN_MODE=compose
WORKER_CONCURRENCY=4
WORKER_POLL_INTERVAL=250ms
LEASE_DURATION=30s
LEASE_MARGIN=5s
HTTP_TIMEOUT=5s
REAPER_INTERVAL=1s
DB_STATEMENT_TIMEOUT=3s
API_TOKEN=%s
DEMO_CONTROL_TOKEN=%s
FULFILLMENT_SIGNING_KEY_V1=%s
NOTIFICATION_SIGNING_KEY_V1=%s
RECEIVER_CODE=fulfillment
`, postgresPassword, postgresPassword, postgresPassword, apiToken, demoControlToken, fulfillmentKey, notificationKey)
	_, err = file.WriteString(contents)
	return err
}

func randomHexSecret() (string, error) {
	var secret [minimumSecretBytes]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(secret[:]), nil
}
