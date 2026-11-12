//go:build testhooks

package delivery

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	testHookEnableEnv    = "WEBHOOK_TEST_HOOK_ENABLE"
	testHookStageEnv     = "WEBHOOK_TEST_HOOK_STAGE"
	testHookOperationEnv = "WEBHOOK_TEST_HOOK_OPERATION_ID"
	testHookAttemptEnv   = "WEBHOOK_TEST_HOOK_ATTEMPT_NO"
)

var (
	testHookDatabaseName = regexp.MustCompile(`^webhook_test_[0-9a-f]{32}$`)
	testHookUUID         = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	testHookActivated    atomic.Bool
)

type testCheckpointMessage struct {
	Stage       string `json:"stage"`
	OperationID string `json:"operation_id"`
	AttemptNo   int    `json:"attempt_no"`
}

// testCheckpoint is compiled only into a binary built with the testhooks tag.
// It fails closed unless an explicit target matches and the live PostgreSQL
// connection reports an isolated test database name.
func testCheckpoint(stage string, claim ClaimedDelivery) {
	if os.Getenv(testHookEnableEnv) != "1" || os.Getenv(testHookStageEnv) != stage ||
		os.Getenv(testHookOperationEnv) != claim.DeliveryID || !testHookUUID.MatchString(claim.DeliveryID) {
		return
	}
	attempt, err := strconv.Atoi(os.Getenv(testHookAttemptEnv))
	if err != nil || attempt < 1 || attempt != claim.AttemptNo || !testHookDatabaseGuard() {
		return
	}
	if !testHookActivated.CompareAndSwap(false, true) {
		return
	}

	message := testCheckpointMessage{Stage: stage, OperationID: claim.DeliveryID, AttemptNo: claim.AttemptNo}
	encoded, err := json.Marshal(message)
	if err != nil || len(encoded) > 256 {
		os.Exit(2)
	}
	if _, err = os.Stdout.Write(append(encoded, '\n')); err != nil {
		os.Exit(2)
	}

	continued := make(chan string, 1)
	go func() {
		line, readErr := bufio.NewReader(io.LimitReader(os.Stdin, 64)).ReadString('\n')
		if readErr != nil {
			continued <- ""
			return
		}
		continued <- line
	}()
	timer := time.NewTimer(45 * time.Second)
	defer timer.Stop()
	select {
	case line := <-continued:
		if line != "continue\n" {
			os.Exit(2)
		}
	case <-timer.C:
		os.Exit(2)
	}
}

func testHookDatabaseGuard() bool {
	raw := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Hostname() == "" {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, raw)
	if err != nil {
		return false
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		_ = conn.Close(closeCtx)
	}()

	var currentDatabase string
	if err := conn.QueryRow(ctx, `SELECT current_database()`).Scan(&currentDatabase); err != nil {
		return false
	}
	return testHookDatabaseName.MatchString(currentDatabase)
}
