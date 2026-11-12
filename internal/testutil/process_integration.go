//go:build integration && (darwin || linux)

package testutil

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"syscall"
)

// ChildProcess controls an actual OS process used by an integration test.
// Stdout is continuously drained so a checkpoint cannot deadlock on a full pipe.
type ChildProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	lines   chan string
	done    chan struct{}

	mu      sync.Mutex
	waitErr error
	stderr  *synchronizedBuffer
}

type synchronizedBuffer struct {
	mu      sync.Mutex
	data    []byte
	changed chan struct{}
}

func newSynchronizedBuffer() *synchronizedBuffer {
	return &synchronizedBuffer{changed: make(chan struct{})}
}

func (buffer *synchronizedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	const maximum = 64 * 1024
	buffer.data = append(buffer.data, data...)
	if len(buffer.data) > maximum {
		buffer.data = append([]byte(nil), buffer.data[len(buffer.data)-maximum:]...)
	}
	close(buffer.changed)
	buffer.changed = make(chan struct{})
	return len(data), nil
}

// StartChildProcess starts executable with only the supplied environment. This
// avoids accidentally inheriting local credentials or hook activation settings.
func StartChildProcess(executable string, environment map[string]string) (*ChildProcess, error) {
	command := exec.Command(executable)
	command.Env = make([]string, 0, len(environment))
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.ContainsAny(key, "=\x00") || strings.ContainsRune(environment[key], '\x00') {
			return nil, fmt.Errorf("invalid child environment entry %q", key)
		}
		command.Env = append(command.Env, key+"="+environment[key])
	}

	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("open child stdout: %w", err)
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open child stdin: %w", err)
	}
	child := &ChildProcess{command: command, stdin: stdin, lines: make(chan string, 8), done: make(chan struct{}), stderr: newSynchronizedBuffer()}
	command.Stderr = child.stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start child process: %w", err)
	}
	go child.drain(stdout)
	go func() {
		err := command.Wait()
		child.mu.Lock()
		child.waitErr = err
		child.mu.Unlock()
		close(child.done)
	}()
	return child, nil
}

func (child *ChildProcess) drain(stdout io.ReadCloser) {
	defer close(child.lines)
	defer stdout.Close()
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 256), 4096)
	for scanner.Scan() {
		line := scanner.Text()
		select {
		case child.lines <- line:
		default:
		}
	}
}

// ReadLine returns one bounded stdout line, process exit, or context deadline.
func (child *ChildProcess) ReadLine(ctx context.Context) (string, error) {
	select {
	case line, ok := <-child.lines:
		if !ok {
			return "", fmt.Errorf("child stdout closed: %w", child.WaitError())
		}
		return line, nil
	case <-child.done:
		select {
		case line, ok := <-child.lines:
			if ok {
				return line, nil
			}
		default:
		}
		return "", fmt.Errorf("child exited before output: %w", child.WaitError())
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Continue releases a testhook checkpoint using its documented stdin protocol.
func (child *ChildProcess) Continue() error {
	if child.IsDone() {
		return fmt.Errorf("child process already exited")
	}
	_, err := io.WriteString(child.stdin, "continue\n")
	return err
}

// Signal sends an OS signal to the actual child process.
func (child *ChildProcess) Signal(signal os.Signal) error {
	if child.IsDone() {
		return nil
	}
	return child.command.Process.Signal(signal)
}

// Kill sends SIGKILL; tests use it to verify real process-death recovery.
func (child *ChildProcess) Kill() error {
	if child.IsDone() {
		return nil
	}
	return child.command.Process.Signal(syscall.SIGKILL)
}

// Wait returns after process cleanup or the caller's deadline.
func (child *ChildProcess) Wait(ctx context.Context) error {
	select {
	case <-child.done:
		return child.WaitError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitError returns the exec result after the child has exited.
func (child *ChildProcess) WaitError() error {
	child.mu.Lock()
	defer child.mu.Unlock()
	return child.waitErr
}

// WaitForStderr waits for a specific sanitized diagnostic without polling or
// exposing the child's full stderr, which may contain implementation details.
func (child *ChildProcess) WaitForStderr(ctx context.Context, text string) error {
	for {
		child.stderr.mu.Lock()
		found := bytes.Contains(child.stderr.data, []byte(text))
		changed := child.stderr.changed
		child.stderr.mu.Unlock()
		if found {
			return nil
		}
		select {
		case <-changed:
		case <-child.done:
			child.stderr.mu.Lock()
			found = bytes.Contains(child.stderr.data, []byte(text))
			child.stderr.mu.Unlock()
			if found {
				return nil
			}
			return fmt.Errorf("child exited before diagnostic")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (child *ChildProcess) IsDone() bool {
	select {
	case <-child.done:
		return true
	default:
		return false
	}
}

// KilledBySIGKILL reports the actual process wait status on supported hosts.
func (child *ChildProcess) KilledBySIGKILL() bool {
	if !child.IsDone() || child.command.ProcessState == nil {
		return false
	}
	status, ok := child.command.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

// Close kills a leftover process and bounds its wait for failure cleanup.
func (child *ChildProcess) Close(ctx context.Context) error {
	_ = child.stdin.Close()
	if !child.IsDone() {
		if err := child.Kill(); err != nil && !child.IsDone() {
			return fmt.Errorf("kill child process: %w", err)
		}
	}
	select {
	case <-child.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
