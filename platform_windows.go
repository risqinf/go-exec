//go:build windows

package main

import (
	"os"
	"os/exec"
	"os/signal"
)

// setCredentials is a no-op on Windows.
// User switching via UID/GID is not supported on this platform.
func setCredentials(_ *exec.Cmd, _ string) error {
	return nil
}

// notifySignals registers OS signals to be sent to ch.
// Windows only supports os.Interrupt (Ctrl+C); SIGHUP does not exist.
func notifySignals(ch chan<- os.Signal) {
	signal.Notify(ch, os.Interrupt)
}

// isReload always returns false on Windows (no SIGHUP support).
func isReload(_ os.Signal) bool {
	return false
}
