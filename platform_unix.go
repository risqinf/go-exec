//go:build !windows

package main

import (
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// setCredentials switches the child process UID/GID on Unix systems.
func setCredentials(cmd *exec.Cmd, username string) error {
	if username == "" || username == "root" {
		return nil
	}
	uid, gid, err := lookupUser(username)
	if err != nil {
		return err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid: uid,
			Gid: gid,
		},
	}
	return nil
}

// notifySignals registers OS signals to be sent to ch.
// Unix supports SIGINT, SIGTERM, and SIGHUP (hot-reload).
func notifySignals(ch chan<- os.Signal) {
	signal.Notify(ch,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGHUP,
	)
}

// isReload returns true if the signal should trigger a config reload.
func isReload(sig os.Signal) bool {
	return sig == syscall.SIGHUP
}
