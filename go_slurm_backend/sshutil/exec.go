package sshutil

import (
	"bytes"
	"fmt"
	"sync"

	"go-slurm-backend/api"
	"golang.org/x/crypto/ssh"
)

// CmdOut holds the result of a remote SSH command.
type CmdOut struct {
	ExitCode int
	StdOut   string
	StdErr   string
}

// CmdRunner is a function type that executes a remote command and returns
// its output.  Used to decouple callers from a live SSH connection in tests.
type CmdRunner func(string) (CmdOut, error)

// SSHExecutor manages a persistent SSH connection to a SLURM login node.
// It reconnects automatically if the session is lost.
// Extracted and simplified from bwb_scheduler/workflow/slurm_poller.go
// (SlurmActivity) and bwb_scheduler/workflow/worker.go (getSshConnection).
type SSHExecutor struct {
	config     api.SshConfig
	connConfig *ssh.ClientConfig
	client     *ssh.Client
	mu         sync.Mutex
}

// NewSSHExecutor dials the SSH server described by conf and returns a ready
// executor.  Returns an error if authentication or connection fails.
func NewSSHExecutor(conf api.SshConfig) (*SSHExecutor, error) {
	authMethods, err := BuildAuthMethods()
	if err != nil {
		return nil, fmt.Errorf("ssh auth: %w", err)
	}
	hostKeyCallback, err := GetHostKeyCallback()
	if err != nil {
		return nil, fmt.Errorf("known_hosts: %w", err)
	}

	connConfig := &ssh.ClientConfig{
		User:            conf.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
	}
	addr := sshAddr(conf)
	client, err := ssh.Dial("tcp", addr, connConfig)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	return &SSHExecutor{
		config:     conf,
		connConfig: connConfig,
		client:     client,
	}, nil
}

func sshAddr(conf api.SshConfig) string {
	port := conf.Port
	if port == 0 {
		port = 22
	}
	return fmt.Sprintf("%s:%d", conf.IpAddr, port)
}

// ensureConnected returns the current client or dials a new one.
func (e *SSHExecutor) ensureConnected() (*ssh.Client, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil {
		return e.client, nil
	}
	client, err := ssh.Dial("tcp", sshAddr(e.config), e.connConfig)
	if err != nil {
		return nil, fmt.Errorf("ssh reconnect: %w", err)
	}
	e.client = client
	return client, nil
}

// Close shuts down the SSH connection.
func (e *SSHExecutor) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil {
		e.client.Close()
		e.client = nil
	}
}

// Exec runs cmd on the remote host and returns the output.
// If CmdPrefix is configured it is prepended to every command.
// A non-zero exit code is NOT treated as a Go error; callers should
// inspect CmdOut.ExitCode directly.
func (e *SSHExecutor) Exec(cmd string) (CmdOut, error) {
	client, err := e.ensureConnected()
	if err != nil {
		return CmdOut{}, err
	}

	session, err := client.NewSession()
	if err != nil {
		// Session creation failed; reset and retry once.
		e.Close()
		client, err = e.ensureConnected()
		if err != nil {
			return CmdOut{}, fmt.Errorf("reconnect after session failure: %w", err)
		}
		session, err = client.NewSession()
		if err != nil {
			return CmdOut{}, fmt.Errorf("new session after reconnect: %w", err)
		}
	}

	fullCmd := cmd
	if e.config.CmdPrefix != nil {
		fullCmd = fmt.Sprintf("%s %s", *e.config.CmdPrefix, cmd)
	}

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	runErr := session.Run(fullCmd)

	exitCode := 0
	if runErr != nil {
		if exitErr, ok := runErr.(*ssh.ExitError); ok {
			exitCode = exitErr.ExitStatus()
			runErr = nil // non-zero exit is normal; surface via ExitCode
		}
	}
	return CmdOut{
		ExitCode: exitCode,
		StdOut:   stdout.String(),
		StdErr:   stderr.String(),
	}, runErr
}

// Runner returns a CmdRunner closure backed by this executor.
// Useful for passing to functions that accept a CmdRunner interface.
func (e *SSHExecutor) Runner() CmdRunner {
	return func(cmd string) (CmdOut, error) {
		return e.Exec(cmd)
	}
}
