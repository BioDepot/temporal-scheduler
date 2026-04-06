// Package sshutil handles SSH authentication and connection management
// for the SLURM login node.  Extracted from bwb_scheduler/workflow/worker.go.
package sshutil

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// BuildAuthMethods returns SSH auth methods in preference order:
// SSH agent first (if SSH_AUTH_SOCK is set and usable), then all
// unencrypted private keys found in ~/.ssh/.
func BuildAuthMethods() ([]ssh.AuthMethod, error) {
	var methods []ssh.AuthMethod
	if am, ok := getAgentAuth(); ok {
		methods = append(methods, am)
	}
	keyAuth, err := getAllKeyAuth()
	if err != nil {
		return nil, err
	}
	if keyAuth != nil {
		methods = append(methods, keyAuth)
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("no usable SSH auth methods found (no agent, no keys in ~/.ssh/)")
	}
	return methods, nil
}

// getAgentAuth tries to connect to the SSH agent via SSH_AUTH_SOCK.
func getAgentAuth() (ssh.AuthMethod, bool) {
	sock := os.Getenv("SSH_AUTH_SOCK")
	if sock == "" {
		return nil, false
	}
	conn, err := net.Dial("unix", sock)
	if err != nil {
		return nil, false
	}
	ag := agent.NewClient(conn)
	signers, err := ag.Signers()
	if err != nil || len(signers) == 0 {
		return nil, false
	}
	return ssh.PublicKeys(signers...), true
}

// getAllKeyAuth loads all unencrypted private keys from ~/.ssh/ and returns
// a PublicKeys auth method.  Keys that are passphrase-protected are skipped
// with a warning (matching the behaviour of the old Go scheduler).
func getAllKeyAuth() (ssh.AuthMethod, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, err
	}
	sshDir := filepath.Join(usr.HomeDir, ".ssh")
	entries, err := os.ReadDir(sshDir)
	if err != nil {
		// ~/.ssh missing is not fatal; caller will fall back to agent only.
		return nil, nil
	}

	var signers []ssh.Signer
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(sshDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		signer, err := ssh.ParsePrivateKey(data)
		if err != nil {
			fmt.Printf("sshutil: skipping %s (not a private key or passphrase-protected)\n", path)
			continue
		}
		signers = append(signers, signer)
	}
	if len(signers) == 0 {
		return nil, nil
	}
	return ssh.PublicKeys(signers...), nil
}

// GetHostKeyCallback reads ~/.ssh/known_hosts for strict host key verification.
func GetHostKeyCallback() (ssh.HostKeyCallback, error) {
	usr, err := user.Current()
	if err != nil {
		return nil, err
	}
	khPath := filepath.Join(usr.HomeDir, ".ssh", "known_hosts")
	return knownhosts.New(khPath)
}
