// Package rsyncfs provides rsync-over-SSH file transfer helpers.
// Extracted from bwb_scheduler/fs/ssh_fs.go.
package rsyncfs

import (
	"fmt"
	"os/exec"

	"go-slurm-backend/api"
)

// Upload rsync-uploads src (local path) to dst (path on the remote host).
func Upload(conf api.SshConfig, src, dst string) error {
	cmd := rsyncCmd(conf, fmt.Sprintf(
		"rsync --mkpath -av -e 'ssh -p %d' %s %s@%s:%s",
		sshPort(conf), src, conf.User, transferAddr(conf), dst,
	))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("rsync upload %s -> %s@%s:%s: %w\n%s",
			src, conf.User, transferAddr(conf), dst, err, out)
	}
	return nil
}

// Download rsync-downloads src (path on the remote host) to dst (local path).
func Download(conf api.SshConfig, src, dst string) error {
	cmd := rsyncCmd(conf, fmt.Sprintf(
		"rsync --mkpath -av -e 'ssh -p %d' %s@%s:%s %s",
		sshPort(conf), conf.User, transferAddr(conf), src, dst,
	))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("rsync download %s@%s:%s -> %s: %w\n%s",
			conf.User, transferAddr(conf), src, dst, err, out)
	}
	return nil
}

// rsyncCmd wraps the rsync invocation as an exec.Cmd via sh -c.
// Stdout/Stderr are intentionally left unset so callers can use CombinedOutput.
func rsyncCmd(_ api.SshConfig, cmdStr string) *exec.Cmd {
	return exec.Command("sh", "-c", cmdStr)
}

func transferAddr(conf api.SshConfig) string {
	if conf.TransferAddr != "" {
		return conf.TransferAddr
	}
	return conf.IpAddr
}

func sshPort(conf api.SshConfig) int {
	if conf.Port == 0 {
		return 22
	}
	return conf.Port
}
