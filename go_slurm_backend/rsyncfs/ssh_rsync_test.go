package rsyncfs

import (
	"testing"

	"go-slurm-backend/api"
)

func TestTransferPort_UsesTransferPortWhenSet(t *testing.T) {
	conf := api.SshConfig{Port: 22, TransferPort: 2222}
	if got := TransferPort(conf); got != 2222 {
		t.Errorf("TransferPort: want 2222, got %d", got)
	}
}

func TestTransferPort_FallsBackToPort(t *testing.T) {
	conf := api.SshConfig{Port: 3022, TransferPort: 0}
	if got := TransferPort(conf); got != 3022 {
		t.Errorf("TransferPort: want 3022 (fallback to Port), got %d", got)
	}
}

func TestTransferPort_FallsBackTo22WhenBothZero(t *testing.T) {
	conf := api.SshConfig{}
	if got := TransferPort(conf); got != 22 {
		t.Errorf("TransferPort: want 22 (default), got %d", got)
	}
}

func TestTransferAddr_UsesTransferAddrWhenSet(t *testing.T) {
	conf := api.SshConfig{IpAddr: "login.cluster.edu", TransferAddr: "data.cluster.edu"}
	if got := transferAddr(conf); got != "data.cluster.edu" {
		t.Errorf("transferAddr: want data.cluster.edu, got %q", got)
	}
}

func TestTransferAddr_FallsBackToIpAddr(t *testing.T) {
	conf := api.SshConfig{IpAddr: "login.cluster.edu"}
	if got := transferAddr(conf); got != "login.cluster.edu" {
		t.Errorf("transferAddr: want login.cluster.edu, got %q", got)
	}
}
