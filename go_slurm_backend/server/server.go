// Package server implements the Go SLURM backend HTTP service.
// It exposes a narrow JSON API that the Python scheduler calls instead of
// running Paramiko / rsync directly.
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	osexec "os/exec"
	"strconv"
	"strings"
	"time"

	"go-slurm-backend/api"
	"go-slurm-backend/polling"
	"go-slurm-backend/rsyncfs"
	"go-slurm-backend/sshutil"
)

// sshPool is the process-wide SSH connection pool.  Connections idle for
// more than 60 s are reaped.  poll_slurm runs every 5 s so the connection
// to the SLURM login node stays warm.
var sshPool = sshutil.NewPool(60 * time.Second)

// getSSH borrows a connection from the pool.  Callers must call
// sshPool.Put(exec) when done (currently a no-op but keeps the contract).
func getSSH(conf api.SshConfig) (*sshutil.SSHExecutor, error) {
	return sshPool.Get(conf)
}

// New returns an http.Handler with all backend routes registered.
func New() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /setup_login_node_volumes", handleSetupVolumes)
	mux.HandleFunc("POST /upload_to_slurm_login_node", handleUpload)
	mux.HandleFunc("POST /start_slurm_job", handleStartJob)
	mux.HandleFunc("POST /poll_slurm", handlePollSlurm)
	mux.HandleFunc("POST /get_slurm_outputs", handleGetOutputs)
	mux.HandleFunc("POST /download_from_slurm_login_node", handleDownload)
	mux.HandleFunc("POST /validate_transfer_connectivity", handleValidateTransferConnectivity)
	return mux
}

// ---------- health ----------

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`)) //nolint:errcheck
}

// ---------- setup volumes ----------

func handleSetupVolumes(w http.ResponseWriter, r *http.Request) {
	var req api.SetupVolumesRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	exec, err := getSSH(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer sshPool.Put(exec)

	runner := exec.Runner()
	for _, dir := range req.Dirs {
		out, err := runner(fmt.Sprintf("mkdir -p %s", dir))
		if err != nil || out.ExitCode != 0 {
			writeError(w, fmt.Sprintf("mkdir -p %s failed (exit %d, stderr %s): %v",
				dir, out.ExitCode, out.StdErr, err))
			return
		}
	}

	writeJSON(w, api.SetupVolumesResponse{})
}

// ---------- upload ----------

func handleUpload(w http.ResponseWriter, r *http.Request) {
	var req api.FileUploadRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := rsyncfs.Upload(req.SshConfig, req.LocalSrcPath, req.RemoteDstPath); err != nil {
		writeError(w, err.Error())
		return
	}
	writeJSON(w, api.FileUploadResponse{})
}

// ---------- start job ----------

func handleStartJob(w http.ResponseWriter, r *http.Request) {
	var req api.StartJobRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	exec, err := getSSH(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer sshPool.Put(exec)

	runner := exec.Runner()
	for _, dir := range req.ExtraDirs {
		out, err := runner(fmt.Sprintf("mkdir -p %s", dir))
		if err != nil || out.ExitCode != 0 {
			writeError(w, fmt.Sprintf("mkdir -p %s failed (exit %d, stderr %s): %v",
				dir, out.ExitCode, out.StdErr, err))
			return
		}
	}

	job, err := polling.StartJob(exec, req.SshConfig, req.Cmd, req.JobConfig, req.Volumes, req.SshConfig.StorageDir)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	writeJSON(w, api.StartJobResponse{Job: job})
}

// ---------- poll slurm ----------

func handlePollSlurm(w http.ResponseWriter, r *http.Request) {
	var req api.PollJobsRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	exec, err := getSSH(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer sshPool.Put(exec)

	runner := exec.Runner()

	// Keepalive when no jobs are outstanding.
	if len(req.JobIDs) == 0 {
		runner("echo keepalive") //nolint:errcheck
		writeJSON(w, api.PollJobsResponse{Results: map[string]api.SacctResult{}})
		return
	}

	results, err := polling.RunSacct(req.JobIDs, runner)
	if err != nil {
		writeError(w, err.Error())
		return
	}
	writeJSON(w, api.PollJobsResponse{Results: results})
}

// ---------- get outputs ----------

func handleGetOutputs(w http.ResponseWriter, r *http.Request) {
	var req api.GetOutputsRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	exec, err := getSSH(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer sshPool.Put(exec)

	runner := exec.Runner()
	outputs := make([]api.CmdOutput, 0, len(req.Jobs))
	errs := make([]string, 0)

	for _, job := range req.Jobs {
		out, err := polling.GetOutputs(job, runner)
		if err != nil {
			errs = append(errs, fmt.Sprintf("job %s: %v", job.JobID, err))
			continue
		}
		out.Success = true
		outputs = append(outputs, out)
	}

	resp := api.GetOutputsResponse{Outputs: outputs}
	if len(errs) > 0 {
		resp.Error = strings.Join(errs, "; ")
	}
	writeJSON(w, resp)
}

// ---------- download ----------

func handleDownload(w http.ResponseWriter, r *http.Request) {
	var req api.FileDownloadRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := rsyncfs.Download(req.SshConfig, req.RemoteSrcPath, req.LocalDstPath); err != nil {
		writeError(w, err.Error())
		return
	}
	writeJSON(w, api.FileDownloadResponse{})
}

// ---------- validate transfer connectivity ----------

func handleValidateTransferConnectivity(w http.ResponseWriter, r *http.Request) {
	var req api.ValidateTransferConnectivityRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	sshExec, err := getSSH(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer sshPool.Put(sshExec)

	runner := sshExec.Runner()
	var checks []string

	// 1. SSH echo check
	out, err := runner("echo __ssh_ok__")
	if err != nil || out.ExitCode != 0 || !strings.Contains(out.StdOut, "__ssh_ok__") {
		errMsg := "unknown"
		if err != nil {
			errMsg = err.Error()
		} else if out.StdErr != "" {
			errMsg = out.StdErr
		}
		writeError(w, fmt.Sprintf("SSH check failed: %s", errMsg))
		return
	}
	checks = append(checks, "ssh_login")

	// 2. rsync reachability to transfer endpoint
	addr := req.SshConfig.TransferAddr
	if addr == "" {
		addr = req.SshConfig.IpAddr
	}
	xferPort := rsyncfs.TransferPort(req.SshConfig)
	rsyncProbe := fmt.Sprintf(
		"rsync --dry-run -e 'ssh -p %d' %s@%s:/dev/null /dev/null",
		xferPort, req.SshConfig.User, addr,
	)
	probeCmd := osexec.Command("sh", "-c", rsyncProbe)
	probeOut, probeErr := probeCmd.CombinedOutput()
	exitCode := 0
	if probeErr != nil {
		if exitErr, ok := probeErr.(*osexec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			writeError(w, fmt.Sprintf("rsync probe exec failed: %v", probeErr))
			return
		}
	}
	// exit 23 = partial transfer is acceptable for a dry-run probe
	if exitCode != 0 && exitCode != 23 {
		writeError(w, fmt.Sprintf("rsync check failed to %s:%d: exit=%d output=%s",
			addr, xferPort, exitCode, string(probeOut)))
		return
	}
	checks = append(checks, "rsync_transfer_endpoint")

	// 3. Remote storage writable
	storageDir := req.RemoteStorageDir
	if storageDir == "" {
		storageDir = req.SshConfig.StorageDir
	}
	probe := storageDir + "/.scheduler_probe"
	out, err = runner(fmt.Sprintf("mkdir -p %s && touch %s && rm -f %s && echo __write_ok__",
		storageDir, probe, probe))
	if err != nil || out.ExitCode != 0 || !strings.Contains(out.StdOut, "__write_ok__") {
		errMsg := "unknown"
		if err != nil {
			errMsg = err.Error()
		} else if out.StdErr != "" {
			errMsg = out.StdErr
		}
		writeError(w, fmt.Sprintf("remote storage %s not writable: %s", storageDir, errMsg))
		return
	}
	checks = append(checks, "remote_storage_writable")

	// 4. Disk space
	diskAvailGB := 0
	out, err = runner(fmt.Sprintf("df -BG %s | tail -1 | awk '{print $4}'", storageDir))
	if err == nil && out.ExitCode == 0 {
		raw := strings.TrimSpace(strings.ReplaceAll(out.StdOut, "G", ""))
		if n, parseErr := strconv.Atoi(raw); parseErr == nil {
			diskAvailGB = n
			if n < 10 {
				log.Printf("WARNING: only %dGB free on %s", n, storageDir)
			}
			checks = append(checks, fmt.Sprintf("disk=%dGB", n))
		}
	}

	status := fmt.Sprintf("pre-flight ok: %s", strings.Join(checks, ", "))
	writeJSON(w, api.ValidateTransferConnectivityResponse{
		Status:      status,
		Checks:      checks,
		DiskAvailGB: diskAvailGB,
	})
}

// ---------- helpers ----------

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON: %v", err)
	}
}

func writeError(w http.ResponseWriter, msg string) {
	log.Printf("backend error: %s", msg)
	writeJSON(w, map[string]string{"error": msg})
}
