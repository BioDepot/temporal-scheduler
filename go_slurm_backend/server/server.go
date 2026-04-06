// Package server implements the Go SLURM backend HTTP service.
// It exposes a narrow JSON API that the Python scheduler calls instead of
// running Paramiko / rsync directly.
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"go-slurm-backend/api"
	"go-slurm-backend/polling"
	"go-slurm-backend/rsyncfs"
	"go-slurm-backend/sshutil"
)

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

	exec, err := sshutil.NewSSHExecutor(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer exec.Close()

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

	exec, err := sshutil.NewSSHExecutor(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer exec.Close()

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

	exec, err := sshutil.NewSSHExecutor(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer exec.Close()

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

	exec, err := sshutil.NewSSHExecutor(req.SshConfig)
	if err != nil {
		writeError(w, fmt.Sprintf("ssh connect: %v", err))
		return
	}
	defer exec.Close()

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
