// Package polling provides sacct-based SLURM job status polling and
// output retrieval helpers.
// Extracted from bwb_scheduler/workflow/slurm_poller.go.
package polling

import (
	"fmt"
	"strings"
	"time"

	"go-slurm-backend/api"
	"go-slurm-backend/sshutil"
)

// JOB_CODES maps each SLURM job state string to its semantics.
var JOB_CODES = map[string]struct {
	Done   bool
	Failed bool
	Fatal  bool
}{
	"RUNNING":       {Done: false, Failed: false, Fatal: false},
	"COMPLETED":     {Done: true, Failed: false, Fatal: false},
	"BOOT_FAIL":     {Done: true, Failed: true, Fatal: false},
	"CANCELLED":     {Done: true, Failed: true, Fatal: true},
	"DEADLINE":      {Done: true, Failed: true, Fatal: false},
	"FAILED":        {Done: true, Failed: true, Fatal: true},
	"NODE_FAIL":     {Done: true, Failed: true, Fatal: false},
	"OUT_OF_MEMORY": {Done: true, Failed: true, Fatal: false},
	"PREEMPTED":     {Done: true, Failed: true, Fatal: false},
}

// RunSacct queries sacct for the given job IDs and returns a map of
// job ID → SacctResult.  The Done/Failed/Fatal fields are derived from
// JOB_CODES.
func RunSacct(jobIDs []string, runCmd sshutil.CmdRunner) (map[string]api.SacctResult, error) {
	jobsStr := strings.Join(jobIDs, ",")
	sacctCmd := fmt.Sprintf("sacct -j %s -o JobID,State,ExitCode -n -P", jobsStr)
	out, err := runCmd(sacctCmd)
	if err != nil {
		return nil, fmt.Errorf(
			"sacct failed with exit code %d, error %w, stderr %s",
			out.ExitCode, err, out.StdErr,
		)
	}

	results := make(map[string]api.SacctResult)
	for _, rawRecord := range strings.Split(out.StdOut, "\n") {
		if rawRecord == "" {
			continue
		}
		fields := strings.Split(rawRecord, "|")
		if len(fields) != 3 {
			return nil, fmt.Errorf(
				"sacct returned record %q with %d fields; expected 3 (JobID, State, ExitCode)",
				rawRecord, len(fields),
			)
		}

		jobIDField := fields[0]
		// Skip sub-job records like "12345.batch" or "12345.extern".
		if strings.HasSuffix(jobIDField, ".batch") || strings.HasSuffix(jobIDField, ".extern") {
			continue
		}
		// Strip any step suffix (e.g. "12345.0" → "12345").
		cleanID := strings.Split(jobIDField, ".")[0]

		state := fields[1]
		codes := JOB_CODES[state]
		results[cleanID] = api.SacctResult{
			JobID:    cleanID,
			State:    state,
			ExitCode: fields[2],
			Done:     codes.Done,
			Failed:   codes.Failed,
			Fatal:    codes.Fatal,
		}
	}
	return results, nil
}

// AwaitFileExistence checks whether path exists on the remote host.  If the
// first check fails it waits 5 s and retries once, to allow for LUSTRE/
// container-volume propagation lag.
func AwaitFileExistence(path string, runCmd sshutil.CmdRunner) bool {
	out, _ := runCmd(fmt.Sprintf("ls %s", path))
	if out.ExitCode == 0 {
		return true
	}
	time.Sleep(5 * time.Second)
	out, _ = runCmd(fmt.Sprintf("ls %s", path))
	return out.ExitCode == 0
}
