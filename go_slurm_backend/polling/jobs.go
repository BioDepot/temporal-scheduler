package polling

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go-slurm-backend/api"
	"go-slurm-backend/rsyncfs"
	"go-slurm-backend/sbatch"
	"go-slurm-backend/sshutil"
)

// StartJob submits a SLURM batch job for cmd using the provided SSH executor
// and rsync config.  It writes a local sbatch script, uploads it, runs sbatch,
// and returns a SlurmJob descriptor.
func StartJob(
	exec *sshutil.SSHExecutor,
	rsyncConf api.SshConfig,
	cmd api.SlurmCmdTemplate,
	jobConfig api.SlurmJobConfig,
	volumes map[string]string,
	storageDir string,
) (api.SlurmJob, error) {
	runCmd := exec.Runner()

	// Create a temporary output directory on the remote host.
	tmpOutputHostPath := filepath.Join(storageDir, "tmp", sbatch.RandID())
	if _, err := runCmd(fmt.Sprintf("mkdir -p %s", tmpOutputHostPath)); err != nil {
		return api.SlurmJob{}, fmt.Errorf("mkdir tmp output dir %s: %w", tmpOutputHostPath, err)
	}

	// Add the container /tmp/output → host tmp path volume mapping.
	volsCopy := make(map[string]string, len(volumes)+1)
	for k, v := range volumes {
		volsCopy[k] = v
	}
	volsCopy["/tmp/output"] = tmpOutputHostPath

	// Write sbatch script to a local temp file.
	sbatchFname := sbatch.RandID() + ".sbatch"
	sbatchLocalPath := filepath.Join("/tmp", sbatchFname)
	sbatchRemotePath := filepath.Join(storageDir, sbatchFname)

	tmpFile, err := os.Create(sbatchLocalPath)
	if err != nil {
		return api.SlurmJob{}, fmt.Errorf("create local sbatch file %s: %w", sbatchLocalPath, err)
	}
	defer tmpFile.Close()

	outPath, errPath, err := sbatch.WriteSbatchFile(tmpFile, cmd, volsCopy, jobConfig, storageDir)
	if err != nil {
		return api.SlurmJob{}, fmt.Errorf("write sbatch file: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		return api.SlurmJob{}, fmt.Errorf("sync sbatch file: %w", err)
	}

	// Upload sbatch script to login node.
	if err := rsyncfs.Upload(rsyncConf, sbatchLocalPath, sbatchRemotePath); err != nil {
		return api.SlurmJob{}, fmt.Errorf("upload sbatch %s → %s: %w", sbatchLocalPath, sbatchRemotePath, err)
	}

	// Submit.
	sbatchCmd := fmt.Sprintf("sbatch --parsable %s", sbatchRemotePath)
	sbatchOut, err := runCmd(sbatchCmd)
	if err != nil {
		return api.SlurmJob{}, fmt.Errorf("sbatch %s: exit %d stderr %s: %w",
			sbatchRemotePath, sbatchOut.ExitCode, sbatchOut.StdErr, err)
	}

	// Parse job ID.  In multi-cluster systems the output is "JOBID;CLUSTER".
	jobIDRaw := strings.Split(sbatchOut.StdOut, ";")[0]
	jobID := strings.TrimSuffix(jobIDRaw, "\n")

	return api.SlurmJob{
		CmdID:             cmd.ID,
		JobID:             jobID,
		TmpOutputHostPath: tmpOutputHostPath,
		ExpOutFilePnames:  cmd.OutFilePnames,
		SbatchPath:        sbatchRemotePath,
		OutPath:           outPath,
		ErrPath:           errPath,
	}, nil
}

// GetOutputs reads stdout, stderr, and output files for a completed SLURM job,
// then removes the temporary files from the remote host.
func GetOutputs(job api.SlurmJob, runCmd sshutil.CmdRunner) (api.CmdOutput, error) {
	baseErr := fmt.Sprintf("get outputs for job %s (cmd %d)", job.JobID, job.CmdID)

	// Schedule remote cleanup regardless of outcome.
	cleanupCmd := fmt.Sprintf("rm -rf %s %s %s", job.OutPath, job.ErrPath, job.TmpOutputHostPath)
	defer runCmd(cleanupCmd) //nolint:errcheck

	var err error
	out := api.CmdOutput{
		ID:          job.CmdID,
		RawOutputs:  make(map[string]string),
		OutputFiles: make([]string, 0),
	}

	// Empty paths are allowed (e.g. caller only wants stderr for a failed job).
	if job.OutPath != "" {
		if out.StdOut, err = readRemoteFile(job.OutPath, runCmd); err != nil {
			return api.CmdOutput{}, fmt.Errorf("%s: %w", baseErr, err)
		}
	}
	if job.ErrPath != "" {
		if out.StdErr, err = readRemoteFile(job.ErrPath, runCmd); err != nil {
			return api.CmdOutput{}, fmt.Errorf("%s: %w", baseErr, err)
		}
	}

	if job.TmpOutputHostPath == "" {
		out.RawOutputs, out.OutputFiles = processRawCmdOutputs(nil, job.ExpOutFilePnames)
		return out, nil
	}

	lsCmd := fmt.Sprintf("ls -1 %s", job.TmpOutputHostPath)
	lsOut, err := runCmd(lsCmd)
	if err != nil {
		return api.CmdOutput{}, fmt.Errorf("%s: ls %s exit %d stderr %s: %w",
			baseErr, job.TmpOutputHostPath, lsOut.ExitCode, lsOut.StdErr, err)
	}

	rawOutputs := make(map[string]string)
	for _, file := range strings.Split(strings.TrimSpace(lsOut.StdOut), "\n") {
		if file == "" {
			continue
		}
		filePath := filepath.Join(job.TmpOutputHostPath, file)
		val, err := readRemoteFile(filePath, runCmd)
		if err != nil {
			return api.CmdOutput{}, fmt.Errorf("%s: read output file %s: %w", baseErr, filePath, err)
		}
		rawOutputs[file] = val
	}

	out.RawOutputs, out.OutputFiles = processRawCmdOutputs(rawOutputs, job.ExpOutFilePnames)
	return out, nil
}

// readRemoteFile waits for path to exist on the remote host, then cats it.
func readRemoteFile(path string, runCmd sshutil.CmdRunner) (string, error) {
	if !AwaitFileExistence(path, runCmd) {
		return "", fmt.Errorf("remote file %s does not exist", path)
	}
	catOut, err := runCmd(fmt.Sprintf("cat %s", path))
	if err != nil {
		return "", fmt.Errorf("cat %s: exit %d stderr %s: %w", path, catOut.ExitCode, catOut.StdErr, err)
	}
	return catOut.StdOut, nil
}

// processRawCmdOutputs trims trailing newlines from raw file contents and
// extracts any output file paths stored in the expected pname slots.
// Extracted from bwb_scheduler/workflow/bwb_workflow.go.
func processRawCmdOutputs(
	rawOutputs map[string]string,
	expOutFilePnames []string,
) (map[string]string, []string) {
	outKvs := make(map[string]string, len(rawOutputs))
	for pname, contents := range rawOutputs {
		outKvs[pname] = strings.TrimSuffix(contents, "\n")
	}

	outFiles := make([]string, 0)
	for _, pname := range expOutFilePnames {
		if raw, ok := outKvs[pname]; ok {
			vals := strings.Split(raw, "\n")
			if len(vals) > 0 && vals[len(vals)-1] == "" {
				vals = vals[:len(vals)-1]
			}
			outFiles = append(outFiles, vals...)
		}
	}
	return outKvs, outFiles
}
