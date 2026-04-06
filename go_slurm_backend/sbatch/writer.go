package sbatch

import (
	"crypto/rand"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"go-slurm-backend/api"
)

// WriteSbatchFile writes an sbatch script to outStream for the given command
// and job configuration.  It returns the remote paths where SLURM will write
// stdout and stderr, or an error.
// Extracted from bwb_scheduler/workflow/slurm_poller.go.
func WriteSbatchFile(
	outStream io.Writer,
	cmd api.SlurmCmdTemplate,
	volumes map[string]string,
	jobConfig api.SlurmJobConfig,
	storageDir string,
) (outPath, errPath string, err error) {
	outPath = filepath.Join(storageDir, "slurm", randomString(16)+".out")
	errPath = filepath.Join(storageDir, "slurm", randomString(16)+".err")

	fmt.Fprintln(outStream, "#!/bin/bash")
	fmt.Fprintf(outStream, "#SBATCH --output=%s\n", outPath)
	fmt.Fprintf(outStream, "#SBATCH --error=%s\n", errPath)

	if jobConfig.Partition != nil {
		fmt.Fprintf(outStream, "#SBATCH --partition=%s\n", *jobConfig.Partition)
	}
	if jobConfig.Time != nil {
		fmt.Fprintf(outStream, "#SBATCH --time=%s\n", *jobConfig.Time)
	}
	if jobConfig.Ntasks != nil {
		fmt.Fprintf(outStream, "#SBATCH --ntasks=%d\n", *jobConfig.Ntasks)
	}
	if jobConfig.Nodes != nil {
		fmt.Fprintf(outStream, "#SBATCH --nodes=%d\n", *jobConfig.Nodes)
	}
	if jobConfig.Gpus != nil {
		fmt.Fprintf(outStream, "#SBATCH --gpus=%s\n", *jobConfig.Gpus)
	}

	if jobConfig.Mem != nil {
		fmt.Fprintf(outStream, "#SBATCH --mem=%s\n", *jobConfig.Mem)
	} else {
		fmt.Fprintf(outStream, "#SBATCH --mem=%dMB\n", cmd.ResourceReqs.MemMb)
	}

	if jobConfig.CpusPerTask != nil {
		fmt.Fprintf(outStream, "#SBATCH --cpus-per-task=%d\n", *jobConfig.CpusPerTask)
	} else {
		fmt.Fprintf(outStream, "#SBATCH --cpus-per-task=%d\n", cmd.ResourceReqs.Cpus)
	}

	if jobConfig.Modules != nil {
		for _, module := range *jobConfig.Modules {
			fmt.Fprintf(outStream, "module load %s\n", module)
		}
	}

	localSifPath := filepath.Join(storageDir, "images", cmd.ImageName)
	useGpu := jobConfig.Gpus != nil || cmd.ResourceReqs.Gpus > 0
	cmdStr, envs := FormSingularityCmd(cmd, volumes, localSifPath, useGpu)
	fmt.Fprintf(outStream, "%s %s", strings.Join(envs, " "), cmdStr)

	return outPath, errPath, nil
}

// RandID returns a random 16-character hex string suitable for use as a unique
// filename or directory suffix.
// Extracted from bwb_scheduler/workflow/bwb_workflow.go.
func RandID() string {
	return randomString(16)
}

func randomString(length int) string {
	b := make([]byte, length+2)
	rand.Read(b) //nolint:errcheck // crypto/rand.Read never returns an error
	return fmt.Sprintf("%x", b)[2 : length+2]
}
