// Package sbatch contains helpers for building Singularity container commands
// and writing SLURM batch scripts.
// Extracted from bwb_scheduler/parsing/parse_workflow_cmds.go.
package sbatch

import (
	"fmt"
	"strings"

	"go-slurm-backend/api"
)

// FormSingularityCmdPrefix builds the "singularity exec ..." prefix string
// (everything up to but not including the inner command).
func FormSingularityCmdPrefix(
	cmd api.SlurmCmdTemplate,
	volumes map[string]string,
	useGpu bool,
	sifPath string,
) string {
	gpuFlag := ""
	if useGpu {
		gpuFlag = "--nv"
	}

	volumesStr := ""
	for cntPath, hostPath := range volumes {
		volumesStr += fmt.Sprintf("-B %s:%s ", hostPath, cntPath)
	}
	return fmt.Sprintf(
		"singularity exec %s -p -i --containall --cleanenv %s %s",
		gpuFlag, volumesStr, sifPath,
	)
}

// FormSingularityCmd builds the full singularity invocation and returns the
// complete command string along with the SINGULARITYENV_* environment variable
// assignments that must precede it.
func FormSingularityCmd(
	cmd api.SlurmCmdTemplate,
	volumes map[string]string,
	sifPath string,
	useGpu bool,
) (string, []string) {
	envStrs := make([]string, 0, len(cmd.Envs))
	for envK, envV := range cmd.Envs {
		envStrs = append(envStrs, fmt.Sprintf("SINGULARITYENV_%s=%s", envK, envV))
	}

	cmdStr := strings.Join(cmd.BaseCmd, " && ") + " "
	cmdStr += strings.Join(cmd.Flags, " ") + " "
	cmdStr += strings.Join(cmd.Args, " ")

	singularityPrefix := FormSingularityCmdPrefix(cmd, volumes, useGpu, sifPath)
	fullCmd := fmt.Sprintf("%s sh -c '%s'", singularityPrefix, cmdStr)

	return fullCmd, envStrs
}
