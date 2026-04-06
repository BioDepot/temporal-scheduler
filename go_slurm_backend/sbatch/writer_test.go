package sbatch

import (
	"bytes"
	"strings"
	"testing"

	"go-slurm-backend/api"
)

func ptr[T any](v T) *T { return &v }

func TestWriteSbatchFile_BasicDirectives(t *testing.T) {
	var buf bytes.Buffer
	cmd := api.SlurmCmdTemplate{
		ID:        1,
		ImageName: "myimage.sif",
		BaseCmd:   []string{"python", "run.py"},
		Flags:     []string{"--verbose"},
		Args:      []string{"input.txt"},
		Envs:      map[string]string{},
		ResourceReqs: api.ResourceVector{
			Cpus:  4,
			MemMb: 8192,
		},
	}
	jobConfig := api.SlurmJobConfig{}

	outPath, errPath, err := WriteSbatchFile(&buf, cmd, nil, jobConfig, "/storage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(outPath, "/storage/slurm/") || !strings.HasSuffix(outPath, ".out") {
		t.Errorf("unexpected outPath: %s", outPath)
	}
	if !strings.HasPrefix(errPath, "/storage/slurm/") || !strings.HasSuffix(errPath, ".err") {
		t.Errorf("unexpected errPath: %s", errPath)
	}

	script := buf.String()
	if !strings.HasPrefix(script, "#!/bin/bash\n") {
		t.Error("missing shebang")
	}
	if !strings.Contains(script, "#SBATCH --cpus-per-task=4\n") {
		t.Errorf("missing cpus directive in:\n%s", script)
	}
	if !strings.Contains(script, "#SBATCH --mem=8192MB\n") {
		t.Errorf("missing mem directive in:\n%s", script)
	}
}

func TestWriteSbatchFile_JobConfigOverrides(t *testing.T) {
	var buf bytes.Buffer
	cmd := api.SlurmCmdTemplate{
		ImageName:    "img.sif",
		ResourceReqs: api.ResourceVector{Cpus: 1, MemMb: 512},
	}
	jobConfig := api.SlurmJobConfig{
		Partition:   ptr("gpu"),
		Time:        ptr("02:00:00"),
		Mem:         ptr("16G"),
		CpusPerTask: ptr(8),
		Gpus:        ptr("1"),
		Ntasks:      ptr(2),
		Nodes:       ptr(1),
		Modules:     &[]string{"cuda/11.8"},
	}

	_, _, err := WriteSbatchFile(&buf, cmd, nil, jobConfig, "/store")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	script := buf.String()
	for _, want := range []string{
		"#SBATCH --partition=gpu",
		"#SBATCH --time=02:00:00",
		"#SBATCH --mem=16G",
		"#SBATCH --cpus-per-task=8",
		"#SBATCH --gpus=1",
		"#SBATCH --ntasks=2",
		"#SBATCH --nodes=1",
		"module load cuda/11.8",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("missing %q in script:\n%s", want, script)
		}
	}
}

func TestWriteSbatchFile_GpuFlagPropagated(t *testing.T) {
	var buf bytes.Buffer
	cmd := api.SlurmCmdTemplate{
		ImageName:    "gpu.sif",
		ResourceReqs: api.ResourceVector{Gpus: 1},
	}
	_, _, err := WriteSbatchFile(&buf, cmd, nil, api.SlurmJobConfig{}, "/s")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	script := buf.String()
	if !strings.Contains(script, "--nv") {
		t.Errorf("GPU flag --nv not found in script:\n%s", script)
	}
}

func TestRandID_Length(t *testing.T) {
	id := RandID()
	if len(id) != 16 {
		t.Errorf("RandID length = %d, want 16", len(id))
	}
	id2 := RandID()
	if id == id2 {
		t.Error("RandID returned identical values on two calls")
	}
}
