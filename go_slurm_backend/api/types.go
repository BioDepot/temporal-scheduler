// Package api defines the request/response types for the Go SLURM backend
// HTTP service and the core data structures shared across packages.
//
// Type names and JSON field names mirror the Python scheduler_types.py Slurm*
// structs so that the Python client shim can serialise/deserialise without
// any intermediate translation layer.
package api

// SshConfig holds SSH connection parameters for the SLURM login node.
type SshConfig struct {
	IpAddr       string  `json:"ip_addr"`
	Port         int     `json:"port,omitempty"`          // SSH login port, defaults to 22
	User         string  `json:"user"`
	TransferAddr string  `json:"transfer_addr"`
	TransferPort int     `json:"transfer_port,omitempty"` // rsync transfer port; 0 = use Port
	StorageDir   string  `json:"storage_dir"`
	CmdPrefix    *string `json:"cmd_prefix,omitempty"`
}

// SlurmJobConfig holds per-job SLURM submission directives.
// All fields are optional; absent fields fall back to the resource
// requirements embedded in SlurmCmdTemplate.ResourceReqs.
type SlurmJobConfig struct {
	MaxRetries  *int      `json:"max_retries,omitempty"`
	Mem         *string   `json:"mem,omitempty"`
	CpusPerTask *int      `json:"cpus_per_task,omitempty"`
	Gpus        *string   `json:"gpus,omitempty"`
	Nodes       *int      `json:"nodes,omitempty"`
	Ntasks      *int      `json:"ntasks,omitempty"`
	Time        *string   `json:"time,omitempty"`
	Partition   *string   `json:"partition,omitempty"`
	Modules     *[]string `json:"modules,omitempty"`
}

// ResourceVector holds compute resource requirements for a job.
type ResourceVector struct {
	Cpus  int `json:"cpus"`
	Gpus  int `json:"gpus"`
	MemMb int `json:"mem_mb"`
}

// SlurmCmdTemplate is the subset of command data required to write an
// sbatch script.  It is a projection of the Python SlurmCmdObj /
// SlurmContainerCmdParams types.
type SlurmCmdTemplate struct {
	ID            int               `json:"id"`
	ImageName     string            `json:"image_name"`
	BaseCmd       []string          `json:"base_cmd"`
	Flags         []string          `json:"flags"`
	Args          []string          `json:"args"`
	Envs          map[string]string `json:"envs"`
	OutFilePnames []string          `json:"out_file_pnames"`
	ResourceReqs  ResourceVector    `json:"resource_reqs"`
	// RawCmd, when non-empty, is written directly into the sbatch script instead
	// of using FormSingularityCmd.  Used by the Python shim which builds the
	// container command itself and passes the pre-built string here.
	RawCmd string `json:"raw_cmd,omitempty"`
}

// SlurmJob tracks a submitted SLURM batch job.
type SlurmJob struct {
	CmdID             int      `json:"cmd_id"`
	JobID             string   `json:"job_id"`
	TmpOutputHostPath string   `json:"tmp_output_host_path"`
	ExpOutFilePnames  []string `json:"exp_out_file_pnames"`
	SbatchPath        string   `json:"sbatch_path"`
	OutPath           string   `json:"out_path"`
	ErrPath           string   `json:"err_path"`
}

// CmdOutput is the result of a completed SLURM job, returned to the
// Python scheduler via the get_slurm_outputs endpoint.
type CmdOutput struct {
	ID          int               `json:"id"`
	StdOut      string            `json:"stdout"`
	StdErr      string            `json:"stderr"`
	RawOutputs  map[string]string `json:"raw_outputs"`
	OutputFiles []string          `json:"output_files"`
	Success     bool              `json:"success"`
}

// ---------- HTTP request/response pairs ----------

// SetupVolumesRequest asks the backend to mkdir the required directories
// on the SLURM login node.
type SetupVolumesRequest struct {
	SshConfig SshConfig `json:"ssh_config"`
	StorageID string    `json:"storage_id"`
	Dirs      []string  `json:"dirs"`
}

type SetupVolumesResponse struct {
	Error string `json:"error,omitempty"`
}

// FileUploadRequest rsync-uploads a local file to the SLURM login node.
type FileUploadRequest struct {
	SshConfig     SshConfig `json:"ssh_config"`
	LocalSrcPath  string    `json:"local_src_path"`
	RemoteDstPath string    `json:"remote_dst_path"`
}

type FileUploadResponse struct {
	Error string `json:"error,omitempty"`
}

// StartJobRequest submits a new SLURM batch job.
type StartJobRequest struct {
	SshConfig SshConfig         `json:"ssh_config"`
	Cmd       SlurmCmdTemplate  `json:"cmd"`
	JobConfig SlurmJobConfig    `json:"job_config"`
	Volumes   map[string]string `json:"volumes"`
	// ExtraDirs lists additional directories that must be created (mkdir -p) on
	// the login node before the sbatch file is uploaded.  Used by the Python
	// shim to create per-job container output directories.
	ExtraDirs []string `json:"extra_dirs,omitempty"`
}

type StartJobResponse struct {
	Job   SlurmJob `json:"job"`
	Error string   `json:"error,omitempty"`
}

// PollJobsRequest queries sacct for the status of outstanding jobs.
type PollJobsRequest struct {
	SshConfig SshConfig `json:"ssh_config"`
	JobIDs    []string  `json:"job_ids"`
}

// SacctResult is the parsed status of one SLURM job returned by sacct.
type SacctResult struct {
	JobID    string `json:"job_id"`
	State    string `json:"state"`
	ExitCode string `json:"exit_code"`
	Done     bool   `json:"done"`
	Failed   bool   `json:"failed"`
	Fatal    bool   `json:"fatal"`
}

type PollJobsResponse struct {
	Results map[string]SacctResult `json:"results"`
	Error   string                 `json:"error,omitempty"`
}

// GetOutputsRequest retrieves stdout/stderr and output files for finished jobs.
type GetOutputsRequest struct {
	SshConfig SshConfig  `json:"ssh_config"`
	Jobs      []SlurmJob `json:"jobs"`
}

type GetOutputsResponse struct {
	Outputs []CmdOutput `json:"outputs"`
	Error   string      `json:"error,omitempty"`
}

// FileDownloadRequest rsync-downloads a file from the SLURM login node.
type FileDownloadRequest struct {
	SshConfig     SshConfig `json:"ssh_config"`
	RemoteSrcPath string    `json:"remote_src_path"`
	LocalDstPath  string    `json:"local_dst_path"`
}

type FileDownloadResponse struct {
	Error string `json:"error,omitempty"`
}

// ValidateTransferConnectivityRequest checks SSH, rsync, and storage health
// before any data transfers begin.
type ValidateTransferConnectivityRequest struct {
	SshConfig        SshConfig `json:"ssh_config"`
	RemoteStorageDir string    `json:"remote_storage_dir"`
}

type ValidateTransferConnectivityResponse struct {
	Status      string   `json:"status"`
	Checks      []string `json:"checks,omitempty"`
	DiskAvailGB int      `json:"disk_avail_gb,omitempty"`
	Error       string   `json:"error,omitempty"`
}
