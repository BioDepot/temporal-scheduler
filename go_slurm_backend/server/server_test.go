package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go-slurm-backend/api"
)

func testHandler(t *testing.T) http.Handler {
	t.Helper()
	return New()
}

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)
	testHandler(t).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: want 200, got %d", rec.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("healthz: decode error: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("healthz: want status=ok, got %q", body["status"])
	}
}

func TestMalformedJSON_Returns400(t *testing.T) {
	endpoints := []string{
		"/setup_login_node_volumes",
		"/upload_to_slurm_login_node",
		"/start_slurm_job",
		"/poll_slurm",
		"/get_slurm_outputs",
		"/download_from_slurm_login_node",
		"/validate_transfer_connectivity",
	}
	h := testHandler(t)
	for _, ep := range endpoints {
		t.Run(strings.TrimPrefix(ep, "/"), func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", ep, bytes.NewBufferString("not-json"))
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("%s: want 400 for malformed JSON, got %d", ep, rec.Code)
			}
		})
	}
}

func TestUnknownRoute_Returns404(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	testHandler(t).ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown route: want 404, got %d", rec.Code)
	}
}

// TestValidateTransferConnectivity_DecodesTransferPort verifies the wire
// contract between the Python shim and the Go backend: transfer_port must
// survive JSON decoding into SshConfig.  Before the TransferPort field was
// added, this value was silently dropped and the rsync probe would use the
// wrong port.
func TestValidateTransferConnectivity_DecodesTransferPort(t *testing.T) {
	payload := `{
		"ssh_config": {
			"ip_addr": "login.cluster.edu",
			"port": 22,
			"user": "testuser",
			"transfer_addr": "data.cluster.edu",
			"transfer_port": 2222,
			"storage_dir": "/scratch"
		},
		"remote_storage_dir": "/scratch"
	}`

	var req api.ValidateTransferConnectivityRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.SshConfig.TransferPort != 2222 {
		t.Errorf("transfer_port: want 2222, got %d", req.SshConfig.TransferPort)
	}
	if req.SshConfig.TransferAddr != "data.cluster.edu" {
		t.Errorf("transfer_addr: want data.cluster.edu, got %q", req.SshConfig.TransferAddr)
	}
	if req.SshConfig.Port != 22 {
		t.Errorf("port: want 22, got %d", req.SshConfig.Port)
	}
	if req.RemoteStorageDir != "/scratch" {
		t.Errorf("remote_storage_dir: want /scratch, got %q", req.RemoteStorageDir)
	}
}

// TestValidateTransferConnectivity_TransferPortInAllEndpoints proves that
// transfer_port is decoded in the SshConfig used by all endpoints, not just
// validate.  A representative upload request is tested here.
func TestValidateTransferConnectivity_TransferPortInUploadRequest(t *testing.T) {
	payload := `{
		"ssh_config": {
			"ip_addr": "login.cluster.edu",
			"port": 22,
			"user": "testuser",
			"transfer_addr": "data.cluster.edu",
			"transfer_port": 2222,
			"storage_dir": "/scratch"
		},
		"local_src_path": "/local/file.sif",
		"remote_dst_path": "/remote/file.sif"
	}`

	var req api.FileUploadRequest
	if err := json.Unmarshal([]byte(payload), &req); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.SshConfig.TransferPort != 2222 {
		t.Errorf("transfer_port: want 2222, got %d", req.SshConfig.TransferPort)
	}
}

func TestResponsesAreJSON(t *testing.T) {
	// Healthz should always respond with JSON Content-Type.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/healthz", nil)
	testHandler(t).ServeHTTP(rec, req)

	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("healthz: Content-Type = %q, want application/json", ct)
	}
}
