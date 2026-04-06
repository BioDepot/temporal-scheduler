package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
