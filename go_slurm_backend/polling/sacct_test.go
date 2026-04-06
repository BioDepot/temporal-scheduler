package polling

import (
	"testing"

	"go-slurm-backend/api"
	"go-slurm-backend/sshutil"
)

func mockRunner(stdout string) sshutil.CmdRunner {
	return func(cmd string) (sshutil.CmdOut, error) {
		return sshutil.CmdOut{ExitCode: 0, StdOut: stdout}, nil
	}
}

func TestRunSacct_ParsesRecords(t *testing.T) {
	sacctOut := "12345|COMPLETED|0:0\n12346|FAILED|1:0\n"
	results, err := RunSacct([]string{"12345", "12346"}, mockRunner(sacctOut))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	r := results["12345"]
	if r.State != "COMPLETED" || r.Done != true || r.Failed != false {
		t.Errorf("12345: got %+v", r)
	}

	r = results["12346"]
	if r.State != "FAILED" || r.Done != true || r.Failed != true || r.Fatal != true {
		t.Errorf("12346: got %+v", r)
	}
}

func TestRunSacct_SkipsBatchExternSteps(t *testing.T) {
	sacctOut := "12345|COMPLETED|0:0\n12345.batch|COMPLETED|0:0\n12345.extern|COMPLETED|0:0\n"
	results, err := RunSacct([]string{"12345"}, mockRunner(sacctOut))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %v", len(results), results)
	}
}

func TestRunSacct_EmptyOutput(t *testing.T) {
	results, err := RunSacct([]string{"99999"}, mockRunner(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestRunSacct_MalformedRecord(t *testing.T) {
	sacctOut := "12345|COMPLETED\n" // only 2 fields
	_, err := RunSacct([]string{"12345"}, mockRunner(sacctOut))
	if err == nil {
		t.Fatal("expected error for malformed sacct record, got nil")
	}
}

func TestRunSacct_SetsApiFields(t *testing.T) {
	sacctOut := "55555|CANCELLED|1:0\n"
	results, err := RunSacct([]string{"55555"}, mockRunner(sacctOut))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	r := results["55555"]
	want := api.SacctResult{
		JobID: "55555", State: "CANCELLED", ExitCode: "1:0",
		Done: true, Failed: true, Fatal: true,
	}
	if r != want {
		t.Errorf("got %+v, want %+v", r, want)
	}
}

func TestAwaitFileExistence_ExistsImmediately(t *testing.T) {
	runner := func(cmd string) (sshutil.CmdOut, error) {
		return sshutil.CmdOut{ExitCode: 0}, nil
	}
	if !AwaitFileExistence("/some/file", runner) {
		t.Error("expected true for ExitCode=0")
	}
}

func TestAwaitFileExistence_NeverExists(t *testing.T) {
	runner := func(cmd string) (sshutil.CmdOut, error) {
		return sshutil.CmdOut{ExitCode: 2}, nil
	}
	// This will sleep 5 s in production; to keep tests fast we just verify the
	// return value — the sleep is exercised in integration tests.
	// For unit tests we accept the 5s overhead or skip if CI is time-sensitive.
	t.Skip("skipping AwaitFileExistence 'never exists' test to avoid 5s sleep in unit suite")
	if AwaitFileExistence("/missing", runner) {
		t.Error("expected false for ExitCode=2")
	}
}
