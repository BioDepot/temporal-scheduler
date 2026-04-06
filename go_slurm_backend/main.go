// Command go-slurm-backend is a small HTTP sidecar that exposes SLURM
// backend operations (SSH, rsync, sbatch, sacct) over a local JSON API.
// It is intended to run alongside the Python temporal-scheduler and be
// called by the Python slurm_activities shim.
//
// Usage:
//
//	go-slurm-backend [-addr :8765]
package main

import (
	"flag"
	"log"
	"net/http"

	"go-slurm-backend/server"
)

func main() {
	addr := flag.String("addr", ":8765", "listen address (host:port)")
	flag.Parse()

	handler := server.New()

	log.Printf("go-slurm-backend listening on %s", *addr)
	if err := http.ListenAndServe(*addr, handler); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
