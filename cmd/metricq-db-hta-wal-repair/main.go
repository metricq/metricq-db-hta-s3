package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/metricq/metricq-db-hta-go/engine"
)

func main() {
	dir := flag.String("wal-dir", "", "directory containing ingest.wal")
	apply := flag.Bool("apply", false, "archive and truncate the WAL at the inspected boundary")
	digest := flag.String("expected-sha256", "", "SHA-256 reported by inspection")
	offset := flag.Int64("truncate-at", -1, "valid_bytes reported by inspection")
	backup := flag.String("backup", "", "new path for a complete durable copy of the original WAL")
	flag.Parse()
	if *dir == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	var (
		r   engine.WALReport
		err error
	)
	if *apply {
		r, err = engine.RepairWAL(*dir, *digest, *offset, *backup)
	} else {
		r, err = engine.InspectWAL(*dir)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *apply {
		fmt.Fprintf(os.Stderr, "WAL truncated to %d bytes; original archived at %s\n", r.ValidBytes, *backup)
	}
}
