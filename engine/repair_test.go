package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/metricq/metricq-db-hta-go/hta"
)

func TestExplicitWALRepairPreservesVerifiedPrefixAndOriginal(t *testing.T) {
	s := newStore()
	dir := t.TempDir()
	e := openTest(t, s, dir)
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	clean, err := InspectWAL(dir)
	if err != nil || clean.Damage != "" {
		t.Fatalf("clean WAL: %+v, %v", clean, err)
	}
	if _, err := RepairWAL(dir, clean.SHA256, clean.ValidBytes, filepath.Join(dir, "backup")); err == nil {
		t.Fatal("repaired healthy WAL")
	}
	e, err = Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	ingest(t, e, hta.Point{Time: 200, Value: 2})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "ingest.wal")
	f, err := os.OpenFile(walPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, clean.ValidBytes+frameHeader+5); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	original, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	r, err := InspectWAL(dir)
	if err != nil || r.Damage == "" || r.ValidBytes != clean.ValidBytes {
		t.Fatalf("damage report: %+v, %v", r, err)
	}
	if _, err := RepairWAL(dir, clean.SHA256, r.ValidBytes, filepath.Join(dir, "backup")); err == nil {
		t.Fatal("accepted stale digest")
	}
	if _, err := RepairWAL(dir, r.SHA256, r.ValidBytes+1, filepath.Join(dir, "backup")); err == nil {
		t.Fatal("accepted non-frame boundary")
	}
	if _, err := RepairWAL(dir, r.SHA256, r.ValidBytes, walPath); err == nil {
		t.Fatal("accepted WAL as backup")
	}
	if _, err := Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil); err == nil {
		t.Fatal("database accepted corruption before repair")
	}
	backupPath := filepath.Join(dir, "backup")
	if _, err := RepairWAL(dir, r.SHA256, r.ValidBytes, backupPath); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(backupPath)
	if err != nil || !bytes.Equal(backup, original) {
		t.Fatal("original WAL not preserved")
	}
	trimmed, err := os.ReadFile(walPath)
	if err != nil || !bytes.Equal(trimmed, original[:r.ValidBytes]) {
		t.Fatal("WAL not truncated to verified prefix")
	}
	recovered, err := Open(context.Background(), s, Options{WALDirectory: dir}, testConfig, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.sequence != 1 {
		t.Fatalf("expected one preserved frame, got %d", recovered.sequence)
	}
}

func TestWALRepairRequiresExclusiveLockAndBackup(t *testing.T) {
	dir := t.TempDir()
	e := openTest(t, newStore(), dir)
	ingest(t, e, hta.Point{Time: 100, Value: 1})
	if _, err := InspectWAL(dir); err == nil {
		t.Fatal("inspected WAL while database owns lock")
	}
	e.Close()
	walPath := filepath.Join(dir, "ingest.wal")
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	r, err := InspectWAL(dir)
	if err != nil || r.Damage == "" {
		t.Fatalf("expected damage: %+v, %v", r, err)
	}
	backup := filepath.Join(dir, "backup")
	if err := os.WriteFile(backup, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := RepairWAL(dir, r.SHA256, r.ValidBytes, backup); err == nil {
		t.Fatal("overwrote existing backup")
	}
	stillDamaged, err := InspectWAL(dir)
	if err != nil || stillDamaged.SHA256 != r.SHA256 {
		t.Fatal("failed backup altered WAL")
	}
}
