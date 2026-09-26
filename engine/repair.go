package engine

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// WALReport describes the first damaged frame. ValidBytes is a frame boundary,
// not a statement that the suffix was unacknowledged or already stored in S3.
type WALReport struct {
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	ValidBytes int64  `json:"valid_bytes"`
	LastSeq    uint64 `json:"last_sequence"`
	Damage     string `json:"damage,omitempty"`
}

func existingWAL(dir string) (*wal, error) {
	path := filepath.Join(dir, "ingest.wal")
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	return openWAL(dir)
}

func inspectLocked(w *wal) (WALReport, error) {
	r := WALReport{Size: w.size}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(w.file, 0, w.size)); err != nil {
		return r, err
	}
	r.SHA256 = hex.EncodeToString(h.Sum(nil))
	for r.ValidBytes < r.Size {
		offset := r.ValidBytes
		if r.Size-offset < frameHeader {
			r.Damage = fmt.Sprintf("incomplete WAL header at offset %d", offset)
			break
		}
		var head [frameHeader]byte
		if _, err := w.file.ReadAt(head[:], offset); err != nil {
			return r, err
		}
		if crc32.Checksum(head[:12], crcTable) != binary.LittleEndian.Uint32(head[12:16]) {
			r.Damage = fmt.Sprintf("WAL header checksum mismatch at offset %d", offset)
			break
		}
		seq := binary.LittleEndian.Uint64(head[:8])
		n := binary.LittleEndian.Uint32(head[8:12])
		if n > maxFrame || seq == 0 || (r.LastSeq != 0 && seq != r.LastSeq+1) {
			r.Damage = fmt.Sprintf("invalid WAL frame at offset %d", offset)
			break
		}
		if int64(n) > r.Size-offset-frameHeader {
			r.Damage = fmt.Sprintf("incomplete WAL payload at offset %d", offset)
			break
		}
		b := make([]byte, n)
		if _, err := w.file.ReadAt(b, offset+frameHeader); err != nil {
			return r, err
		}
		if crc32.Checksum(b, crcTable) != binary.LittleEndian.Uint32(head[16:20]) {
			r.Damage = fmt.Sprintf("WAL payload checksum mismatch at offset %d", offset)
			break
		}
		var data batch
		if err := decode(b, &data); err != nil {
			r.Damage = fmt.Sprintf("WAL payload decode failed at offset %d: %v", offset, err)
			break
		}
		r.ValidBytes += frameHeader + int64(n)
		r.LastSeq = seq
	}
	return r, nil
}

// InspectWAL is read-only with respect to WAL contents and takes the writer lock.
func InspectWAL(dir string) (WALReport, error) {
	w, err := existingWAL(dir)
	if err != nil {
		return WALReport{}, err
	}
	defer w.file.Close()
	return inspectLocked(w)
}

// RepairWAL archives the exact original bytes before an explicit truncation.
// A changed checksum, different boundary, or healthy WAL is never truncated.
func RepairWAL(dir, expectedSHA256 string, truncateAt int64, backupPath string) (WALReport, error) {
	if backupPath == "" || len(expectedSHA256) != 64 {
		return WALReport{}, fmt.Errorf("backup path and inspected SHA-256 are required")
	}
	w, err := existingWAL(dir)
	if err != nil {
		return WALReport{}, err
	}
	defer w.file.Close()
	r, err := inspectLocked(w)
	if err != nil {
		return r, err
	}
	if r.Damage == "" {
		return r, fmt.Errorf("WAL has no damaged frame")
	}
	if r.SHA256 != expectedSHA256 || r.ValidBytes != truncateAt {
		return r, fmt.Errorf("WAL changed or truncation offset is not the last verified frame boundary")
	}
	walPath, err := filepath.Abs(w.file.Name())
	if err != nil {
		return r, err
	}
	backupPath, err = filepath.Abs(backupPath)
	if err != nil {
		return r, err
	}
	if walPath == backupPath {
		return r, fmt.Errorf("backup path must differ from WAL path")
	}
	backup, err := os.OpenFile(backupPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return r, fmt.Errorf("create backup: %w", err)
	}
	backupHash := sha256.New()
	copied, copyErr := io.Copy(io.MultiWriter(backup, backupHash), io.NewSectionReader(w.file, 0, r.Size))
	err = copyErr
	if err == nil && copied != r.Size {
		err = io.ErrUnexpectedEOF
	}
	if err == nil && hex.EncodeToString(backupHash.Sum(nil)) != r.SHA256 {
		err = fmt.Errorf("WAL changed while copying backup")
	}
	if err == nil {
		err = backup.Sync()
	}
	closeErr := backup.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		var d *os.File
		d, err = os.Open(filepath.Dir(backupPath))
		if err == nil {
			err = d.Sync()
			closeErr = d.Close()
			if err == nil {
				err = closeErr
			}
		}
	}
	if err != nil {
		return r, fmt.Errorf("backup not durable; WAL untouched: %w", err)
	}
	if err := w.truncate(truncateAt); err != nil {
		return r, fmt.Errorf("WAL truncation failed; retain backup %s: %w", backupPath, err)
	}
	return r, nil
}
