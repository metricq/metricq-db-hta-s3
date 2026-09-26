package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Each WAL frame has a sequence number, length, header CRC and payload CRC.
// Replay never discards bytes automatically; incomplete frames block startup.
const frameHeader = 20
const maxFrame = 64 << 20

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type wal struct {
	file   *os.File
	size   int64
	failed error
}

func openWAL(dir string) (*wal, error) {
	if err := makeDurableDir(dir); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "ingest.wal"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("WAL already in use: %w", err)
	}
	d, err := os.Open(dir)
	if err == nil {
		err = d.Sync()
		d.Close()
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &wal{file: f, size: st.Size()}, nil
}
func (w *wal) replay(apply func(uint64, []byte) error) error {
	var offset int64
	var previous uint64
	for offset < w.size {
		head := make([]byte, frameHeader)
		if _, err := w.file.ReadAt(head, offset); err != nil {
			if err == io.EOF {
				return fmt.Errorf("incomplete WAL header at offset %d; no data removed", offset)
			}
			return err
		}
		if crc32.Checksum(head[:12], crcTable) != binary.LittleEndian.Uint32(head[12:16]) {
			return fmt.Errorf("WAL header checksum mismatch at %d", offset)
		}
		n := binary.LittleEndian.Uint32(head[8:12])
		seq := binary.LittleEndian.Uint64(head[:8])
		if n > maxFrame || seq == 0 || seq <= previous {
			return fmt.Errorf("invalid WAL frame at %d", offset)
		}
		if offset+frameHeader+int64(n) > w.size {
			return fmt.Errorf("incomplete WAL payload at offset %d; no data removed", offset)
		}
		b := make([]byte, n)
		if _, err := w.file.ReadAt(b, offset+frameHeader); err != nil {
			return err
		}
		crc := crc32.Checksum(b, crcTable)
		if crc != binary.LittleEndian.Uint32(head[16:]) {
			return fmt.Errorf("WAL checksum mismatch at %d", offset)
		}
		if err := apply(seq, b); err != nil {
			return err
		}
		offset += frameHeader + int64(n)
		previous = seq
	}
	_, err := w.file.Seek(w.size, io.SeekStart)
	return err
}
func (w *wal) append(seq uint64, b []byte) error {
	if w.failed != nil {
		return w.failed
	}
	if len(b) > maxFrame {
		return fmt.Errorf("WAL frame exceeds %d bytes", maxFrame)
	}
	frame := make([]byte, frameHeader+len(b))
	binary.LittleEndian.PutUint64(frame[:8], seq)
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(b)))
	copy(frame[frameHeader:], b)
	binary.LittleEndian.PutUint32(frame[12:16], crc32.Checksum(frame[:12], crcTable))
	binary.LittleEndian.PutUint32(frame[16:20], crc32.Checksum(b, crcTable))
	n, err := w.file.WriteAt(frame, w.size)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = w.file.Sync()
	}
	if err != nil {
		w.failed = fmt.Errorf("WAL durability failed; restart required: %w", err)
		return w.failed
	}
	w.size += int64(len(frame))
	return nil
}
func (w *wal) truncate(n int64) error {
	if err := w.file.Truncate(n); err != nil {
		w.failed = err
		return err
	}
	if err := w.file.Sync(); err != nil {
		w.failed = err
		return err
	}
	w.size = n
	_, err := w.file.Seek(n, io.SeekStart)
	return err
}

// bind prevents accidentally replaying a WAL into a different bucket/prefix and
// detects restoration of an older remote manifest after a local GC checkpoint.
func (w *wal) bind(identity string, remote uint64) error {
	dir := filepath.Dir(w.file.Name())
	path := filepath.Join(dir, "identity")
	b, err := os.ReadFile(path)
	if err == nil {
		if !bytes.Equal(b, []byte(identity)) {
			return fmt.Errorf("WAL belongs to another backend namespace")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err = w.atomicFile("identity", []byte(identity)); err != nil {
			return err
		}
	} else {
		return err
	}
	b, err = os.ReadFile(filepath.Join(dir, "checkpoint"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(b) != 8 {
		return fmt.Errorf("invalid local checkpoint")
	}
	if binary.LittleEndian.Uint64(b) > remote {
		return fmt.Errorf("remote manifest is older than local WAL GC checkpoint")
	}
	return nil
}
func (w *wal) atomicFile(name string, b []byte) error {
	dir := filepath.Dir(w.file.Name())
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (w *wal) checkpoint(seq uint64) error {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, seq)
	return w.atomicFile("checkpoint", b)
}

func makeDurableDir(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if info, err := os.Stat(absolute); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("WAL path is not a directory")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(absolute)
	if err = makeDurableDir(parent); err != nil {
		return err
	}
	if err = os.Mkdir(absolute, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	d, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
