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
	"sort"
	"strconv"
	"strings"
	"syscall"
)

// Each WAL frame has a sequence number, length, header CRC and payload CRC.
// Replay never discards bytes automatically; incomplete frames block startup.
const frameHeader = 20
const maxFrame = 64 << 20

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// The active segment is ingest.wal. A checkpoint freezes it by renaming it to
// ingest.wal.<last sequence>, so ingestion continues in a new active segment
// while the frozen frames are uploaded. Committed segments are deleted.
type wal struct {
	dir    string
	lock   *os.File
	file   *os.File
	size   int64
	frozen []walSegment
	failed error
}

type walSegment struct {
	path string
	last uint64
	size int64
}

const activeWAL = "ingest.wal"

func openWAL(dir string) (*wal, error) {
	if err := makeDurableDir(dir); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("WAL already in use: %w", err)
	}
	w := &wal{dir: dir, lock: lock}
	if err = w.open(); err != nil {
		lock.Close()
		return nil, err
	}
	return w, nil
}

func (w *wal) open() error {
	// A crash during rotation can leave an unused, never written new segment.
	if err := os.Remove(filepath.Join(w.dir, activeWAL+".new")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		suffix, ok := strings.CutPrefix(entry.Name(), activeWAL+".")
		if !ok {
			continue
		}
		last, err := strconv.ParseUint(suffix, 10, 64)
		if err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		w.frozen = append(w.frozen, walSegment{path: filepath.Join(w.dir, entry.Name()), last: last, size: info.Size()})
	}
	sort.Slice(w.frozen, func(i, j int) bool { return w.frozen[i].last < w.frozen[j].last })
	f, err := os.OpenFile(filepath.Join(w.dir, activeWAL), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err = syncDir(w.dir); err != nil {
		f.Close()
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	w.file, w.size = f, st.Size()
	return nil
}

func (w *wal) close() error {
	err := w.file.Close()
	if lockErr := w.lock.Close(); err == nil {
		err = lockErr
	}
	return err
}

// total counts every byte not yet covered by a published checkpoint.
func (w *wal) total() int64 {
	n := w.size
	for _, s := range w.frozen {
		n += s.size
	}
	return n
}

// rotate freezes the active segment, whose last durable frame is last.
func (w *wal) rotate(last uint64) error {
	if w.failed != nil {
		return w.failed
	}
	if w.size == 0 {
		return nil
	}
	next := filepath.Join(w.dir, activeWAL+".new")
	f, err := os.OpenFile(next, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	frozen := filepath.Join(w.dir, fmt.Sprintf("%s.%020d", activeWAL, last))
	if err = os.Rename(filepath.Join(w.dir, activeWAL), frozen); err != nil {
		f.Close()
		os.Remove(next)
		return err
	}
	// From here the active name is missing until the second rename; failing now
	// would leave appends without a durable home.
	if err = os.Rename(next, filepath.Join(w.dir, activeWAL)); err == nil {
		err = syncDir(w.dir)
	}
	if err != nil {
		f.Close()
		w.failed = fmt.Errorf("WAL rotation failed; restart required: %w", err)
		return w.failed
	}
	w.file.Close()
	w.frozen = append(w.frozen, walSegment{path: frozen, last: last, size: w.size})
	w.file, w.size = f, 0
	return nil
}

// release deletes frozen segments covered by a published checkpoint.
func (w *wal) release(checkpoint uint64) error {
	kept := w.frozen[:0]
	removed := false
	for _, s := range w.frozen {
		if s.last > checkpoint {
			kept = append(kept, s)
			continue
		}
		if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			w.failed = err
			return err
		}
		removed = true
	}
	w.frozen = kept
	if removed {
		if err := syncDir(w.dir); err != nil {
			w.failed = err
			return err
		}
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (w *wal) replay(apply func(uint64, []byte) error) error {
	var previous uint64
	for _, s := range w.frozen {
		f, err := os.Open(s.path)
		if err != nil {
			return err
		}
		err = replayFile(f, s.size, &previous, apply)
		f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(s.path), err)
		}
	}
	if err := replayFile(w.file, w.size, &previous, apply); err != nil {
		return err
	}
	_, err := w.file.Seek(w.size, io.SeekStart)
	return err
}

func replayFile(file *os.File, size int64, previous *uint64, apply func(uint64, []byte) error) error {
	var offset int64
	for offset < size {
		head := make([]byte, frameHeader)
		if _, err := file.ReadAt(head, offset); err != nil {
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
		if n > maxFrame || seq == 0 || seq <= *previous {
			return fmt.Errorf("invalid WAL frame at %d", offset)
		}
		if offset+frameHeader+int64(n) > size {
			return fmt.Errorf("incomplete WAL payload at offset %d; no data removed", offset)
		}
		b := make([]byte, n)
		if _, err := file.ReadAt(b, offset+frameHeader); err != nil {
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
		*previous = seq
	}
	return nil
}
func (w *wal) append(seq uint64, b []byte) error {
	if len(b) > maxFrame {
		return fmt.Errorf("WAL frame exceeds %d bytes", maxFrame)
	}
	return w.write(appendFrame(nil, seq, b))
}

// appendFrame encodes one frame; write makes several frames durable at once.
func appendFrame(dst []byte, seq uint64, b []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, frameHeader)...)
	dst = append(dst, b...)
	frame := dst[start:]
	binary.LittleEndian.PutUint64(frame[:8], seq)
	binary.LittleEndian.PutUint32(frame[8:12], uint32(len(b)))
	binary.LittleEndian.PutUint32(frame[12:16], crc32.Checksum(frame[:12], crcTable))
	binary.LittleEndian.PutUint32(frame[16:20], crc32.Checksum(b, crcTable))
	return dst
}

func (w *wal) write(frames []byte) error {
	if w.failed != nil {
		return w.failed
	}
	n, err := w.file.WriteAt(frames, w.size)
	if err == nil && n != len(frames) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = w.file.Sync()
	}
	if err != nil {
		w.failed = fmt.Errorf("WAL durability failed; restart required: %w", err)
		return w.failed
	}
	w.size += int64(len(frames))
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
	dir := w.dir
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
	dir := w.dir
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
