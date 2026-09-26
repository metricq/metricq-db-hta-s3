// Package storage defines the durable object contract used by the engine.
package storage

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("object not found")
var ErrConflict = errors.New("object version conflict")

// Store must provide strongly consistent GET and atomic conditional PUT.
// expected=nil permits overwrite; expected=&"" requires absence; otherwise
// expected is the opaque version returned by Get/Put. A future file backend
// must use fsync+atomic rename and serialize conditional updates.
type Store interface {
	// Identity binds a WAL directory to one backend namespace. It must be stable
	// and must never contain credentials.
	Identity() string
	Get(context.Context, string) ([]byte, string, error)
	Put(context.Context, string, []byte, *string) (string, error)
}

// RangeGetter reads a byte range from a sealed object. Production backends
// should implement this so packed blocks are independently addressable.
type RangeGetter interface {
	GetRange(context.Context, string, int64, int64) ([]byte, error)
}
