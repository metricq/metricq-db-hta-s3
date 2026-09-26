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

// Deleter removes an object idempotently, including when it is already absent.
type Deleter interface {
	Delete(context.Context, string) error
}

// Statter reports the complete immutable object's physical size.
type Statter interface {
	Stat(context.Context, string) (int64, error)
}

// Lister inventories a registered staging prefix with bounded pages.
type Lister interface {
	List(context.Context, string, string, int32) ([]string, string, error)
}
