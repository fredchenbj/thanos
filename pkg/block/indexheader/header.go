package indexheader

import (
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"
)

// NotFoundRangeErr is an error returned by PostingsOffset when there is no posting for given name and value pairs.
var NotFoundRangeErr = errors.New("range not found")

// Reader is an interface allowing to read essential, minimal number of index entries from the small portion of index file called header.
type Reader interface {
	io.Closer

	IndexVersion() int
	// TODO(bwplotka): Move to PostingsOffsets(name string, value ...string) []index.Range and benchmark.
	PostingsOffset(name string, value string) (index.Range, error)
	LookupSymbol(o uint32) (string, error)
	LabelValues(name string) ([]string, error)
	LabelNames() []string
}
