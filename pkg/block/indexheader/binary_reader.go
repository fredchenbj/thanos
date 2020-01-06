package indexheader

import (
	"context"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
)

const (
	// BinaryFormatV1 represents first version of index-header file.
	BinaryFormatV1 = 1

	indexTOCLen  = 6*8 + crc32.Size
	binaryTOCLen = 2*8 + crc32.Size
	// headerLen represents number of bytes reserved of index header for header.
	headerLen = 4 + 1 + 1 + 8

	// MagicIndex are 4 bytes at the head of an index-header file.
	MagicIndex = 0xBAAAD792

	symbolFactor = 32
)

// The table gets initialized with sync.Once but may still cause a race
// with any other use of the crc32 package anywhere. Thus we initialize it
// before.
var castagnoliTable *crc32.Table

func init() {
	castagnoliTable = crc32.MakeTable(crc32.Castagnoli)
}

// newCRC32 initializes a CRC32 hash with a preconfigured polynomial, so the
// polynomial may be easily changed in one location at a later time, if necessary.
func newCRC32() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// BinaryTOC is a table of content for index-header file.
type BinaryTOC struct {
	// Symbols holds start to the same symbols section as index related to this index header.
	Symbols uint64
	// PostingsTable holds start to the the same Postings Offset Table section as index related to this index header.
	PostingsOffsetTable uint64
}

// WriteBinary build index-header file from the pieces of index in object storage.
func WriteBinary(ctx context.Context, bkt objstore.BucketReader, id ulid.ULID, fn string) (err error) {
	ir, indexVersion, err := newChunkedIndexReader(ctx, bkt, id)
	if err != nil {
		return errors.Wrap(err, "new index reader")
	}

	bw, err := newBinaryWriter(fn)
	if err != nil {
		return errors.Wrap(err, "new binary index header writer")
	}
	defer runutil.CloseWithErrCapture(&err, bw, "close binary writer for %s", fn)

	if err := bw.AddIndexMeta(indexVersion, ir.toc.PostingsTable); err != nil {
		return errors.Wrap(err, "add index meta")
	}

	if err := ir.CopySymbols(bw.SymbolsWriter()); err != nil {
		return err
	}

	if err := bw.f.Flush(); err != nil {
		return err
	}

	if err := ir.CopyPostingsOffsets(bw.PostingOffsetsWriter()); err != nil {
		return err
	}

	if err := bw.f.Flush(); err != nil {
		return err
	}

	if err := bw.WriteTOC(); err != nil {
		return errors.Wrap(err, "write index header TOC")
	}
	return nil
}

type chunkedIndexReader struct {
	ctx  context.Context
	path string
	size uint64
	bkt  objstore.BucketReader
	toc  *index.TOC
}

func newChunkedIndexReader(ctx context.Context, bkt objstore.BucketReader, id ulid.ULID) (*chunkedIndexReader, int, error) {
	indexFilepath := filepath.Join(id.String(), block.IndexFilename)
	size, err := bkt.ObjectSize(ctx, indexFilepath)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "get object size of %s", indexFilepath)
	}

	rc, err := bkt.GetRange(ctx, indexFilepath, 0, index.HeaderLen)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "get TOC from object storage of %s", indexFilepath)
	}

	b, err := ioutil.ReadAll(rc)
	if err != nil {
		runutil.CloseWithErrCapture(&err, rc, "close reader")
		return nil, 0, errors.Wrapf(err, "get header from object storage of %s", indexFilepath)
	}

	if err := rc.Close(); err != nil {
		return nil, 0, errors.Wrap(err, "close reader")
	}

	if m := binary.BigEndian.Uint32(b[0:4]); m != index.MagicIndex {
		return nil, 0, errors.Errorf("invalid magic number %x for %s", m, indexFilepath)
	}

	version := int(b[4:5][0])

	if version != index.FormatV1 && version != index.FormatV2 {
		return nil, 0, errors.Errorf("not supported index file version %d of %s", version, indexFilepath)
	}

	ir := &chunkedIndexReader{
		ctx:  ctx,
		path: indexFilepath,
		size: size,
		bkt:  bkt,
	}

	toc, err := ir.readTOC()
	if err != nil {
		return nil, 0, err
	}
	ir.toc = toc

	return ir, version, nil
}

func (r *chunkedIndexReader) readTOC() (*index.TOC, error) {
	rc, err := r.bkt.GetRange(r.ctx, r.path, int64(r.size-indexTOCLen-crc32.Size), indexTOCLen+crc32.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "get TOC from object storage of %s", r.path)
	}

	tocBytes, err := ioutil.ReadAll(rc)
	if err != nil {
		runutil.CloseWithErrCapture(&err, rc, "close toc reader")
		return nil, errors.Wrapf(err, "get TOC from object storage of %s", r.path)
	}

	if err := rc.Close(); err != nil {
		return nil, errors.Wrap(err, "close toc reader")
	}

	toc, err := index.NewTOCFromByteSlice(realByteSlice(tocBytes))
	if err != nil {
		return nil, errors.Wrap(err, "new TOC")
	}
	return toc, nil
}

func (r *chunkedIndexReader) CopySymbols(w io.Writer) (err error) {
	rc, err := r.bkt.GetRange(r.ctx, r.path, int64(r.toc.Symbols), int64(r.toc.Series-r.toc.Symbols))
	if err != nil {
		return errors.Wrapf(err, "get symbols from object storage of %s", r.path)
	}
	runutil.CloseWithErrCapture(&err, rc, "close reader")

	if _, err := io.Copy(w, rc); err != nil {
		return errors.Wrap(err, "copy symbols")
	}

	return rc.Close()
}

func (r *chunkedIndexReader) CopyPostingsOffsets(w io.Writer) (err error) {
	rc, err := r.bkt.GetRange(r.ctx, r.path, int64(r.toc.PostingsTable), int64(r.size-r.toc.PostingsTable))
	if err != nil {
		return errors.Wrapf(err, "get posting offset table from object storage of %s", r.path)
	}
	runutil.CloseWithErrCapture(&err, rc, "close reader")

	if _, err := io.Copy(w, rc); err != nil {
		return errors.Wrap(err, "copy posting offset")
	}

	return rc.Close()
}

type binaryWriter struct {
	f *index.FileWriter

	toc BinaryTOC

	// Reusable memory.
	buf encoding.Encbuf

	crc32 hash.Hash
}

func newBinaryWriter(fn string) (w *binaryWriter, err error) {
	dir := filepath.Dir(fn)

	df, err := fileutil.OpenDir(dir)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithErrCapture(&err, df, "dir close")

	if err := os.RemoveAll(fn); err != nil {
		return nil, errors.Wrap(err, "remove any existing index at path")
	}

	f, err := index.NewFileWriter(fn)
	if err != nil {
		return nil, err
	}
	if err := df.Sync(); err != nil {
		return nil, errors.Wrap(err, "sync dir")
	}

	w = &binaryWriter{
		f: f,

		// Reusable memory.
		buf:   encoding.Encbuf{B: make([]byte, 0, 1<<22)},
		crc32: newCRC32(),
	}

	w.buf.Reset()
	w.buf.PutBE32(MagicIndex)
	w.buf.PutByte(BinaryFormatV1)

	return w, w.f.Write(w.buf.Get())
}

func (w *binaryWriter) AddIndexMeta(indexVersion int, indexPostingOffsetTable uint64) error {
	w.buf.Reset()
	w.buf.PutByte(byte(indexVersion))
	w.buf.PutBE64(indexPostingOffsetTable)
	return w.f.Write(w.buf.Get())
}

func (w *binaryWriter) SymbolsWriter() io.Writer {
	w.toc.Symbols = w.f.Pos()
	return w
}

func (w *binaryWriter) PostingOffsetsWriter() io.Writer {
	w.toc.PostingsOffsetTable = w.f.Pos()
	return w
}

func (w *binaryWriter) WriteTOC() error {
	w.buf.Reset()

	w.buf.PutBE64(w.toc.Symbols)
	w.buf.PutBE64(w.toc.PostingsOffsetTable)

	w.buf.PutHash(w.crc32)

	return w.f.Write(w.buf.Get())
}

func (w *binaryWriter) Write(p []byte) (int, error) {
	n := w.f.Pos()
	err := w.f.Write(p)
	return int(w.f.Pos() - n), err
}

func (w *binaryWriter) Close() error {
	return w.f.Close()
}

type postingOffset struct {
	// label value.
	value string
	// offset of this entry in posting offset table in index-header file.
	tableOff int
}

type BinaryReader struct {
	b   index.ByteSlice
	toc *BinaryTOC

	// Close that releases the underlying resources of the byte slice.
	c io.Closer

	// Map of LabelName to a list of some LabelValues's position in the offset table.
	// The first and last values for each name are always present.
	postings map[string][]postingOffset
	// For the v1 format, labelname -> labelvalue -> offset.
	postingsV1 map[string]map[string]index.Range

	symbols     *index.Symbols
	nameSymbols map[uint32]string // Cache of the label name symbol lookups,
	// as there are not many and they are half of all lookups.

	dec *index.Decoder

	version             int
	indexVersion        int
	indexLastPostingEnd int64
}

// NewBinaryReader loads or builds new index-header if not present on disk.
func NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID) (*BinaryReader, error) {
	binfn := filepath.Join(dir, id.String(), block.IndexHeaderFilename)
	br, err := newFileBinaryReader(binfn)
	if err == nil {
		return br, nil
	}

	level.Warn(logger).Log("msg", "failed to read index-header from disk; recreating", "path", binfn, "err", err)

	if err := WriteBinary(ctx, bkt, id, binfn); err != nil {
		return nil, errors.Wrap(err, "write index header")
	}

	level.Debug(logger).Log("msg", "build index-header file", "path", binfn, "err", err)

	return newFileBinaryReader(binfn)
}

func newFileBinaryReader(path string) (bw *BinaryReader, err error) {
	f, err := fileutil.OpenMmapFile(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			runutil.CloseWithErrCapture(&err, f, "index header close")
		}
	}()

	r := &BinaryReader{
		b:        realByteSlice(f.Bytes()),
		c:        f,
		postings: map[string][]postingOffset{},
	}

	// Verify header.
	if r.b.Len() < headerLen {
		return nil, errors.Wrap(encoding.ErrInvalidSize, "index header's header")
	}
	if m := binary.BigEndian.Uint32(r.b.Range(0, 4)); m != MagicIndex {
		return nil, errors.Errorf("invalid magic number %x", m)
	}
	r.version = int(r.b.Range(4, 5)[0])
	r.indexVersion = int(r.b.Range(5, 6)[0])

	indexPostingOffsetTable := binary.BigEndian.Uint64(r.b.Range(6, headerLen))
	r.indexLastPostingEnd = int64(indexPostingOffsetTable - 1)

	if r.version != BinaryFormatV1 {
		return nil, errors.Errorf("unknown index header file version %d", r.version)
	}

	r.toc, err = newBinaryTOCFromByteSlice(r.b)
	if err != nil {
		return nil, errors.Wrap(err, "read index header TOC")
	}

	r.symbols, err = index.NewSymbols(r.b, r.indexVersion, int(r.toc.Symbols))
	if err != nil {
		return nil, errors.Wrap(err, "read symbols")
	}

	var lastKey []string
	if r.indexVersion == index.FormatV1 {
		// Earlier V1 formats don't have a sorted postings offset table, so
		// load the whole offset table into memory.
		r.postingsV1 = map[string]map[string]index.Range{}

		var prevRng index.Range
		if err := index.ReadOffsetTable(r.b, r.toc.PostingsOffsetTable, func(key []string, off uint64, _ int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}
			// TODO(bwplotka): This is wrong, probably we have to sort.
			if lastKey != nil {
				prevRng.End = int64(off - 1)
				r.postingsV1[lastKey[0]][lastKey[1]] = prevRng
			}

			if _, ok := r.postingsV1[key[0]]; !ok {
				r.postingsV1[key[0]] = map[string]index.Range{}
				r.postings[key[0]] = nil // Used to get a list of labelnames in places.
			}

			lastKey = key
			prevRng = index.Range{Start: int64(off)}
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
		if lastKey != nil {
			prevRng.End = r.indexLastPostingEnd
			r.postingsV1[lastKey[0]][lastKey[1]] = prevRng
		}
	} else {
		lastTableOff := 0
		valueCount := 0

		// For the postings offset table we keep every label name but only every nth
		// label value (plus the first and last one), to save memory.
		if err := index.ReadOffsetTable(r.b, r.toc.PostingsOffsetTable, func(key []string, off uint64, tableOff int) error {
			if len(key) != 2 {
				return errors.Errorf("unexpected key length for posting table %d", len(key))
			}

			if _, ok := r.postings[key[0]]; !ok {
				// Next label name.
				r.postings[key[0]] = []postingOffset{}
				if lastKey != nil {
					// Always include last value for each label name.
					r.postings[lastKey[0]] = append(r.postings[lastKey[0]], postingOffset{value: lastKey[1], tableOff: lastTableOff})
				}
				valueCount = 0
			}

			if valueCount%symbolFactor == 0 {
				r.postings[key[0]] = append(r.postings[key[0]], postingOffset{value: key[1], tableOff: tableOff})
				lastKey = nil
				return nil
			}
			lastKey = key
			lastTableOff = tableOff
			valueCount++
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read postings table")
		}
		if lastKey != nil {
			r.postings[lastKey[0]] = append(r.postings[lastKey[0]], postingOffset{value: lastKey[1], tableOff: lastTableOff})
		}
		// Trim any extra space in the slices.
		for k, v := range r.postings {
			l := make([]postingOffset, len(v))
			copy(l, v)
			r.postings[k] = l
		}
	}

	r.nameSymbols = make(map[uint32]string, len(r.postings))
	for k := range r.postings {
		if k == "" {
			continue
		}
		off, err := r.symbols.ReverseLookup(k)
		if err != nil {
			return nil, errors.Wrap(err, "reverse symbol lookup")
		}
		r.nameSymbols[off] = k
	}

	r.dec = &index.Decoder{LookupSymbol: r.LookupSymbol}

	return r, nil
}

// newBinaryTOCFromByteSlice return parsed TOC from given index header byte slice.
func newBinaryTOCFromByteSlice(bs index.ByteSlice) (*BinaryTOC, error) {
	if bs.Len() < binaryTOCLen {
		return nil, encoding.ErrInvalidSize
	}
	b := bs.Range(bs.Len()-binaryTOCLen, bs.Len())

	expCRC := binary.BigEndian.Uint32(b[len(b)-4:])
	d := encoding.Decbuf{B: b[:len(b)-4]}

	if d.Crc32(castagnoliTable) != expCRC {
		return nil, errors.Wrap(encoding.ErrInvalidChecksum, "read index header TOC")
	}

	if err := d.Err(); err != nil {
		return nil, err
	}

	return &BinaryTOC{
		Symbols:             d.Be64(),
		PostingsOffsetTable: d.Be64(),
	}, nil
}

func (r BinaryReader) IndexVersion() int {
	return r.indexVersion
}

// TODO(bwplotka): Get advantage of multi value offset fetch.
func (r BinaryReader) PostingsOffset(name string, value string) (index.Range, error) {
	rngs, err := r.postingsOffset(name, value)
	if err != nil {
		return index.Range{}, err
	}
	if len(rngs) != 1 {
		return index.Range{}, NotFoundRangeErr
	}
	return rngs[0], nil
}

func (r BinaryReader) postingsOffset(name string, values ...string) ([]index.Range, error) {
	rngs := make([]index.Range, 0, len(values))
	if r.indexVersion == index.FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return nil, nil
		}
		for _, v := range values {
			rng, ok := e[v]
			if !ok {
				continue
			}
			rngs = append(rngs, rng)
		}
		return rngs, nil
	}

	e, ok := r.postings[name]
	if !ok {
		return nil, nil
	}

	if len(values) == 0 {
		return nil, nil
	}

	skip := 0
	valueIndex := 0
	for valueIndex < len(values) && values[valueIndex] < e[0].value {
		// Discard values before the start.
		valueIndex++
	}

	var tmpRngs []index.Range // The start, end offsets in the postings table in the original index file.
	for valueIndex < len(values) {
		value := values[valueIndex]

		i := sort.Search(len(e), func(i int) bool { return e[i].value >= value })
		if i == len(e) {
			// We're past the end.
			break
		}
		if i > 0 && e[i].value != value {
			// Need to look from previous entry.
			i--
		}
		// Don't Crc32 the entire postings offset table, this is very slow
		// so hope any issues were caught at startup.
		d := encoding.NewDecbufAt(r.b, int(r.toc.PostingsOffsetTable), nil)
		d.Skip(e[i].tableOff)

		tmpRngs = tmpRngs[:0]
		// Iterate on the offset table.
		for d.Err() == nil {
			if skip == 0 {
				// These are always the same number of bytes,
				// and it's faster to skip than parse.
				skip = d.Len()
				d.Uvarint()      // Keycount.
				d.UvarintBytes() // Label name.
				skip -= d.Len()
			} else {
				d.Skip(skip)
			}
			v := d.UvarintBytes()                 // Label value.
			postingOffset := int64(d.Uvarint64()) // Offset.
			for string(v) >= value {
				if string(v) == value {
					// Actual posting is 4 bytes after offset, which includes length.
					tmpRngs = append(tmpRngs, index.Range{Start: postingOffset + 4})
				}
				valueIndex++
				if valueIndex == len(values) {
					break
				}
				value = values[valueIndex]
			}
			if i+1 == len(e) {
				for i := range tmpRngs {
					tmpRngs[i].End = r.indexLastPostingEnd
				}
				rngs = append(rngs, tmpRngs...)
				// Need to go to a later postings offset entry, if there is one.
				break
			}

			if value >= e[i+1].value || valueIndex == len(values) {
				d.Skip(skip)
				d.UvarintBytes()                      // Label value.
				postingOffset := int64(d.Uvarint64()) // Offset.
				for j := range tmpRngs {
					// Actual posting end is 4 bytes before next offset.
					tmpRngs[j].End = postingOffset - 4
				}
				rngs = append(rngs, tmpRngs...)
				// Need to go to a later postings offset entry, if there is one.
				break
			}
		}
		if d.Err() != nil {
			return nil, errors.Wrap(d.Err(), "get postings offset entry")
		}
	}

	return rngs, nil
}

func (r BinaryReader) LookupSymbol(o uint32) (string, error) {
	if s, ok := r.nameSymbols[o]; ok {
		return s, nil
	}
	return r.symbols.Lookup(o)
}

func (r BinaryReader) LabelValues(name string) ([]string, error) {
	if r.indexVersion == index.FormatV1 {
		e, ok := r.postingsV1[name]
		if !ok {
			return nil, nil
		}
		values := make([]string, 0, len(e))
		for k := range e {
			values = append(values, k)
		}
		sort.Strings(values)
		return values, nil

	}
	e, ok := r.postings[name]
	if !ok {
		return nil, nil
	}
	if len(e) == 0 {
		return nil, nil
	}
	values := make([]string, 0, len(e)*symbolFactor)

	d := encoding.NewDecbufAt(r.b, int(r.toc.PostingsOffsetTable), nil)
	d.Skip(e[0].tableOff)
	lastVal := e[len(e)-1].value

	skip := 0
	for d.Err() == nil {
		if skip == 0 {
			// These are always the same number of bytes,
			// and it's faster to skip than parse.
			skip = d.Len()
			d.Uvarint()      // Keycount.
			d.UvarintBytes() // Label name.
			skip -= d.Len()
		} else {
			d.Skip(skip)
		}
		s := yoloString(d.UvarintBytes()) //Label value.
		values = append(values, s)
		if s == lastVal {
			break
		}
		d.Uvarint64() // Offset.
	}
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "get postings offset entry")
	}
	return values, nil
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

func (r BinaryReader) LabelNames() []string {
	allPostingsKeyName, _ := index.AllPostingsKey()
	labelNames := make([]string, 0, len(r.postings))
	for name := range r.postings {
		if name == allPostingsKeyName {
			// This is not from any metric.
			continue
		}
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)
	return labelNames
}

func (r *BinaryReader) Close() error { return nil }