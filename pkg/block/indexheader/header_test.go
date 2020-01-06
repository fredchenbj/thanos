package indexheader

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore/inmem"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestReaders(t *testing.T) {
	ctx := context.Background()
	bkt := inmem.NewBucket()

	tmpDir, err := ioutil.TempDir("", "test-indexheader")
	testutil.Ok(t, err)
	defer func() { testutil.Ok(t, os.RemoveAll(tmpDir)) }()

	// Create block index version 2.
	id1, err := testutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124)
	testutil.Ok(t, err)

	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, id1.String())))

	// Copy block index version 1 for backward compatibility.
	/* The block here was produced at the commit
	    706602daed1487f7849990678b4ece4599745905 used in 2.0.0 with:
	   db, _ := Open("v1db", nil, nil, nil)
	   app := db.Appender()
	   app.Add(labels.FromStrings("foo", "bar"), 1, 2)
	   app.Add(labels.FromStrings("foo", "baz"), 3, 4)
	   app.Add(labels.FromStrings("foo", "meh"), 1000*3600*4, 4) // Not in the block.
	   // Make sure we've enough values for the lack of sorting of postings offsets to show up.
	   for i := 0; i < 100; i++ {
	     app.Add(labels.FromStrings("bar", strconv.FormatInt(int64(i), 10)), 0, 0)
	   }
	   app.Commit()
	   db.compact()
	   db.Close()
	*/

	m, err := metadata.Read("./testdata/index_format_v1")
	testutil.Ok(t, err)
	testutil.Copy(t, "./testdata/index_format_v1", filepath.Join(tmpDir, m.ULID.String()))

	_, err = metadata.InjectThanos(log.NewNopLogger(), filepath.Join(tmpDir, m.ULID.String()), metadata.Thanos{
		Labels:     labels.Labels{{Name: "ext1", Value: "1"}}.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: 0},
		Source:     metadata.TestSource,
	}, &m.BlockMeta)
	testutil.Ok(t, err)
	testutil.Ok(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, m.ULID.String())))

	for _, id := range []ulid.ULID{id1, m.ULID} {
		t.Run(id.String(), func(t *testing.T) {
			ir, err := index.NewFileReader(filepath.Join(tmpDir, id.String(), block.IndexFilename))
			testutil.Ok(t, err)
			defer func() { _ = ir.Close() }()

			t.Run("binary", func(t *testing.T) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexHeaderFilename)
				testutil.Ok(t, WriteBinary(ctx, bkt, id, fn))

				br, err := NewBinaryReader(ctx, log.NewNopLogger(), nil, tmpDir, id)
				testutil.Ok(t, err)

				defer func() { testutil.Ok(t, br.Close()) }()

				if id == id1 {
					testutil.Equals(t, 1, br.version)
					testutil.Equals(t, 2, br.indexVersion)
					testutil.Equals(t, &BinaryTOC{Symbols: headerLen, PostingsOffsetTable: 38}, br.toc)
					testutil.Equals(t, int64(300), br.indexLastPostingEnd)
					testutil.Equals(t, 8, br.symbols.Size())
					testutil.Equals(t, 3, len(br.postings))
					testutil.Equals(t, 0, len(br.postingsV1))
					testutil.Equals(t, 2, len(br.nameSymbols))
				}

				compareIndexToHeader(t, ir, br)
			})

			t.Run("json", func(t *testing.T) {
				fn := filepath.Join(tmpDir, id.String(), block.IndexCacheFilename)
				testutil.Ok(t, WriteJSON(log.NewNopLogger(), filepath.Join(tmpDir, id.String(), "index"), fn))

				jr, err := NewJSONReader(ctx, log.NewNopLogger(), nil, tmpDir, id)
				testutil.Ok(t, err)

				defer func() { testutil.Ok(t, jr.Close()) }()

				if id == id1 {
					testutil.Equals(t, 6, len(jr.symbols))
					testutil.Equals(t, 2, len(jr.lvals))
					testutil.Equals(t, 6, len(jr.postings))
				}

				compareIndexToHeader(t, ir, jr)
			})
		})
	}

}

func compareIndexToHeader(t *testing.T, indexReader *index.Reader, headerReader Reader) {
	testutil.Equals(t, indexReader.Version(), headerReader.IndexVersion())

	iter := indexReader.Symbols()
	i := 0
	for ; iter.Next(); i++ {
		r, err := headerReader.LookupSymbol(uint32(i))
		testutil.Ok(t, err)
		testutil.Equals(t, iter.At(), r)
	}
	testutil.Ok(t, iter.Err())
	_, err := headerReader.LookupSymbol(uint32(i))
	testutil.NotOk(t, err)

	expLabelNames, err := indexReader.LabelNames()
	testutil.Ok(t, err)
	testutil.Equals(t, expLabelNames, headerReader.LabelNames())

	expRanges, err := indexReader.PostingsRanges()
	testutil.Ok(t, err)

	for _, lname := range expLabelNames {
		expectedLabelVals, err := indexReader.LabelValues(lname)
		testutil.Ok(t, err)

		vals, err := headerReader.LabelValues(lname)
		testutil.Ok(t, err)
		testutil.Equals(t, expectedLabelVals, vals)

		for i, v := range vals {
			fmt.Println(lname, v)
			ptr, err := headerReader.PostingsOffset(lname, v)
			testutil.Ok(t, err)
			// For index-cache those values are exact. For binary they are exact except last item posting offset.
			if i == len(vals)-1 {
				testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}].Start, ptr.Start)
				testutil.Assert(t, expRanges[labels.Label{Name: lname, Value: v}].End <= ptr.End, "got offset %v earlier than actual posting end %v ", ptr.End, expRanges[labels.Label{Name: lname, Value: v}].End)
				continue
			}
			testutil.Equals(t, expRanges[labels.Label{Name: lname, Value: v}], ptr)
		}
	}

	vals, err := indexReader.LabelValues("not-existing")
	testutil.Ok(t, err)
	testutil.Equals(t, []string(nil), vals)

	vals, err = headerReader.LabelValues("not-existing")
	testutil.Ok(t, err)
	testutil.Equals(t, []string(nil), vals)

	_, err = headerReader.PostingsOffset("not-existing", "1")
	testutil.NotOk(t, err)
}
