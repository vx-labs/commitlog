package commitlog

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitLog(t *testing.T) {
	datadir := "/tmp/commitlog_test/"
	os.MkdirAll(datadir, 0750)
	defer os.RemoveAll(datadir)

	clog, err := create(datadir, 10)
	require.NoError(t, err)
	defer clog.Delete()
	value := []byte("test")
	t.Run("should allow reading from empty log", func(t *testing.T) {
		r := clog.Reader()
		buf := make([]byte, len(value))
		n, err := r.Read(buf)
		require.Equal(t, io.EOF, err)
		require.Equal(t, 0, n)
	})

	for i := 0; i < 50; i++ {
		n, err := clog.WriteEntry(uint64(i), value)
		require.NoError(t, err)
		require.Equal(t, uint64(i), n)
	}
	l := clog.(*commitLog)
	require.Equal(t, 5, len(l.segments))
	t.Run("should close then reopen without error", func(t *testing.T) {
		require.NoError(t, clog.Close())
		clog, err = Open(datadir, 10)
		require.NoError(t, err)
	})
	t.Run("should allow looking up for offset", func(t *testing.T) {
		l := clog.(*commitLog)
		require.Equal(t, 2, l.LookupOffsetSegment(27))
		require.Equal(t, 0, l.LookupOffsetSegment(9))
		require.Equal(t, 1, l.LookupOffsetSegment(10))
	})
	t.Run("should allow reading from log", func(t *testing.T) {
		r := clog.Reader()
		buf := make([]byte, len(value)+EntryHeaderSize)
		for i := 0; i < 50; i++ {
			n, err := r.Read(buf)
			require.NoError(t, err, fmt.Sprintf("index: %d", i))
			require.Equal(t, len(value)+EntryHeaderSize, n, buf)
		}
	})
	t.Run("should allow seeking timestamp in reader", func(t *testing.T) {
		require.Equal(t, uint64(5), clog.(*commitLog).LookupTimestamp(5))
		require.Equal(t, uint64(26), clog.(*commitLog).LookupTimestamp(26))
	})
	t.Run("should decoder to be plugged in", func(t *testing.T) {
		r := clog.Reader()
		r.Seek(1, io.SeekStart)
		dec := NewDecoder(r)
		entry, err := dec.Decode()
		require.NoError(t, err)
		require.Equal(t, []byte("test"), entry.Payload())
	})
	t.Run("should allow log truncation", func(t *testing.T) {
		log.Print(clog.GetStatistics().SegmentCount)
		require.NoError(t, clog.TruncateBefore(40))
		require.Equal(t, uint64(1), clog.GetStatistics().SegmentCount)
	})
}

func BenchmarkLog(b *testing.B) {
	datadir := "/tmp/commitlog_test/"
	os.MkdirAll(datadir, 0750)
	defer os.RemoveAll(datadir)

	s, err := create(datadir, 500)
	require.NoError(b, err)
	defer s.Delete()
	value := []byte("test")
	b.Run("write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err = s.WriteEntry(0, value)
			if err != nil {
				b.Fatalf("segment write failed: %v", err)
			}
		}
	})
}
