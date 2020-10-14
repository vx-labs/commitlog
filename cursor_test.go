package commitlog

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursor(t *testing.T) {
	datadir := "/tmp/commitlog_test/"
	os.MkdirAll(datadir, 0750)
	defer os.RemoveAll(datadir)

	clog, err := create(datadir, 10)
	require.NoError(t, err)
	defer clog.Delete()
	value := []byte("test")

	for i := 0; i < 50; i++ {
		n, err := clog.WriteEntry(uint64(i), value)
		require.NoError(t, err)
		require.Equal(t, uint64(i), n)
	}
	l := clog.(*commitLog)
	require.Equal(t, 5, len(l.segments))

	t.Run("should allow being written to an io.Writer", func(t *testing.T) {
		buf := bytes.NewBuffer(nil)
		c := clog.Reader()
		c.Seek(0, io.SeekStart)
		n, err := io.Copy(buf, c)
		require.NoError(t, err)
		require.Equal(t, int64(1600), n)
	})
	t.Run("should allow reading the log", func(t *testing.T) {
		buf := make([]byte, 1600)
		r := clog.Reader()
		n, err := io.ReadFull(r, buf)
		require.NoError(t, err)
		require.Equal(t, 1600, n)
	})
	t.Run("should allow seeking position in cursor", func(t *testing.T) {
		cReader := clog.Reader()
		r := cReader.(*cursor)
		t.Run("start", func(t *testing.T) {
			offset, _ := r.Seek(1, io.SeekStart)
			require.Equal(t, int64(1), offset)
			offset, _ = r.Seek(2, io.SeekStart)
			require.Equal(t, int64(2), offset)
			offset, _ = r.Seek(30, io.SeekStart)
			require.Equal(t, int64(30), offset)
			offset, _ = r.Seek(0, io.SeekStart)
			require.Equal(t, int64(0), offset)
		})
	})

}

func TestCursorDeletion(t *testing.T) {
	datadir := "/tmp/commitlog_test/"
	os.MkdirAll(datadir, 0750)
	defer os.RemoveAll(datadir)

	clog, err := create(datadir, 10, WithMaxSegmentCount(4))
	require.NoError(t, err)
	defer clog.Delete()
	value := []byte("test")
	r := clog.Reader()
	for i := 0; i < 50; i++ {
		n, err := clog.WriteEntry(uint64(i), value)
		require.NoError(t, err)
		require.Equal(t, uint64(i), n)
	}
	l := clog.(*commitLog)
	require.Equal(t, 4, len(l.segments))
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	require.Equal(t, err.Error(), "read /tmp/commitlog_test/0.log: file already closed")
	require.Equal(t, 0, n)
	r.Seek(0, io.SeekStart)
	n, err = r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, n)
}
