package commitlog

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	datadir := "/tmp/commitlog_test/"
	os.MkdirAll(datadir, 0750)
	defer os.RemoveAll(datadir)
	s, err := createSegment(datadir, 0, 200)
	require.NoError(t, err)
	defer s.Delete()
	value := []byte("test")

	t.Run("should not allow creating an existing segment", func(t *testing.T) {
		_, err := createSegment(datadir, 0, 200)
		require.Error(t, err)
	})
	t.Run("should write provided value", func(t *testing.T) {
		n, err := s.WriteEntry(2, value)
		require.NoError(t, err)
		require.Equal(t, uint64(0), n)
	})
	t.Run("should close then reopen without error", func(t *testing.T) {
		value := []byte("test")
		err := s.Close()
		require.NoError(t, err)
		s, err = openSegment(datadir, 0, 200, true)
		require.NoError(t, err)
		require.Equal(t, uint64(1), s.CurrentOffset())
		require.Equal(t, uint64(EntryHeaderSize+len(value)), s.Size())
	})
	t.Run("should allow seeking timestamp", func(t *testing.T) {
		require.Equal(t, uint64(0), s.(*segment).LookupTimestamp(0))
		require.Equal(t, uint64(1), s.(*segment).LookupTimestamp(10))
	})
	t.Run("should allow looking up position", func(t *testing.T) {
		pos, err := s.(*segment).LookupPosition(1)
		require.NoError(t, err)
		require.Equal(t, int64(32), pos)
		pos, err = s.(*segment).LookupPosition(0)
		require.NoError(t, err)
		require.Equal(t, int64(0), pos)
	})
	t.Run("should allow truncate after a given offset", func(t *testing.T) {
		n, err := s.WriteEntry(3, value)
		require.NoError(t, err)
		require.Equal(t, uint64(1), n)
		n, err = s.WriteEntry(4, value)
		require.NoError(t, err)
		require.Equal(t, uint64(2), n)
		n, err = s.WriteEntry(5, value)
		require.NoError(t, err)
		require.Equal(t, uint64(3), n)

		require.Equal(t, uint64(4), s.CurrentOffset())
		require.NoError(t, s.(*segment).TruncateAfter(2))
		require.Equal(t, uint64(3), s.CurrentOffset())
		n, err = s.WriteEntry(5, value)
		require.NoError(t, err)
		require.Equal(t, uint64(3), n)
	})
}

func BenchmarkSegment(b *testing.B) {
	datadir := "/tmp/commitlog_test/"
	os.MkdirAll(datadir, 0750)
	s, err := createSegment(datadir, 0, 20000000)
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
	b.Run("open", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := s.Close()
			require.NoError(b, err)
			s, err = openSegment(datadir, 0, 20000000, true)
			require.NoError(b, err)
		}
	})
}
