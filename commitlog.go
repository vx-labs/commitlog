package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrCorruptedLog = errors.New("corrupted commitlog")
)

type commitLog struct {
	datadir               string
	mtx                   sync.Mutex
	activeSegment         Segment
	segments              []uint64
	segmentMaxRecordCount uint64
	maxSegmentCount       int
}
type CommitLog interface {
	io.Closer
	WriteEntry(ts uint64, value []byte) (uint64, error)
	Delete() error
	Reader() Cursor
	Offset() uint64
	Datadir() string
	LookupTimestamp(ts uint64) uint64
	Latest() uint64
	GetStatistics() Statistics
	TruncateAfter(offset uint64) error
}

func logFiles(datadir string) []uint64 {
	matches, err := filepath.Glob(fmt.Sprintf("%s/*.log", datadir))
	if err != nil {
		return nil
	}
	out := make([]uint64, 0)
	for idx := range matches {
		offsetStr := strings.TrimSuffix(filepath.Base(matches[idx]), ".log")
		offset, err := strconv.ParseUint(offsetStr, 10, 64)
		if err == nil {
			out = append(out, offset)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

type createOpt func(*commitLog)

func WithMaxSegmentCount(i int) createOpt {
	return func(c *commitLog) { c.maxSegmentCount = i }
}

func Open(datadir string, segmentMaxRecordCount uint64, opts ...createOpt) (CommitLog, error) {
	files := logFiles(datadir)
	if len(files) > 0 {
		return open(datadir, segmentMaxRecordCount, opts...)
	}
	err := os.MkdirAll(datadir, 0750)
	if err != nil {
		return nil, err
	}
	return create(datadir, segmentMaxRecordCount, opts...)
}

func newLog(datadir string, segmentMaxRecordCount uint64, opts ...createOpt) *commitLog {
	c := &commitLog{
		datadir:               datadir,
		segmentMaxRecordCount: segmentMaxRecordCount,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func create(datadir string, segmentMaxRecordCount uint64, opts ...createOpt) (CommitLog, error) {
	l := newLog(datadir, segmentMaxRecordCount, opts...)
	return l, l.appendSegment()
}

func open(datadir string, segmentMaxRecordCount uint64, opts ...createOpt) (CommitLog, error) {
	l := newLog(datadir, segmentMaxRecordCount, opts...)
	files := logFiles(datadir)
	var offset uint64 = files[0]
	for {
		segment, err := openSegment(datadir, offset, segmentMaxRecordCount, true)
		if err != nil {
			if err == ErrSegmentDoesNotExist {
				break
			}
			return nil, ErrCorruptedLog
		}
		l.segments = append(l.segments, offset)
		if l.activeSegment != nil {
			if l.activeSegment.BaseOffset() < segment.BaseOffset() {
				l.activeSegment.Close()
				l.activeSegment = segment
			} else {
				segment.Close()
			}
		} else {
			l.activeSegment = segment
		}
		offset += uint64(segmentMaxRecordCount)
	}
	l.trimSegments()
	return l, nil
}

func (e *commitLog) Offset() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return e.activeSegment.CurrentOffset() + e.activeSegment.BaseOffset()
}
func (e *commitLog) Latest() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return e.activeSegment.Latest()
}
func (e *commitLog) Close() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	if e.activeSegment != nil {
		return e.activeSegment.Close()
	}
	return nil
}
func (e *commitLog) Datadir() string {
	return e.datadir
}
func (e *commitLog) Delete() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.activeSegment != nil {
		e.activeSegment.Close()
	}
	for _, idx := range e.segments {
		segment, err := openSegment(e.datadir, idx, e.segmentMaxRecordCount, false)
		if err == nil {
			err = segment.Delete()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (e *commitLog) appendSegment() error {
	var nextOffset uint64
	if len(e.segments) > 0 {
		nextOffset = e.segments[len(e.segments)-1] + e.segmentMaxRecordCount
	}

	segment, err := createSegment(e.datadir, nextOffset, e.segmentMaxRecordCount)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create new segment with offset %d", nextOffset))
	}
	e.segments = append(e.segments, nextOffset)
	if e.activeSegment != nil {
		err = e.activeSegment.Close()
		if err != nil {
			return err
		}
	}
	e.activeSegment = segment
	return nil
}

// trimSegments will delete segments acording to e.maxSegmentCount
func (e *commitLog) trimSegments() {
	if e.maxSegmentCount <= 0 || len(e.segments) < e.maxSegmentCount {
		return
	}
	count := len(e.segments)
	for _, segment := range e.segments[0 : count-e.maxSegmentCount] {
		err := deleteSegment(e.datadir, segment)
		if err != nil {
			// TODO: provide feedback on error
		}
	}
	e.segments = e.segments[count-e.maxSegmentCount:]
}

// lookupOffset returns the segment index of the segment containing the provided offset
func (e *commitLog) lookupOffset(offset uint64) int {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.lookupOffsetUnlocked(offset)
}

func (e *commitLog) lookupOffsetUnlocked(offset uint64) int {
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		return e.segments[i] > offset
	})
	if idx == 0 {
		return 0
	}
	return idx - 1
}
func (e *commitLog) LookupTimestamp(ts uint64) uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		seg, err := e.readSegment(e.segments[i])
		if err != nil {
			return true
		}
		defer seg.Close()
		return seg.Earliest() > ts
	})
	if idx <= 0 {
		return 0
	}
	seg, err := e.readSegment(e.segments[idx-1])
	if err != nil {
		return 0
	}
	defer seg.Close()
	return seg.LookupTimestamp(ts)
}

func (e *commitLog) readSegment(id uint64) (Segment, error) {
	s, err := openSegment(e.datadir, id, e.segmentMaxRecordCount, false)
	if err != nil {
		return nil, err
	}
	_, err = s.Seek(int64(id), io.SeekStart)
	return s, err
}

func (e *commitLog) Reader() Cursor {
	return &cursor{
		log: e,
	}
}

// Truncate the log *after* the given offset. You must ensure no one is reading the log before truncating it.
func (e *commitLog) TruncateAfter(offset uint64) error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	segmentIdx := e.lookupOffsetUnlocked(offset)

	var segment Segment
	var err error
	if segmentIdx == len(e.segments)-1 {
		segment = e.activeSegment
	} else {
		e.activeSegment.Close()
		segment, err = openSegment(e.datadir, e.segments[segmentIdx], e.segmentMaxRecordCount, true)
		if err != nil {
			panic(err)
		}
	}
	err = segment.TruncateAfter(offset)
	if err != nil {
		panic(err)
	}
	e.activeSegment = segment
	for i := segmentIdx + 1; i < len(e.segments); i++ {
		segment, err := openSegment(e.datadir, e.segments[i], e.segmentMaxRecordCount, true)
		if err != nil {
			panic(err)
		}
		err = segment.Delete()
		if err != nil {
			panic(err)
		}
	}
	e.segments = e.segments[:segmentIdx+1]
	return nil
}
func (e *commitLog) WriteEntry(ts uint64, value []byte) (uint64, error) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if segmentEntryCount := e.activeSegment.CurrentOffset(); segmentEntryCount >= e.segmentMaxRecordCount {
		err := e.appendSegment()
		if err != nil {
			return 0, errors.Wrap(err, "failed to extend log")
		}
		e.trimSegments()
	}
	n, err := e.activeSegment.WriteEntry(ts, value)
	return n + e.activeSegment.BaseOffset(), err
}
