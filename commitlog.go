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
	segments              []Segment
	segmentMaxRecordCount uint64
	currentOffset         uint64
	maxSegmentCount       int
}
type CommitLog interface {
	io.Closer
	// WriteEntry appends a binary payload to the commitlog.
	WriteEntry(ts uint64, value []byte) (uint64, error)
	// Delete closes then deletes the commitlog from disk.
	Delete() error
	// Reader returns a seekable reader
	Reader() Cursor
	// Offset returns the offset of the next record the be written.
	Offset() uint64
	Datadir() string
	//LookupTimestamp returns the seekable offset of the first record writen after the provided timestamp.
	LookupTimestamp(ts uint64) uint64
	//Latest returns the timestamp of the most recent record
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
		l.segments = append(l.segments, segment)
		if l.activeSegment != nil {
			if l.activeSegment.BaseOffset() < segment.BaseOffset() {
				l.activeSegment = segment
			}
		} else {
			l.activeSegment = segment
		}
		offset += uint64(segmentMaxRecordCount)
	}
	l.trimSegments()
	l.currentOffset = l.activeSegment.CurrentOffset() + l.activeSegment.BaseOffset()
	return l, nil
}

func (e *commitLog) Offset() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.currentOffset
}
func (e *commitLog) Latest() uint64 {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return e.activeSegment.Latest()
}
func (e *commitLog) Close() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	for _, segment := range e.segments {
		segment.Close()
	}
	return nil
}
func (e *commitLog) Datadir() string {
	return e.datadir
}
func (e *commitLog) Delete() error {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	for _, segment := range e.segments {
		err := segment.Delete()
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *commitLog) appendSegment() error {
	var nextOffset uint64
	if len(e.segments) > 0 {
		nextOffset = e.activeSegment.CurrentOffset() + e.activeSegment.BaseOffset()
	}
	segment, err := createSegment(e.datadir, nextOffset, e.segmentMaxRecordCount)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create new segment with offset %d", nextOffset))
	}
	e.segments = append(e.segments, segment)
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
		segment.Delete()
	}
	e.segments = e.segments[count-e.maxSegmentCount:]
}

// LookupOffsetSegment returns the segment containing the provided offset
func (e *commitLog) lookupOffset(offset uint64) Segment {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	idx := e.lookupOffsetSegment(offset)
	return e.segments[idx]
}

// LookupOffsetSegment returns the segment index of the segment containing the provided offset
func (e *commitLog) LookupOffsetSegment(offset uint64) int {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.lookupOffsetSegment(offset)
}

func (e *commitLog) lookupOffsetSegment(offset uint64) int {
	count := len(e.segments)
	idx := sort.Search(count, func(i int) bool {
		return e.segments[i].BaseOffset() > offset
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
		seg := e.segments[i]
		return seg.Earliest() > ts
	})
	if idx <= 0 {
		return e.segments[0].BaseOffset()
	}
	seg := e.segments[idx-1]
	return seg.LookupTimestamp(ts)
}

func (e *commitLog) Reader() Cursor {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return &cursor{
		log:            e,
		currentSegment: e.segments[0],
		pos:            0,
	}
}

// Truncate the log *after* the given offset. You must ensure no one is reading the log before truncating it.
func (e *commitLog) TruncateAfter(offset uint64) error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	segmentIdx := e.lookupOffsetSegment(offset)

	var segment Segment
	var err error
	if segmentIdx == len(e.segments)-1 {
		segment = e.activeSegment
	} else {
		segment = e.segments[segmentIdx]
	}
	err = segment.TruncateAfter(offset)
	if err != nil {
		panic(err)
	}
	e.activeSegment = segment
	for i := segmentIdx + 1; i < len(e.segments); i++ {
		segment = e.segments[i]
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
	_, err := e.activeSegment.WriteEntry(ts, value)
	e.currentOffset++
	return e.currentOffset - 1, err
}
