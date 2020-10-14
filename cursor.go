package commitlog

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

type Cursor interface {
	io.Seeker
	io.Reader
	io.Closer
}

type cursor struct {
	currentIdx     int
	mtx            sync.Mutex
	currentSegment Segment
	pos            int64
	log            *commitLog
}

func (c *cursor) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.currentSegment != nil {
		c.currentSegment = nil
		c.pos = 0
		return nil
	}
	return nil
}
func (c *cursor) Seek(offset int64, whence int) (int64, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.seek(offset, whence)
}
func (c *cursor) seekFromStart(offset int64) (int64, error) {
	idx := c.log.lookupOffset(uint64(offset))
	if idx != c.currentIdx || c.currentSegment == nil {
		if c.currentSegment != nil {
			c.currentSegment = nil
		}
		c.currentIdx = idx
		c.currentSegment = c.log.segments[idx]
	}
	pos, err := c.currentSegment.LookupPosition(uint64(offset))
	if err != nil {
		return 0, err
	}
	c.pos = pos
	return offset, nil
}

func (c *cursor) seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		return c.seekFromStart(offset)
	case io.SeekCurrent:
		return 0, errors.New("SeekCurrent unsupported when reading the log")
	default:
		return 0, errors.New("invalid whence")
	}
}

func (c *cursor) doesSegmentExists(idx int) bool {
	return idx < len(c.log.segments)
}
func (c *cursor) openCurrentSegment() error {
	if c.doesSegmentExists(c.currentIdx) {
		c.currentSegment = c.log.segments[c.currentIdx]
		return nil
	}
	return io.EOF
}
func (c *cursor) Read(p []byte) (int, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	var total int

	for {
		if c.currentSegment == nil {
			if err := c.openCurrentSegment(); err != nil {
				return total, err
			}
		}
		n, err := c.currentSegment.ReadAt(p[total:], c.pos)
		c.pos += int64(n)
		total += n
		if err == io.EOF {
			if c.doesSegmentExists(c.currentIdx + 1) {
				c.currentIdx++
				c.pos = 0
				c.currentSegment = nil
				continue
			}
		}
		return total, err
	}
}
