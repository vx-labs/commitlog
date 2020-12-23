package commitlog

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

type Cursor interface {
	io.ReadSeeker
}

type cursor struct {
	mtx            sync.Mutex
	currentSegment Segment
	pos            int64
	log            *commitLog
}

func (c *cursor) Seek(offset int64, whence int) (int64, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.seek(offset, whence)
}

func (c *cursor) seek(offset int64, whence int) (int64, error) {
	var err error
	var target uint64
	switch whence {
	case io.SeekEnd:
		lastOffset := int64(atomic.LoadUint64(&c.log.currentOffset))
		if offset > lastOffset {
			target = uint64(lastOffset)
		} else {
			target = uint64(lastOffset + offset)
		}
	case io.SeekStart:
		if offset < 0 {
			return 0, errors.New("invalid offset")
		}
		target = uint64(offset)
	case io.SeekCurrent:
		return 0, errors.New("SeekCurrent unsupported when reading the log")
	default:
		return 0, errors.New("invalid whence")
	}
	c.currentSegment = c.log.lookupOffset(target)
	c.pos, err = c.currentSegment.LookupPosition(target)
	return int64(target), err

}

func (c *cursor) Read(p []byte) (int, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	var total int

	for {
		n, err := c.currentSegment.ReadAt(p[total:], c.pos)
		c.pos += int64(n)
		total += n
		if err == io.EOF {
			currentLogOffset := c.currentSegment.BaseOffset() + c.currentSegment.CurrentOffset()
			if atomic.LoadUint64(&c.log.currentOffset) == currentLogOffset {
				return total, err
			}
			c.currentSegment = c.log.lookupOffset(currentLogOffset)
			c.pos = 0
			if len(p) > total {
				continue
			}
		}
		return total, err
	}
}
