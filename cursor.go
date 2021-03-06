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
	lastOffset := int64(atomic.LoadUint64(&c.log.currentOffset))

	switch whence {
	case io.SeekEnd:
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
	if target > uint64(lastOffset) {
		target = uint64(lastOffset)
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
			if total > 0 {
				return total, nil
			}
			currentLogOffset := c.currentSegment.BaseOffset() + c.currentSegment.CurrentOffset()
			nextSegment := c.log.lookupOffset(currentLogOffset)
			if nextSegment.BaseOffset() != c.currentSegment.BaseOffset() {
				c.currentSegment = nextSegment
				c.pos = 0
				if len(p) > total {
					continue
				}
			}
		}
		return total, err
	}
}
