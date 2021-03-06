package stream

import (
	"context"
	"io"
	"time"

	"github.com/vx-labs/commitlog"
)

type eofBehaviour int

const (
	// EOFBehaviourPoll will make the session poll for new records after an EOF error is received
	EOFBehaviourPoll eofBehaviour = 1 << iota
	// EOFBehaviourExit wil make the session exit when EOF is received
	EOFBehaviourExit eofBehaviour = 1 << iota
)

// ConsumerOpts describes stream session preferences
type ConsumerOpts struct {
	Name                      string
	MaxBatchSize              int
	MinBatchSize              int
	MaxBatchMemorySizeInBytes int
	MaxRecordCount            int64
	FromOffset                int64
	EOFBehaviour              eofBehaviour
	OffsetProvider            OffsetIterator
	Middleware                []func(Processor, ConsumerOpts) Processor
}

type Batch struct {
	Records []commitlog.Entry
}
type poller struct {
	maxBatchSize              int
	minBatchSize              int
	maxBatchMemorySizeInBytes int
	currentMemorySizeInBytes  int
	recordCountdown           int64
	current                   Batch
	ch                        chan Batch
	err                       error
}

type Poller interface {
	Ready() <-chan Batch
	Error() error
}

func newPoller(ctx context.Context, r io.ReadSeeker, opts ConsumerOpts) Poller {

	r.Seek(opts.FromOffset, io.SeekStart)
	if opts.OffsetProvider != nil {
		opts.OffsetProvider.AdvanceTo(uint64(opts.FromOffset))
	}
	s := &poller{
		ch:                        make(chan Batch),
		maxBatchSize:              opts.MaxBatchSize,
		maxBatchMemorySizeInBytes: opts.MaxBatchMemorySizeInBytes,
		recordCountdown:           opts.MaxRecordCount,
		minBatchSize:              opts.MinBatchSize,
		current: Batch{
			Records: []commitlog.Entry{},
		},
	}
	go s.run(ctx, r, opts)
	return s
}

func (s *poller) Error() error {
	return s.err
}
func (s *poller) Ready() <-chan Batch {
	return s.ch
}
func (s *poller) waitFlush(ctx context.Context) error {
	if len(s.current.Records) == 0 {
		return nil
	}
	select {
	case s.ch <- s.current:
		s.currentMemorySizeInBytes = 0
		s.current = Batch{
			Records: []commitlog.Entry{},
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
func (s *poller) tryFlush(ctx context.Context) error {
	if len(s.current.Records) == 0 {
		return nil
	}
	select {
	case s.ch <- s.current:
		s.currentMemorySizeInBytes = 0
		s.current = Batch{
			Records: []commitlog.Entry{},
		}
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}
func (s *poller) run(ctx context.Context, r io.ReadSeeker, opts ConsumerOpts) {
	decoder := commitlog.NewDecoder(r)
	defer close(s.ch)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if opts.OffsetProvider != nil {
			next, err := opts.OffsetProvider.Next()
			if err != nil {
				if err == io.EOF {
					if err := s.waitFlush(ctx); err != nil {
						s.err = err
						return
					}
					if opts.EOFBehaviour == EOFBehaviourExit {
						return
					}
					select {
					case <-ticker.C:
						continue
					case <-ctx.Done():
						return
					}
				} else {
					s.err = err
					return
				}
			}
			_, err = r.Seek(int64(next), io.SeekStart)
			if err != nil {
				s.err = err
				return
			}
		}
		if s.recordCountdown == 0 {
			if err := s.waitFlush(ctx); err != nil {
				s.err = err
			}
			return
		}
		entry, err := decoder.Decode()
		if err == nil {
			if s.currentMemorySizeInBytes+len(entry.Payload()) > s.maxBatchMemorySizeInBytes {
				if err := s.waitFlush(ctx); err != nil {
					s.err = err
					return
				}
			}
			s.current.Records = append(s.current.Records, entry)
			s.currentMemorySizeInBytes += len(entry.Payload())
			if s.recordCountdown > 0 {
				s.recordCountdown--
			}
		}
		if len(s.current.Records) >= s.maxBatchSize {
			if err := s.waitFlush(ctx); err != nil {
				s.err = err
				return
			}
			continue
		} else if len(s.current.Records) > s.minBatchSize || entry == nil {
			if err := s.tryFlush(ctx); err != nil {
				s.err = err
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				if err := s.waitFlush(ctx); err != nil {
					s.err = err
					return
				}
				if opts.EOFBehaviour == EOFBehaviourExit {
					return
				}
				select {
				case <-ticker.C:
					continue
				case <-ctx.Done():
					return
				}
			} else {
				s.err = err
				return
			}
		}
	}
}
