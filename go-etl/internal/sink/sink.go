package sink

import (
	"context"
	"sync/atomic"
)

type BatchSink interface {
	WriteWorksBatch(ctx context.Context, b *WorksBatch) error
	WriteTopicsBatch(ctx context.Context, b *TopicsBatch) error
	WriteInstitutionsBatch(ctx context.Context, b *InstitutionsBatch) error
	Close() error
}

type NoopStats struct {
	WorksBatches        uint64
	WorksRows           uint64
	WorksBytes          uint64
	TopicsBatches       uint64
	TopicsRows          uint64
	TopicsBytes         uint64
	InstitutionsBatches uint64
	InstitutionsRows    uint64
	InstitutionsBytes   uint64
}

type NoopSink struct {
	worksBatches        atomic.Uint64
	worksRows           atomic.Uint64
	worksBytes          atomic.Uint64
	topicsBatches       atomic.Uint64
	topicsRows          atomic.Uint64
	topicsBytes         atomic.Uint64
	institutionsBatches atomic.Uint64
	institutionsRows    atomic.Uint64
	institutionsBytes   atomic.Uint64
	closed              atomic.Bool
}

var _ BatchSink = (*NoopSink)(nil)

func NewNoopSink() *NoopSink {
	return &NoopSink{}
}

func (s *NoopSink) WriteWorksBatch(ctx context.Context, b *WorksBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || b.Len() == 0 {
		return nil
	}
	s.worksBatches.Add(1)
	s.worksRows.Add(uint64(b.Len()))
	s.worksBytes.Add(uint64(b.ApproxBytes()))
	return nil
}

func (s *NoopSink) WriteTopicsBatch(ctx context.Context, b *TopicsBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || b.Len() == 0 {
		return nil
	}
	s.topicsBatches.Add(1)
	s.topicsRows.Add(uint64(b.Len()))
	s.topicsBytes.Add(uint64(b.ApproxBytes()))
	return nil
}

func (s *NoopSink) WriteInstitutionsBatch(ctx context.Context, b *InstitutionsBatch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || b.Len() == 0 {
		return nil
	}
	s.institutionsBatches.Add(1)
	s.institutionsRows.Add(uint64(b.Len()))
	s.institutionsBytes.Add(uint64(b.ApproxBytes()))
	return nil
}

func (s *NoopSink) Close() error {
	s.closed.Store(true)
	return nil
}

func (s *NoopSink) Closed() bool {
	return s.closed.Load()
}

func (s *NoopSink) Stats() NoopStats {
	return NoopStats{
		WorksBatches:        s.worksBatches.Load(),
		WorksRows:           s.worksRows.Load(),
		WorksBytes:          s.worksBytes.Load(),
		TopicsBatches:       s.topicsBatches.Load(),
		TopicsRows:          s.topicsRows.Load(),
		TopicsBytes:         s.topicsBytes.Load(),
		InstitutionsBatches: s.institutionsBatches.Load(),
		InstitutionsRows:    s.institutionsRows.Load(),
		InstitutionsBytes:   s.institutionsBytes.Load(),
	}
}
