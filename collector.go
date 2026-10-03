package trace

import (
	"context"
	"crypto/rand"
	"time"
)

// spanContextKey 用于在 context 中携带链路上下文。
type spanContextKey struct{}

// SpanContext 是在 context 中传播的链路上下文。
type SpanContext struct {
	TraceID  TraceID
	SpanID   SpanID
	Sampled  bool
	hasLocal bool
}

// SpanFromContext 从 context 中提取链路上下文，不存在时返回零值。
func SpanFromContext(ctx context.Context) SpanContext {
	if ctx == nil {
		return SpanContext{}
	}
	sc, _ := ctx.Value(spanContextKey{}).(SpanContext)
	return sc
}

// SpanHandle 是采集到的环节句柄，End 用于上报。
type SpanHandle struct {
	traceID   TraceID
	spanID    SpanID
	parentID  SpanID
	name      string
	start     time.Time
	sampled   bool
	status    Status
	tags      map[string]string
	collector *Collector
	ended     bool
}

// End 上报环节耗时。重复调用安全。
func (s *SpanHandle) End() {
	if s.ended {
		return
	}
	s.ended = true
	span := &Span{
		TraceID:  s.traceID,
		SpanID:   s.spanID,
		ParentID: s.parentID,
		Name:     s.name,
		Start:    s.start,
		End:      time.Now(),
		Status:   s.status,
		Tags:     s.tags,
	}
	s.collector.report(span, s.sampled)
	if s.parentID.IsZero() {
		// 根环节结束即封存整条链路。
		s.collector.seal(s.traceID, s.sampled)
	}
}

// SetStatus 设置环节状态。需在 End 前调用。
func (s *SpanHandle) SetStatus(st Status) {
	s.status = st
}

// SetTag 设置环节标签。需在 End 前调用。
func (s *SpanHandle) SetTag(k, v string) {
	if s.tags == nil {
		s.tags = make(map[string]string)
	}
	s.tags[k] = v
}

// Collector 对外的采集入口。
type Collector struct {
	cfg       Config
	sampler   Sampler
	store     *Store
	persister *Persister
	stopCh    chan struct{}
}

// NewCollector 创建采集器并启动后台回收。
func NewCollector(cfg Config) (*Collector, error) {
	cfg.normalize()

	var sampler Sampler
	switch {
	case cfg.SampleRate <= 0:
		sampler = neverSampler{}
	case cfg.SampleRate >= 1:
		sampler = alwaysSampler{}
	default:
		sampler = NewRateSampler(cfg.SampleRate)
	}

	store := NewStore(cfg)

	var persister *Persister
	if cfg.DataDir != "" {
		p, err := NewPersister(cfg)
		if err != nil {
			return nil, err
		}
		if err := p.Recover(store); err != nil {
			p.Close()
			return nil, err
		}
		persister = p
	}

	c := &Collector{
		cfg:       cfg,
		sampler:   sampler,
		store:     store,
		persister: persister,
		stopCh:    make(chan struct{}),
	}
	go c.runReaper()
	return c, nil
}

// StartSpan 开始一个环节。若 ctx 中已有链路上下文，则作为其子环节，否则开始一条新链路。
// 返回的 context 可继续向子环节传播。
func (c *Collector) StartSpan(ctx context.Context, name string) (context.Context, *SpanHandle) {
	parent := SpanFromContext(ctx)
	var traceID TraceID
	var parentID SpanID
	var sampled bool
	if parent.hasLocal {
		traceID = parent.TraceID
		parentID = parent.SpanID
		sampled = parent.Sampled
	} else {
		traceID = newTraceID()
		sampled = c.sampler.Sample(traceID)
	}
	spanID := newSpanID()
	handle := &SpanHandle{
		traceID:   traceID,
		spanID:    spanID,
		parentID:  parentID,
		name:      name,
		start:     time.Now(),
		sampled:   sampled,
		collector: c,
	}
	newCtx := context.WithValue(ctx, spanContextKey{}, SpanContext{
		TraceID:  traceID,
		SpanID:   spanID,
		Sampled:  sampled,
		hasLocal: true,
	})
	return newCtx, handle
}

// Store 返回底层存储，便于查询。
func (c *Collector) Store() *Store { return c.store }

// Shutdown 刷盘并停止后台任务。
func (c *Collector) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	if c.persister != nil {
		if err := c.persister.Flush(); err != nil {
			return err
		}
		return c.persister.Close()
	}
	return nil
}

// report 上报单个环节。采样未命中且不保留异常时走廉价路径，不写存储。
func (c *Collector) report(span *Span, sampled bool) {
	if !sampled && !c.cfg.KeepError {
		return
	}
	c.store.AddSpan(span)
	if c.persister != nil {
		_ = c.persister.AppendSpan(span)
	}
}

// seal 封存链路。未命中采样且无异常的链路被丢弃。
func (c *Collector) seal(traceID TraceID, sampled bool) {
	if sampled {
		c.store.Seal(traceID)
		if c.persister != nil {
			_ = c.persister.AppendSeal(traceID)
		}
		return
	}
	if c.cfg.KeepError {
		if trace, ok := c.store.Get(traceID); ok && trace.HasError() {
			c.store.Seal(traceID)
			if c.persister != nil {
				_ = c.persister.AppendSeal(traceID)
			}
			return
		}
	}
	// 未命中采样且无异常：丢弃。
	c.store.Drop(traceID)
	if c.persister != nil {
		_ = c.persister.AppendDelete(traceID)
	}
}

func newTraceID() TraceID {
	var id TraceID
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return id
}

func newSpanID() SpanID {
	var id SpanID
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return id
}
