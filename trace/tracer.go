package trace

import (
	"context"
	"time"
)

// Sink 接收采集到的环节上报，实现方必须保证并发安全。
type Sink interface {
	Report(s Span)
}

// Tracer 负责在进程内采集追踪条目，可并发使用。
type Tracer struct {
	sink    Sink
	sampler Sampler
	now     func() time.Time // 便于测试注入
}

// NewTracer 创建采集器。sink 不可为 nil；sampler 为 nil 时全部保留。
func NewTracer(sink Sink, sampler Sampler) *Tracer {
	if sampler == nil {
		sampler = RatioSampler{Rate: 1}
	}
	return &Tracer{sink: sink, sampler: sampler, now: time.Now}
}

// Start 在 ctx 上开启一个新环节：
//   - 若 ctx 已携带环节上下文，则新环节作为其子环节，继承同一条链路的采样决策；
//   - 否则开启一条新链路，并在此时一次性确定采样决策。
//
// 返回的结束函数可安全 defer 调用，不改变被测代码的返回值。
// 未采样的链路开销极小：不上报、不分配环节记录。
func (t *Tracer) Start(ctx context.Context, name string) (context.Context, func(err bool)) {
	start := t.now()
	sc, ok := fromContext(ctx)
	if !ok {
		// 新链路：一次性决策，贯穿整条链路。
		sc = spanContext{traceID: newID(), decision: t.sampler.Decide(sc.traceID)}
	}
	child := spanContext{
		traceID:  sc.traceID,
		spanID:   newSpanID(),
		decision: sc.decision,
	}
	ctx = withContext(ctx, child)
	parentID := sc.spanID // 无父环节时为零值

	if !child.decision.record() {
		return ctx, func(bool) {}
	}
	return ctx, func(err bool) {
		t.sink.Report(Span{
			TraceID:  child.traceID,
			SpanID:   child.spanID,
			ParentID: parentID,
			Name:     name,
			Start:    start,
			End:      t.now(),
			Err:      err,
			Sampled:  child.decision.Sampled,
		})
	}
}

// TraceID 返回 ctx 所属链路的 ID，便于日志关联。
func TraceID(ctx context.Context) ID {
	sc, ok := fromContext(ctx)
	if !ok {
		return ID{}
	}
	return sc.traceID
}

// Do 采集 fn 的执行并原样返回其结果，不改变被测代码的返回值。
// fn 返回的 error 非 nil 时环节标记为异常。
func Do[T any](ctx context.Context, t *Tracer, name string, fn func(context.Context) (T, error)) (T, error) {
	ctx, end := t.Start(ctx, name)
	res, err := fn(ctx)
	end(err != nil)
	return res, err
}
