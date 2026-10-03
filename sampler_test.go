package trace

import (
	"context"
	"testing"
)

func TestSamplingRateZero(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleRate = 0
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	for i := 0; i < 20; i++ {
		ctx, r := c.StartSpan(context.Background(), "root")
		r.End()
		_ = ctx
	}

	page, err := c.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 0 {
		t.Fatalf("want 0 traces with rate 0, got %d", len(page.Traces))
	}
}

func TestSamplingRateOne(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleRate = 1
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	for i := 0; i < 20; i++ {
		ctx, r := c.StartSpan(context.Background(), "root")
		r.End()
		_ = ctx
	}

	page, err := c.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 20 {
		t.Fatalf("want 20 traces with rate 1, got %d", len(page.Traces))
	}
}

func TestSamplingConsistentWithinTrace(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleRate = 0.5
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	// 同一条链路的所有环节必须共享同一采样决策，不得出现部分采样。
	ctx, root := c.StartSpan(context.Background(), "root")
	for i := 0; i < 5; i++ {
		_, sp := c.StartSpan(ctx, "child")
		sp.End()
	}
	root.End()

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	// 要么整条链路都在，要么都不在。
	if len(page.Traces) == 1 {
		if len(page.Traces[0].Spans) != 6 {
			t.Fatalf("partial sampling: want 6 spans, got %d", len(page.Traces[0].Spans))
		}
	}
}

func TestKeepErrorOverridesSampling(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleRate = 0
	cfg.KeepError = true
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	// 无异常链路：不保留。
	ctx1, r1 := c.StartSpan(context.Background(), "ok")
	r1.End()
	_ = ctx1

	// 有异常链路：即使采样率为 0 也保留。
	ctx2, r2 := c.StartSpan(context.Background(), "err")
	r2.SetStatus(StatusError)
	r2.End()
	_ = ctx2

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace (error kept), got %d", len(page.Traces))
	}
	if page.Traces[0].RootSpan().Name != "err" {
		t.Fatalf("want err trace, got %s", page.Traces[0].RootSpan().Name)
	}
}

func TestKeepErrorDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleRate = 0
	cfg.KeepError = false
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, r := c.StartSpan(context.Background(), "err")
	r.SetStatus(StatusError)
	r.End()
	_ = ctx

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 0 {
		t.Fatalf("want 0 traces, got %d", len(page.Traces))
	}
}

func TestRateSamplerDeterministic(t *testing.T) {
	sampler := NewRateSampler(0.5)
	id := newTraceID()
	first := sampler.Sample(id)
	for i := 0; i < 10; i++ {
		if sampler.Sample(id) != first {
			t.Fatal("sampler decision not deterministic for same trace ID")
		}
	}
}
