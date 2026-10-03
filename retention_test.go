package trace

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRetentionByTTL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetentionPeriod = 50 * time.Millisecond
	cfg.ReapInterval = 50 * time.Millisecond
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, r := c.StartSpan(context.Background(), "root")
	r.End()
	_ = ctx

	// 立即查询应存在。
	page, _ := c.Store().Query(context.Background(), Query{Limit: 10})
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace before TTL, got %d", len(page.Traces))
	}

	// 等待超过 TTL 后回收。
	time.Sleep(80 * time.Millisecond)
	c.ReapNow()

	page, _ = c.Store().Query(context.Background(), Query{Limit: 10})
	if len(page.Traces) != 0 {
		t.Fatalf("want 0 traces after TTL, got %d", len(page.Traces))
	}
}

func TestRetentionByMaxTraces(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxTraces = 5
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	for i := 0; i < 10; i++ {
		ctx, r := c.StartSpan(context.Background(), fmt.Sprintf("root-%d", i))
		r.End()
		_ = ctx
	}
	c.ReapNow()

	page, err := c.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 5 {
		t.Fatalf("want 5 traces after cap, got %d", len(page.Traces))
	}
	// 应保留最新的 5 条。
	names := map[string]bool{}
	for _, tr := range page.Traces {
		names[tr.RootSpan().Name] = true
	}
	for i := 5; i < 10; i++ {
		if !names[fmt.Sprintf("root-%d", i)] {
			t.Fatalf("trace root-%d should be kept", i)
		}
	}
}

func TestReaperConcurrentWithCollectionAndQuery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxTraces = 50
	cfg.ReapInterval = 5 * time.Millisecond
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 持续采集。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				ctx, r := c.StartSpan(context.Background(), "root")
				r.End()
				_ = ctx
			}
		}
	}()

	// 持续查询。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = c.Store().Query(context.Background(), Query{Limit: 10})
			}
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	// 回收后不应超过上限。
	c.ReapNow()
	page, _ := c.Store().Query(context.Background(), Query{Limit: 1000})
	if len(page.Traces) > 50 {
		t.Fatalf("want <= 50 traces, got %d", len(page.Traces))
	}
}

func TestAbandonedTraceReaped(t *testing.T) {
	cfg := DefaultConfig()
	cfg.AbandonTimeout = 50 * time.Millisecond
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	// 开始一条链路但不封存（不结束 root）。
	ctx, _ := c.StartSpan(context.Background(), "root")
	_ = ctx

	time.Sleep(80 * time.Millisecond)
	c.ReapNow()

	page, _ := c.Store().Query(context.Background(), Query{Limit: 10})
	if len(page.Traces) != 0 {
		t.Fatalf("abandoned trace should be reaped, got %d", len(page.Traces))
	}
}
