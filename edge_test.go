package trace

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentSpansSameTrace(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, root := c.StartSpan(context.Background(), "root")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, sp := c.StartSpan(ctx, fmt.Sprintf("span-%d", i))
			time.Sleep(time.Microsecond)
			sp.End()
		}(i)
	}
	wg.Wait()
	root.End()

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(page.Traces))
	}
	if len(page.Traces[0].Spans) != 21 {
		t.Fatalf("want 21 spans, got %d", len(page.Traces[0].Spans))
	}
}

func TestWALCompaction(t *testing.T) {
	dir := tempDir(t)
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.CompactThreshold = 100 // 很小的阈值，触发压缩。
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		ctx, r := c.StartSpan(context.Background(), "root")
		r.End()
		_ = ctx
	}
	// 触发压缩。
	c.ReapNow()
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 重启后数据仍可查询。
	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())
	page, err := c2.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 50 {
		t.Fatalf("want 50 traces after compaction+recovery, got %d", len(page.Traces))
	}
}

func TestSampledOutCheapPath(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleRate = 0
	cfg.KeepError = false
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	// 采样未命中时，即使大量调用也不应产生存储开销。
	for i := 0; i < 1000; i++ {
		ctx, r := c.StartSpan(context.Background(), "root")
		r.End()
		_ = ctx
	}
	page, _ := c.Store().Query(context.Background(), Query{Limit: 10})
	if len(page.Traces) != 0 {
		t.Fatalf("want 0 traces, got %d", len(page.Traces))
	}
}

func TestQueryEmptyCursor(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	// 空 cursor 应正常返回第一页。
	page, err := c.Store().Query(context.Background(), Query{Limit: 10, Cursor: ""})
	if err != nil {
		t.Fatal(err)
	}
	if page.NextCursor != "" {
		t.Fatal("want empty cursor for empty result")
	}
}

func TestQueryInvalidCursor(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	_, err = c.Store().Query(context.Background(), Query{Limit: 10, Cursor: "invalid!!!"})
	if err == nil {
		t.Fatal("want error for invalid cursor")
	}
}

func TestShutdownFlushes(t *testing.T) {
	dir := tempDir(t)
	cfg := DefaultConfig()
	cfg.DataDir = dir
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, r := c.StartSpan(context.Background(), "root")
	r.End()
	_ = ctx
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 重启后数据应存在。
	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())
	page, _ := c2.Store().Query(context.Background(), Query{Limit: 10})
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace after shutdown+recovery, got %d", len(page.Traces))
	}
}
