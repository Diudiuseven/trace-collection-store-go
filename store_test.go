package trace

import (
	"context"
	"testing"
	"time"
)

func TestOutOfOrderSpans(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	// 环节结束顺序与开始顺序不一致：child 先于 root 结束。
	ctx, root := c.StartSpan(context.Background(), "root")
	ctx2, child := c.StartSpan(ctx, "child")
	child.End()
	root.End()
	_ = ctx2

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(page.Traces))
	}
	tr := page.Traces[0]
	if len(tr.Spans) != 2 {
		t.Fatalf("want 2 spans, got %d", len(tr.Spans))
	}
	// root 开始最早，child 结束最晚（child 先结束但开始晚）。
	byName := map[string]*Span{}
	for _, sp := range tr.Spans {
		byName[sp.Name] = sp
	}
	if byName["root"].Start.After(byName["child"].Start) {
		t.Fatal("root should start before child")
	}
}

func TestLateSpanAfterSeal(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, root := c.StartSpan(context.Background(), "root")
	root.End() // 封存链路

	// 迟到的环节：在 root 封存后才开始并结束。
	_, late := c.StartSpan(ctx, "late")
	time.Sleep(time.Millisecond)
	late.End()

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(page.Traces))
	}
	tr := page.Traces[0]
	if len(tr.Spans) != 2 {
		t.Fatalf("late span should not be dropped, want 2 spans, got %d", len(tr.Spans))
	}
	found := false
	for _, sp := range tr.Spans {
		if sp.Name == "late" {
			found = true
		}
	}
	if !found {
		t.Fatal("late span missing")
	}
}

func TestQueryByTimeRange(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	// 创建两条不同时间开始的链路。
	ctx1, r1 := c.StartSpan(context.Background(), "r1")
	r1.End()
	_ = ctx1
	time.Sleep(10 * time.Millisecond)
	mid := time.Now()
	time.Sleep(10 * time.Millisecond)
	ctx2, r2 := c.StartSpan(context.Background(), "r2")
	r2.End()
	_ = ctx2

	// 查询 mid 之后开始的链路，应只返回 r2。
	page, err := c.Store().Query(context.Background(), Query{
		StartTime: mid,
		Limit:     10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace after mid, got %d", len(page.Traces))
	}
	if page.Traces[0].RootSpan().Name != "r2" {
		t.Fatalf("want r2, got %s", page.Traces[0].RootSpan().Name)
	}
}

func TestQueryBySpanName(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx1, r1 := c.StartSpan(context.Background(), "r1")
	_, c1 := c.StartSpan(ctx1, "http")
	c1.End()
	r1.End()

	ctx2, r2 := c.StartSpan(context.Background(), "r2")
	_, c2 := c.StartSpan(ctx2, "db")
	c2.End()
	r2.End()

	page, err := c.Store().Query(context.Background(), Query{SpanName: "http", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace with http, got %d", len(page.Traces))
	}
	if page.Traces[0].RootSpan().Name != "r1" {
		t.Fatalf("want r1, got %s", page.Traces[0].RootSpan().Name)
	}
}

func TestQueryByDurationThreshold(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx1, r1 := c.StartSpan(context.Background(), "r1")
	_, c1 := c.StartSpan(ctx1, "fast")
	c1.End()
	r1.End()

	ctx2, r2 := c.StartSpan(context.Background(), "r2")
	_, c2 := c.StartSpan(ctx2, "slow")
	time.Sleep(20 * time.Millisecond)
	c2.End()
	r2.End()

	page, err := c.Store().Query(context.Background(), Query{
		MinDuration: 10 * time.Millisecond,
		Limit:       10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace with slow span, got %d", len(page.Traces))
	}
	if page.Traces[0].RootSpan().Name != "r2" {
		t.Fatalf("want r2, got %s", page.Traces[0].RootSpan().Name)
	}
}

func TestQueryPagination(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	const total = 25
	for i := 0; i < total; i++ {
		ctx, r := c.StartSpan(context.Background(), "root")
		r.End()
		_ = ctx
	}

	// 第一页取 10 条。
	page1, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Traces) != 10 {
		t.Fatalf("want 10 in page1, got %d", len(page1.Traces))
	}
	if page1.NextCursor == "" {
		t.Fatal("want non-empty cursor")
	}

	// 第二页。
	page2, err := c.Store().Query(context.Background(), Query{Limit: 10, Cursor: page1.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Traces) != 10 {
		t.Fatalf("want 10 in page2, got %d", len(page2.Traces))
	}

	// 第三页应只有 5 条。
	page3, err := c.Store().Query(context.Background(), Query{Limit: 10, Cursor: page2.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(page3.Traces) != 5 {
		t.Fatalf("want 5 in page3, got %d", len(page3.Traces))
	}
	if page3.NextCursor != "" {
		t.Fatal("want empty cursor on last page")
	}

	// 各页不应重复。
	seen := map[TraceID]bool{}
	for _, p := range []*Page{page1, page2, page3} {
		for _, tr := range p.Traces {
			if seen[tr.TraceID] {
				t.Fatalf("duplicate trace %s across pages", tr.TraceID)
			}
			seen[tr.TraceID] = true
		}
	}
	if len(seen) != total {
		t.Fatalf("want %d unique traces, got %d", total, len(seen))
	}
}

func TestQueryTimeout(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	for i := 0; i < 10; i++ {
		ctx, r := c.StartSpan(context.Background(), "root")
		r.End()
		_ = ctx
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // 确保超时
	_, err = c.Store().Query(ctx, Query{Limit: 10})
	if err == nil {
		t.Fatal("want timeout error")
	}
}

func TestQueryLimitCapped(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	for i := 0; i < 5; i++ {
		ctx, r := c.StartSpan(context.Background(), "root")
		r.End()
		_ = ctx
	}

	// Limit 为 0 时使用默认值，结果不超过默认上限。
	page, err := c.Store().Query(context.Background(), Query{Limit: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 5 {
		t.Fatalf("want 5, got %d", len(page.Traces))
	}
}
