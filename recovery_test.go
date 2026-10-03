package trace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "trace-recovery-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestRecoveryCompleteTraces(t *testing.T) {
	dir := tempDir(t)
	cfg := DefaultConfig()
	cfg.DataDir = dir

	// 第一轮：创建完整链路。
	c1, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		ctx, r := c1.StartSpan(context.Background(), fmt.Sprintf("root-%d", i))
		_, sp := c1.StartSpan(ctx, "child")
		sp.End()
		r.End()
	}
	if err := c1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 第二轮：重启后查询。
	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())

	page, err := c2.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 3 {
		t.Fatalf("want 3 traces after recovery, got %d", len(page.Traces))
	}
	for _, tr := range page.Traces {
		if !tr.Sealed {
			t.Fatalf("trace %s should be sealed", tr.TraceID)
		}
		if len(tr.Spans) != 2 {
			t.Fatalf("want 2 spans, got %d", len(tr.Spans))
		}
	}
}

func TestRecoveryIncompleteDiscard(t *testing.T) {
	dir := tempDir(t)
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.IncompletePolicy = IncompleteDiscard

	// 第一轮：创建一条完整链路和一条不完整链路。
	c1, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx1, r1 := c1.StartSpan(context.Background(), "complete")
	r1.End()
	_ = ctx1

	ctx2, root2 := c1.StartSpan(context.Background(), "incomplete")
	_, child2 := c1.StartSpan(ctx2, "child")
	child2.End() // 上报了环节但未封存，链路不完整。
	_ = root2

	if err := c1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 第二轮：重启后不完整链路应被丢弃。
	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())

	page, err := c2.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace (incomplete discarded), got %d", len(page.Traces))
	}
	if page.Traces[0].RootSpan().Name != "complete" {
		t.Fatalf("want complete trace, got %s", page.Traces[0].RootSpan().Name)
	}
}

func TestRecoveryIncompleteKeep(t *testing.T) {
	dir := tempDir(t)
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.IncompletePolicy = IncompleteKeep

	// 第一轮：创建不完整链路。
	c1, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, root := c1.StartSpan(context.Background(), "incomplete")
	_, child := c1.StartSpan(ctx, "child")
	child.End() // 上报了环节但未封存，链路不完整。
	_ = root
	if err := c1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 第二轮：重启后不完整链路被保留但不封存。
	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())

	page, err := c2.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace (incomplete kept), got %d", len(page.Traces))
	}
	if page.Traces[0].Sealed {
		t.Fatal("incomplete trace should not be sealed")
	}
}

func TestRecoveryWithLateSpan(t *testing.T) {
	dir := tempDir(t)
	cfg := DefaultConfig()
	cfg.DataDir = dir

	// 第一轮：创建链路，root 封存后有迟到环节。
	c1, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, r := c1.StartSpan(context.Background(), "root")
	r.End()
	_, late := c1.StartSpan(ctx, "late")
	late.End()
	if err := c1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 第二轮：重启后迟到环节应保留。
	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())

	page, err := c2.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(page.Traces))
	}
	if len(page.Traces[0].Spans) != 2 {
		t.Fatalf("want 2 spans (late kept), got %d", len(page.Traces[0].Spans))
	}
}

func TestRecoveryNoDataDir(t *testing.T) {
	// 无 DataDir 时仅内存，重启后数据丢失（符合预期）。
	cfg := DefaultConfig()
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

	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())
	page, _ := c2.Store().Query(context.Background(), Query{Limit: 10})
	if len(page.Traces) != 0 {
		t.Fatalf("want 0 traces (no persistence), got %d", len(page.Traces))
	}
}

func TestRecoveryDataDirCreated(t *testing.T) {
	dir := filepath.Join(tempDir(t), "nested", "data")
	cfg := DefaultConfig()
	cfg.DataDir = dir
	c, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatal("DataDir should be created")
	}
}

func TestRecoveryIncompleteKeepWithCompaction(t *testing.T) {
	dir := tempDir(t)
	cfg := DefaultConfig()
	cfg.DataDir = dir
	cfg.IncompletePolicy = IncompleteKeep
	cfg.CompactThreshold = 100 // 触发压缩。

	// 第一轮：创建不完整链路（有环节上报但未封存）。
	c1, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, root := c1.StartSpan(context.Background(), "incomplete")
	_, child := c1.StartSpan(ctx, "child")
	child.End()
	_ = root
	// 触发压缩。
	c1.ReapNow()
	if err := c1.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 第二轮：重启后不完整链路应被保留但不封存。
	c2, err := NewCollector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Shutdown(context.Background())

	page, err := c2.Store().Query(context.Background(), Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace (incomplete kept), got %d", len(page.Traces))
	}
	if page.Traces[0].Sealed {
		t.Fatal("incomplete trace should not be sealed")
	}
	if len(page.Traces[0].Spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(page.Traces[0].Spans))
	}
}
