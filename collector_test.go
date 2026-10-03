package trace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSpanParentChild(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, root := c.StartSpan(context.Background(), "root")
	ctx2, child := c.StartSpan(ctx, "child")
	_, grandchild := c.StartSpan(ctx2, "grandchild")
	grandchild.End()
	child.End()
	root.End()

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(page.Traces))
	}
	tr := page.Traces[0]
	if len(tr.Spans) != 3 {
		t.Fatalf("want 3 spans, got %d", len(tr.Spans))
	}
	byName := map[string]*Span{}
	for _, sp := range tr.Spans {
		byName[sp.Name] = sp
	}
	if byName["child"].ParentID != byName["root"].SpanID {
		t.Fatal("child parent mismatch")
	}
	if byName["grandchild"].ParentID != byName["child"].SpanID {
		t.Fatal("grandchild parent mismatch")
	}
	if byName["root"].Start.After(byName["child"].Start) {
		t.Fatal("root should start before child")
	}
	if byName["child"].Start.After(byName["grandchild"].Start) {
		t.Fatal("child should start before grandchild")
	}
}

func TestConcurrentCollection(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	const goroutines = 50
	const spansPerG = 10
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ctx, root := c.StartSpan(context.Background(), fmt.Sprintf("root-%d", g))
			for i := 0; i < spansPerG; i++ {
				_, sp := c.StartSpan(ctx, fmt.Sprintf("span-%d-%d", g, i))
				time.Sleep(time.Microsecond)
				sp.End()
			}
			root.End()
		}(g)
	}
	wg.Wait()

	page, err := c.Store().Query(context.Background(), Query{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != goroutines {
		t.Fatalf("want %d traces, got %d", goroutines, len(page.Traces))
	}
	// 每条链路的 TraceID 必须一致，不同链路不得串扰。
	for _, tr := range page.Traces {
		for _, sp := range tr.Spans {
			if sp.TraceID != tr.TraceID {
				t.Fatalf("span %s has traceID %s, want %s", sp.Name, sp.TraceID, tr.TraceID)
			}
		}
	}
}

func TestCollectionDoesNotChangeReturnValue(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	work := func(ctx context.Context) (int, error) {
		_, sp := c.StartSpan(ctx, "work")
		defer sp.End()
		return 42, nil
	}

	n, err := work(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 42 {
		t.Fatalf("want 42, got %d", n)
	}

	workErr := func(ctx context.Context) (string, error) {
		_, sp := c.StartSpan(ctx, "work-err")
		defer sp.End()
		return "", errors.New("boom")
	}
	s, err := workErr(context.Background())
	if err == nil || err.Error() != "boom" {
		t.Fatalf("want boom error, got %v", err)
	}
	if s != "" {
		t.Fatalf("want empty string, got %q", s)
	}
}

func TestSpanEndIdempotent(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, root := c.StartSpan(context.Background(), "root")
	root.End()
	root.End()
	_ = ctx

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(page.Traces))
	}
	if len(page.Traces[0].Spans) != 1 {
		t.Fatalf("want 1 span, got %d", len(page.Traces[0].Spans))
	}
}

func TestSpanTagsAndStatus(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, root := c.StartSpan(context.Background(), "root")
	root.SetTag("env", "test")
	root.SetStatus(StatusError)
	root.End()
	_ = ctx

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	tr := page.Traces[0]
	if tr.RootSpan().Tags["env"] != "test" {
		t.Fatal("tag mismatch")
	}
	if !tr.HasError() {
		t.Fatal("want error status")
	}
}
