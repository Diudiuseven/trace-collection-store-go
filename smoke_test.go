package trace

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSmokeBasic(t *testing.T) {
	c, err := NewCollector(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Shutdown(context.Background())

	ctx, root := c.StartSpan(context.Background(), "root")
	time.Sleep(2 * time.Millisecond)
	_, child := c.StartSpan(ctx, "child")
	time.Sleep(2 * time.Millisecond)
	child.End()
	root.End()

	time.Sleep(50 * time.Millisecond)

	page, err := c.Store().Query(context.Background(), Query{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("got %d traces\n", len(page.Traces))
	for i, tr := range page.Traces {
		fmt.Printf("trace %d: id=%s spans=%d sealed=%v\n", i, tr.TraceID, len(tr.Spans), tr.Sealed)
		for _, sp := range tr.Spans {
			fmt.Printf("  span: %s parent=%s name=%s\n", sp.SpanID, sp.ParentID, sp.Name)
		}
	}
	if len(page.Traces) != 1 {
		t.Fatalf("want 1 trace, got %d", len(page.Traces))
	}
	tr := page.Traces[0]
	if len(tr.Spans) != 2 {
		t.Fatalf("want 2 spans, got %d", len(tr.Spans))
	}
}
