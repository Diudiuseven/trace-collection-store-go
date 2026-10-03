package trace

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// memSink 收集上报的环节。
type memSink struct {
	mu    sync.Mutex
	spans []Span
}

func (m *memSink) Report(s Span) {
	m.mu.Lock()
	m.spans = append(m.spans, s)
	m.mu.Unlock()
}

func TestTracerParentChildAndParallel(t *testing.T) {
	sink := &memSink{}
	tr := NewTracer(sink, nil)

	ctx, endRoot := tr.Start(context.Background(), "root")
	// 两个并行子环节
	_, endA := tr.Start(ctx, "a")
	_, endB := tr.Start(ctx, "b")
	endA(false)
	endB(false)
	endRoot(false)

	if len(sink.spans) != 3 {
		t.Fatalf("期望 3 个环节，得到 %d", len(sink.spans))
	}
	byName := map[string]Span{}
	for _, sp := range sink.spans {
		byName[sp.Name] = sp
		if sp.TraceID != TraceID(ctx) {
			t.Fatalf("环节 %s 链路 ID 不一致", sp.Name)
		}
	}
	root := byName["root"]
	if !root.ParentID.IsZero() {
		t.Fatal("根环节父 ID 应为零值")
	}
	for _, name := range []string{"a", "b"} {
		if byName[name].ParentID != root.SpanID {
			t.Fatalf("环节 %s 的父环节应为 root", name)
		}
	}
	// 时间位置可还原
	if root.Start.After(byName["a"].Start) || root.End.Before(byName["b"].End) {
		t.Fatal("根环节应覆盖子环节的时间范围")
	}
}

func TestDoPreservesReturnValues(t *testing.T) {
	sink := &memSink{}
	tr := NewTracer(sink, nil)
	wantErr := errors.New("boom")
	v, err := Do(context.Background(), tr, "op", func(ctx context.Context) (int, error) {
		return 42, wantErr
	})
	if v != 42 || !errors.Is(err, wantErr) {
		t.Fatalf("返回值被改变: %v, %v", v, err)
	}
	if len(sink.spans) != 1 || !sink.spans[0].Err {
		t.Fatal("异常未被标记到环节上")
	}
}

func TestSamplingDecisionConsistentAcrossTrace(t *testing.T) {
	sink := &memSink{}
	tr := NewTracer(sink, RatioSampler{Rate: 0, KeepErrors: false})
	ctx, endRoot := tr.Start(context.Background(), "root")
	_, endChild := tr.Start(ctx, "child")
	endChild(false)
	endRoot(false)
	if len(sink.spans) != 0 {
		t.Fatalf("未采样链路不应有环节上报，得到 %d", len(sink.spans))
	}

	sink2 := &memSink{}
	tr2 := NewTracer(sink2, RatioSampler{Rate: 1})
	ctx2, endRoot2 := tr2.Start(context.Background(), "root")
	_, endChild2 := tr2.Start(ctx2, "child")
	endChild2(false)
	endRoot2(false)
	if len(sink2.spans) != 2 {
		t.Fatalf("全采样链路应有 2 个环节，得到 %d", len(sink2.spans))
	}
	for _, sp := range sink2.spans {
		if !sp.Sampled {
			t.Fatal("采样标记未贯穿整条链路")
		}
	}
}

func TestConcurrentTracesNoCrossTalk(t *testing.T) {
	sink := &memSink{}
	tr := NewTracer(sink, nil)
	const workers = 32
	const spansPerTrace = 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx, endRoot := tr.Start(context.Background(), "root")
			tid := TraceID(ctx)
			if tid.IsZero() {
				t.Error("链路 ID 不应为零")
			}
			ends := make([]func(bool), spansPerTrace)
			for i := 0; i < spansPerTrace; i++ {
				_, ends[i] = tr.Start(ctx, "child")
			}
			for _, end := range ends {
				end(false)
			}
			endRoot(false)
		}(w)
	}
	wg.Wait()

	want := workers * (spansPerTrace + 1)
	if len(sink.spans) != want {
		t.Fatalf("期望 %d 个环节，得到 %d", want, len(sink.spans))
	}
	// 每条链路的环节数必须精确等于其自身产生数，不得串扰
	perTrace := map[ID]int{}
	spanIDs := map[SpanID]bool{}
	for _, sp := range sink.spans {
		perTrace[sp.TraceID]++
		if spanIDs[sp.SpanID] {
			t.Fatal("环节 ID 冲突")
		}
		spanIDs[sp.SpanID] = true
	}
	if len(perTrace) != workers {
		t.Fatalf("期望 %d 条链路，得到 %d", workers, len(perTrace))
	}
	for id, n := range perTrace {
		if n != spansPerTrace+1 {
			t.Fatalf("链路 %s 环节数 %d，期望 %d", id, n, spansPerTrace+1)
		}
	}
}
