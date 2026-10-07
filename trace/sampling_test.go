package trace

import (
	"context"
	"math"
	"sync"
	"testing"
)

// runTrace 通过 tracer 完整跑一条含 1 个根 + children 个子环节的链路，
// 返回链路 ID 与实际上报的环节数。
func runTrace(ctx context.Context, tr *Tracer, children int) (ID, int) {
	ctx, endRoot := tr.Start(ctx, "root")
	var wg sync.WaitGroup
	ends := make([]func(bool), children)
	for i := 0; i < children; i++ {
		_, ends[i] = tr.Start(ctx, "child")
	}
	wg.Wait()
	for _, end := range ends {
		end(false)
	}
	endRoot(false)
	return TraceID(ctx), children + 1
}

// TestTracerFractionRatesEndToEnd 验证修复后 tracer 端到端的实际采集比例。
// 回归点：旧实现让所有链路拿零值 ID 决策，结果要么全采要么全不采。
func TestTracerFractionRatesEndToEnd(t *testing.T) {
	const traces = 8000
	for _, rate := range []float64{0.1, 0.25, 0.5, 0.9} {
		sink := &memSink{}
		tr := NewTracer(sink, RatioSampler{Rate: rate})
		var sampled int
		for i := 0; i < traces; i++ {
			_, _ = runTrace(context.Background(), tr, 1)
		}
		// 每条上报链路恰好 2 个环节，命中链路数 = 环节数 / 2。
		sampled = len(sink.spans) / 2
		got := float64(sampled) / traces
		if math.Abs(got-rate) > 0.02 {
			t.Fatalf("Rate=%v 端到端实际比例 %.4f 偏离配置", rate, got)
		}
	}
}

// TestTracerConcurrentDecisionIndependentAndSticky 并发生成大量链路，验证：
// 1) 不同链路的决策相互独立（既有命中也有未命中，非整批同进同出）；
// 2) 同一链路所有环节的 Sampled 标记完全一致（决策贯穿整条链路，无部分环节缺失）；
// 3) 每条命中链路环节数完整，未命中链路零上报。
func TestTracerConcurrentDecisionIndependentAndSticky(t *testing.T) {
	sink := &memSink{}
	tr := NewTracer(sink, RatioSampler{Rate: 0.5})

	const workers = 64
	const children = 7
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 每个 goroutine 再串行跑多条链路，凑足样本量。
			for k := 0; k < 125; k++ {
				runTrace(context.Background(), tr, children)
			}
		}()
	}
	wg.Wait()

	const total = workers * 125
	perTrace := map[ID]struct {
		spans   int
		sampled bool
		anyS    bool
		anyNS   bool
	}{}
	for _, sp := range sink.spans {
		e := perTrace[sp.TraceID]
		e.spans++
		if sp.Sampled {
			e.anyS = true
		} else {
			e.anyNS = true
		}
		e.sampled = e.anyS
		perTrace[sp.TraceID] = e
	}

	if len(perTrace) == 0 || len(perTrace) == total {
		t.Fatalf("决策不独立：上报链路数 %d / %d", len(perTrace), total)
	}
	for id, e := range perTrace {
		if e.anyS && e.anyNS {
			t.Fatalf("链路 %s 的 Sampled 标记未贯穿整条链路", id)
		}
		if e.spans != children+1 {
			t.Fatalf("链路 %s 环节不完整：%d", id, e.spans)
		}
	}
	// 比例也要落在合理带内（total=8000）。
	got := float64(len(perTrace)) / total
	if math.Abs(got-0.5) > 0.03 {
		t.Fatalf("并发实际比例 %.4f 偏离 0.5", got)
	}
}

// TestTracerUnsampledTracesReportNothing 验证未命中且不保留异常的链路
// 零环节上报：不占存储、热路径开销仅一次决策。
func TestTracerUnsampledTracesReportNothing(t *testing.T) {
	sink := &memSink{}
	tr := NewTracer(sink, RatioSampler{Rate: 0, KeepErrors: false})
	for i := 0; i < 1000; i++ {
		runTrace(context.Background(), tr, 4)
	}
	if len(sink.spans) != 0 {
		t.Fatalf("Rate=0 不应有任何环节上报，得到 %d", len(sink.spans))
	}
}
