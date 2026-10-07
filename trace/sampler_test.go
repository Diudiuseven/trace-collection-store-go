package trace

import (
	"context"
	"math"
	"sync"
	"testing"
)

// TestRatioSamplerRates 小数采样率在大量随机链路下的实际命中比例
// 必须贴近配置值，而不是只有 0/1 两挡。
func TestRatioSamplerRates(t *testing.T) {
	const n = 20000
	ids := make([]ID, n)
	for i := range ids {
		ids[i] = newID()
	}
	for _, rate := range []float64{0.1, 0.25, 0.5, 0.9} {
		s := RatioSampler{Rate: rate}
		hits := 0
		for _, id := range ids {
			if s.Decide(id).Sampled {
				hits++
			}
		}
		got := float64(hits) / n
		// n=20000 下二项分布的波动远小于 3 个百分点，该容差在多次重复
		// 运行（go test -count）下都稳定通过。
		const tol = 0.03
		if math.Abs(got-rate) > tol {
			t.Errorf("Rate=%v 实际比例 %.4f 超出容差 ±%.2f", rate, got, tol)
		}
	}
}

// TestRatioSamplerDistinctRates 不同采样率必须给出可区分的结果。
// 修复前所有链路共享同一散列值（零值 ID），任意 0<rate<1 结果都相同。
func TestRatioSamplerDistinctRates(t *testing.T) {
	const n = 10000
	rates := []float64{0.1, 0.25, 0.5, 0.9}
	counts := make([]int, len(rates))
	for i := 0; i < n; i++ {
		id := newID()
		for j, rate := range rates {
			if (RatioSampler{Rate: rate}).Decide(id).Sampled {
				counts[j]++
			}
		}
	}
	for j := 1; j < len(counts); j++ {
		if counts[j] <= counts[j-1] {
			t.Fatalf("采样率提高但命中数未增加: %v", counts)
		}
	}
	t.Logf("各采样率命中数: %v", counts)
}

// TestRatioSamplerBoundaries 0 与 1（含越界、NaN）的确定性行为。
func TestRatioSamplerBoundaries(t *testing.T) {
	zeroRates := []float64{0, -0.1, -100, math.NaN()}
	for _, rate := range zeroRates {
		for i := 0; i < 1000; i++ {
			if (RatioSampler{Rate: rate}).Decide(newID()).Sampled {
				t.Fatalf("Rate=%v 应收敛为一条不采", rate)
			}
		}
	}
	oneRates := []float64{1, 1.1, 100}
	for _, rate := range oneRates {
		for i := 0; i < 1000; i++ {
			if !(RatioSampler{Rate: rate}).Decide(newID()).Sampled {
				t.Fatalf("Rate=%v 应收敛为全部保留", rate)
			}
		}
	}
}

// TestRatioSamplerDeterministic 同一 ID 反复决策结果恒定，
// 且与穿插的其他调用（调用顺序）无关。
func TestRatioSamplerDeterministic(t *testing.T) {
	s := RatioSampler{Rate: 0.5, KeepErrors: true}
	for i := 0; i < 500; i++ {
		id := newID()
		first := s.Decide(id)
		for j := 0; j < 10; j++ {
			_ = s.Decide(newID())
		}
		if again := s.Decide(id); again != first {
			t.Fatalf("链路 %s 决策发生翻转: %+v != %+v", id, again, first)
		}
	}
}

// TestRatioSamplerKeepErrorsIndependent 异常保留开关与采样率相互独立。
func TestRatioSamplerKeepErrorsIndependent(t *testing.T) {
	for _, keep := range []bool{true, false} {
		s := RatioSampler{Rate: 0.5, KeepErrors: keep}
		for i := 0; i < 1000; i++ {
			if d := s.Decide(newID()); d.KeepErrors != keep {
				t.Fatalf("KeepErrors=%v 未贯穿决策", keep)
			}
		}
	}
}

// TestSamplingConcurrentIndependenceAndWholeTrace 并发链路各自独立决策，
// 且同一链路的决策贯穿根与全部子环节。
func TestSamplingConcurrentIndependenceAndWholeTrace(t *testing.T) {
	sink := &memSink{}
	tr := NewTracer(sink, RatioSampler{Rate: 0.5})

	const goroutines = 64
	var wg sync.WaitGroup
	type result struct {
		traceID ID
		sampled bool
	}
	resCh := make(chan result, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, endRoot := tr.Start(context.Background(), "root")
			ends := make([]func(bool), 4)
			for i := range ends {
				_, ends[i] = tr.Start(ctx, "child")
			}
			// 子环节先结束、根最后结束（乱序上报）
			for _, end := range ends {
				end(false)
			}
			endRoot(false)
			resCh <- result{traceID: TraceID(ctx), sampled: tr.sampler.Decide(TraceID(ctx)).Sampled}
		}()
	}
	wg.Wait()
	close(resCh)

	trues, falses := 0, 0
	want := map[ID]bool{}
	for r := range resCh {
		want[r.traceID] = r.sampled
		if r.sampled {
			trues++
		} else {
			falses++
		}
	}
	if trues == 0 || falses == 0 {
		t.Fatalf("并发链路决策不独立：采样=%d 未采样=%d", trues, falses)
	}

	// 已上报的环节必须整条链路一致（KeepErrors=false 时未采样链路不上报；
	// 采样命中的链路所有环节 Sampled 位必须相同）。
	got := map[ID]bool{}
	count := map[ID]int{}
	for _, sp := range sink.spans {
		count[sp.TraceID]++
		if d, ok := got[sp.TraceID]; ok && d != sp.Sampled {
			t.Fatalf("链路 %s 中途采样决策不一致", sp.TraceID)
		}
		got[sp.TraceID] = sp.Sampled
	}
	for id := range want {
		if !want[id] {
			// 未命中且未开启异常保留：一个环节都不应上报
			if count[id] != 0 {
				t.Fatalf("未采样链路 %s 上报了 %d 个环节", id, count[id])
			}
			continue
		}
		if count[id] != 5 {
			t.Fatalf("采样链路 %s 环节不完整: %d/5", id, count[id])
		}
		if !got[id] {
			t.Fatalf("采样链路 %s 的 Sampled 位未贯穿", id)
		}
	}
}

// TestSamplingConcurrentRatio 大量并发链路的实际比例贴近配置值，
// 且不随并发规模与调用顺序漂移。
func TestSamplingConcurrentRatio(t *testing.T) {
	for _, rate := range []float64{0.1, 0.25, 0.5, 0.9} {
		sink := &memSink{}
		tr := NewTracer(sink, RatioSampler{Rate: rate})
		const n = 4000
		decisions := make([]bool, n)
		var wg sync.WaitGroup
		jobs := make(chan int)
		for w := 0; w < 32; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					ctx, end := tr.Start(context.Background(), "root")
					_, endChild := tr.Start(ctx, "child")
					endChild(false)
					end(false)
					decisions[i] = tr.sampler.Decide(TraceID(ctx)).Sampled
				}
			}()
		}
		for i := 0; i < n; i++ {
			jobs <- i
		}
		close(jobs)
		wg.Wait()

		hits := 0
		for _, d := range decisions {
			if d {
				hits++
			}
		}
		got := float64(hits) / n
		const tol = 0.04
		if math.Abs(got-rate) > tol {
			t.Fatalf("并发 Rate=%v 实际比例 %.4f 超出容差 ±%.2f", rate, got, tol)
		}
	}
}

// BenchmarkSamplerDecide 决策开销：一次散列计算，应保持纳秒级且无分配。
func BenchmarkSamplerDecide(b *testing.B) {
	s := RatioSampler{Rate: 0.1}
	id := newID()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = s.Decide(id)
	}
}
