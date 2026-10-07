package trace

import (
	"math"
	"testing"
)

// sampleIDs 生成 n 个随机链路 ID，模拟进程内并发生成的众多链路。
func sampleIDs(n int) []ID {
	ids := make([]ID, n)
	for i := range ids {
		ids[i] = newID()
	}
	return ids
}

// TestRatioSamplerFractionRates 验证小数采样率在大量链路下的实际命中率贴近配置。
// 容忍带取 ±0.02：n=40000 时最差标准误约 0.0015，余量约 13 个标准差，重复运行稳定。
func TestRatioSamplerFractionRates(t *testing.T) {
	const n = 40000
	ids := sampleIDs(n)
	for _, rate := range []float64{0.1, 0.25, 0.5, 0.9} {
		s := RatioSampler{Rate: rate}
		hit := 0
		for _, id := range ids {
			if s.Decide(id).Sampled {
				hit++
			}
		}
		got := float64(hit) / n
		if math.Abs(got-rate) > 0.02 {
			t.Fatalf("Rate=%v 实际比例 %.4f 偏离配置过大", rate, got)
		}
	}
}

// TestRatioSamplerBoundaries 验证 0/1 与越界取值的确定性行为。
func TestRatioSamplerBoundaries(t *testing.T) {
	ids := sampleIDs(5000)
	cases := []struct {
		name string
		rate float64
		want bool // 期望全部命中 or 全部不命中
	}{
		{"零", 0, false},
		{"一", 1, true},
		{"负数收敛到零", -0.5, false},
		{"大于一收敛到一", 1.5, true},
		{"NaN 收敛到零", math.NaN(), false},
		{"+Inf 收敛到一", math.Inf(1), true},
		{"-Inf 收敛到零", math.Inf(-1), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := RatioSampler{Rate: c.rate}
			for _, id := range ids {
				if got := s.Decide(id).Sampled; got != c.want {
					t.Fatalf("Rate=%v 对 %s 决策=%v，期望全部为 %v", c.rate, id, got, c.want)
				}
			}
		})
	}
}

// TestRatioSamplerDeterministic 验证同一链路反复计算结果恒定，
// 且新建采样器实例（模拟进程重启）后决策不翻转。
func TestRatioSamplerDeterministic(t *testing.T) {
	ids := sampleIDs(2000)
	for _, rate := range []float64{0.0, 0.1, 0.5, 0.9, 1.0} {
		first := RatioSampler{Rate: rate}
		restarted := RatioSampler{Rate: rate} // 进程重启后的全新实例
		for _, id := range ids {
			want := first.Decide(id)
			for i := 0; i < 5; i++ {
				if got := first.Decide(id); got != want {
					t.Fatalf("同实例重复决策翻转: rate=%v id=%s", rate, id)
				}
			}
			if got := restarted.Decide(id); got != want {
				t.Fatalf("重启后决策翻转: rate=%v id=%s", rate, id)
			}
		}
	}
}

// TestRatioSamplerIndependentAcrossTraces 验证不同链路决策相互独立：
// 既不能全部相同，也不能成批同进同出；调用顺序变化不产生漂移。
func TestRatioSamplerIndependentAcrossTraces(t *testing.T) {
	const n = 20000
	ids := sampleIDs(n)
	s := RatioSampler{Rate: 0.5}

	dec := make(map[ID]bool, n)
	var hits int
	for _, id := range ids {
		d := s.Decide(id).Sampled
		dec[id] = d
		if d {
			hits++
		}
	}
	if hits == 0 || hits == n {
		t.Fatalf("决策退化为开关：命中 %d/%d", hits, n)
	}

	// 切成 20 个连续批次，各批命中率必须存在差异，禁止成批同进同出。
	const batches = 20
	step := n / batches
	firstBatchHits := -1
	allSame := true
	for b := 0; b < batches; b++ {
		bh := 0
		for i := b * step; i < (b+1)*step; i++ {
			if dec[ids[i]] {
				bh++
			}
		}
		if firstBatchHits == -1 {
			firstBatchHits = bh
		} else if bh != firstBatchHits {
			allSame = false
		}
	}
	if allSame {
		t.Fatal("各批次决策数完全一致，存在成批同进同出")
	}

	// 逆序（改变调用顺序）重新计算，结果必须完全一致，无漂移。
	for i := n - 1; i >= 0; i-- {
		if s.Decide(ids[i]).Sampled != dec[ids[i]] {
			t.Fatalf("调用顺序变化导致决策漂移: id=%s", ids[i])
		}
	}
}

// TestRatioSamplerKeepErrorsIndependent 验证异常保留开关独立携带，
// 与采样命中与否无关。
func TestRatioSamplerKeepErrorsIndependent(t *testing.T) {
	ids := sampleIDs(4000)
	for _, keep := range []bool{true, false} {
		s := RatioSampler{Rate: 0.5, KeepErrors: keep}
		for _, id := range ids {
			d := s.Decide(id)
			if d.KeepErrors != keep {
				t.Fatalf("KeepErrors 未按配置携带: want=%v", keep)
			}
			d2 := RatioSampler{Rate: 0.5, KeepErrors: keep}.Decide(id)
			if d != d2 {
				t.Fatalf("决策不确定: %+v vs %+v", d, d2)
			}
		}
	}
}
