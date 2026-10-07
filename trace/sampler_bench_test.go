package trace

import (
	"context"
	"testing"
)

// BenchmarkDecide 衡量单次采样决策开销，覆盖典型小数采样率。
// 决策只做 16 字节内联散列 + 一次定点比较，开销应在纳秒级且不随采样率漂移。
func BenchmarkDecide(b *testing.B) {
	ids := make([]ID, 1024)
	for i := range ids {
		ids[i] = newID()
	}
	for _, rate := range []float64{0.0, 0.1, 0.5, 0.9, 1.0} {
		b.Run(rateName(rate), func(b *testing.B) {
			s := RatioSampler{Rate: rate}
			b.ResetTimer()
			var x bool
			for i := 0; i < b.N; i++ {
				x = s.Decide(ids[i&1023]).Sampled
			}
			_ = x
		})
	}
}

func rateName(r float64) string {
	switch r {
	case 0:
		return "rate-0"
	case 0.1:
		return "rate-0.1"
	case 0.5:
		return "rate-0.5"
	case 0.9:
		return "rate-0.9"
	default:
		return "rate-1"
	}
}

// BenchmarkStartUnsampled 衡量未命中链路的 Start/End 热路径开销。
func BenchmarkStartUnsampled(b *testing.B) {
	tr := NewTracer(&memSink{}, RatioSampler{Rate: 0})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, end := tr.Start(context.Background(), "span")
		end(false)
	}
}
