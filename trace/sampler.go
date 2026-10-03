package trace

import "hash/fnv"

// Sampler 在链路开始时一次性决定整条链路的采样策略。
type Sampler interface {
	// Decide 在链路根环节开始时调用，id 为新链路 ID。
	// 返回的 Decision 贯穿整条链路，不得中途改变。
	Decide(id ID) Decision
}

// Decision 是一条链路的采样决策。
type Decision struct {
	// Sampled 表示按采样率命中。
	Sampled bool
	// KeepErrors 为 true 时，即使未命中采样率，环节仍会被暂存，
	// 链路内任一环节上报错误即整条保留，否则链路结束时丢弃。
	// 该规则在链路开始时确定，同样贯穿整条链路。
	KeepErrors bool
}

// record 报告环节是否需要被记录（上报给 Sink）。
// 未命中采样但保留异常链路时也需要记录，以便后续提升。
func (d Decision) record() bool { return d.Sampled || d.KeepErrors }

// RatioSampler 按固定比率采样，可选择始终保留异常链路。
// 采样基于链路 ID 的散列，同一链路 ID 决策结果确定。
type RatioSampler struct {
	Rate       float64 // [0,1]，超出范围会被收敛
	KeepErrors bool    // 含错误环节的链路始终保留
}

// Decide 实现 Sampler。
func (r RatioSampler) Decide(id ID) Decision {
	rate := r.Rate
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	h := fnv.New64a()
	_, _ = h.Write(id[:])
	sampled := float64(h.Sum64()%10000) < rate*10000
	return Decision{Sampled: sampled, KeepErrors: r.KeepErrors}
}
