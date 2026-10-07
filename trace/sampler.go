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
// 采样基于链路 ID 的散列，同一链路 ID 决策结果确定；
// 链路 ID 随机且彼此独立，因此不同链路的决策相互独立，
// 大量链路的实际命中率长期贴近 Rate。
type RatioSampler struct {
	// Rate 取值范围 [0,1]，超出范围（含负数、>1、NaN）会被收敛到边界：
	// 0 表示一条不采，1 表示全部采集。
	Rate float64
	// KeepErrors 为 true 时，未命中采样率的链路仍暂存其环节，
	// 链路内任一环节上报错误即整条保留，否则链路结束时丢弃。
	// 该开关与 Rate 相互独立：Rate 再低，异常链路也会被完整保留；
	// 为 false 时异常链路不享有任何额外保留。
	KeepErrors bool
}

// hashBuckets 是散列空间的分片数，采样率按该粒度生效（万分位）。
const hashBuckets = 10000

// clampRate 把采样率收敛到 [0,1]；NaN 无法与任何数值比较，按 0 处理，
// 保证任何输入都不会产生未定义行为。
func clampRate(rate float64) float64 {
	if rate != rate { // NaN：NaN 不等于自身
		return 0
	}
	if rate < 0 {
		return 0
	}
	if rate > 1 {
		return 1
	}
	return rate
}

// Decide 实现 Sampler。
//
// 决策只依赖链路 ID 的散列值与采样率：同一 ID 反复计算结果恒定，
// 进程重启后结论不变，也不随调用顺序或并发规模漂移。
// 随机生成的链路 ID 在散列空间中近似均匀，因此大量不同链路的
// 实际命中比例长期贴近配置采样率，各链路之间的决策相互独立。
func (r RatioSampler) Decide(id ID) Decision {
	// 全部使用整数比较，避免浮点运算在边界处（如 0.5*10000）的舍入误差：
	// rate == 1 时阈值等于散列空间大小，任何散列值都小于它；
	// rate == 0（含 NaN/负值收敛）时阈值为 0，任何散列值都不小于 0。
	threshold := int(clampRate(r.Rate) * hashBuckets)
	bucket := int(hashID(id) % hashBuckets)
	return Decision{Sampled: bucket < threshold, KeepErrors: r.KeepErrors}
}

// hashID 返回链路 ID 的 FNV-1a 64 位散列。纯函数，
// 同一链路 ID 在任意进程、任意时刻都得到相同结果。
func hashID(id ID) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(id[:])
	return h.Sum64()
}
