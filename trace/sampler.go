package trace

// Sampler 在链路开始时一次性决定整条链路的采样策略。
type Sampler interface {
	// Decide 在链路根环节开始时调用，id 为新链路 ID。
	// 对同一 id 必须始终返回相同结果（确定性）；不同 id 之间的决策
	// 应相互独立，使大量链路的实际命中率在统计上贴近配置比率。
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
// 采样基于链路 ID 的 FNV-1a 散列：同一链路 ID 决策恒定，
// 不同链路 ID 散列值近似独立均匀分布，因此：
//   - Rate=0.1 时，长时间运行下约一成链路命中，0.9 约九成；
//   - 同一进程并发生成的链路各自独立决策，互不连坐；
//   - 决策只依赖 ID，不含随机源、时间或调用顺序，可重复、可恢复。
type RatioSampler struct {
	Rate       float64 // [0,1]，越界（含 NaN）按边界收敛
	KeepErrors bool    // 含错误环节的链路始终保留；与 Rate 相互独立
}

// Decide 实现 Sampler。
func (r RatioSampler) Decide(id ID) Decision {
	rate := r.Rate
	// 边界收敛：NaN 与负值归 0，大于 1 归 1，杜绝未定义行为。
	if rate != rate || rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	// 以全 64 位散列空间做阈值比较，而非先取模再比较：
	// 阈值用定点整数表示（rate*2^64），边界精确且无取模偏差。
	// 边界短路：rate 为 0 / 1 时完全确定，结果不依赖散列。
	sampled := rate > 0 && (rate == 1 || hashID(id) < uint64(rate*(1<<64)))
	return Decision{Sampled: sampled, KeepErrors: r.KeepErrors}
}

// fnvOffset64 / fnvPrime64 是 FNV-1a 64 位散列常量
// （与 hash/fnv 标准库一致）。
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// hashID 计算 ID 的 FNV-1a 64 位散列。直接在数组上内联迭代，
// 不分配堆内存、不使用共享哈希状态，并发与开销都稳定。
func hashID(id ID) uint64 {
	h := uint64(fnvOffset64)
	for _, b := range id {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}
