package trace

import "math"

// Sampler 在链路开始时决定是否采集整条链路。
type Sampler interface {
	// Sample 针对给定 TraceID 返回采样决策。同一 TraceID 必须返回一致结果。
	Sample(id TraceID) bool
}

// RateSampler 按比例采样，决策基于 TraceID 哈希以保证同一链路一致。
type RateSampler struct {
	threshold uint64
}

// NewRateSampler 创建采样器，rate 取值 [0,1]。
func NewRateSampler(rate float64) *RateSampler {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return &RateSampler{threshold: uint64(rate * float64(math.MaxUint64))}
}

func (s *RateSampler) Sample(id TraceID) bool {
	// 取前 8 字节作为确定性哈希，与阈值比较。
	v := uint64(id[0])<<56 | uint64(id[1])<<48 | uint64(id[2])<<40 | uint64(id[3])<<32 |
		uint64(id[4])<<24 | uint64(id[5])<<16 | uint64(id[6])<<8 | uint64(id[7])
	return v <= s.threshold
}

// neverSampler 永不采样。
type neverSampler struct{}

func (neverSampler) Sample(TraceID) bool { return false }

// alwaysSampler 全采。
type alwaysSampler struct{}

func (alwaysSampler) Sample(TraceID) bool { return true }
