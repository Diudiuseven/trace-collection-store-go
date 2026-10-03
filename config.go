package trace

import "time"

// IncompletePolicy 决定崩溃恢复时如何处理只采集到一半的链路。
type IncompletePolicy uint8

const (
	// IncompleteDiscard 丢弃不完整链路，不作为完整链路返回。
	IncompleteDiscard IncompletePolicy = iota
	// IncompleteKeep 保留不完整链路，但以 Incomplete 标记，绝不作为完整链路返回。
	IncompleteKeep
)

// Config 配置采集、保留与恢复行为。
type Config struct {
	// SampleRate 采样率，取值 [0,1]，1 表示全采。采样决策在链路开始时一次性确定。
	SampleRate float64
	// KeepError 是否保留包含异常环节的链路（即使未被采样命中）。
	KeepError bool

	// RetentionPeriod 链路保留时长，超过后被回收。0 表示不按时长回收。
	RetentionPeriod time.Duration
	// MaxTraces 内存中最多保留的链路数，超出后回收最老的链路。0 表示不限。
	MaxTraces int
	// ReapInterval 回收周期。
	ReapInterval time.Duration

	// DataDir 持久化目录，为空表示仅内存。
	DataDir string
	// WALSync 是否在链路封存时强制刷盘，保证已封存链路重启后可查。
	WALSync bool
	// CompactThreshold WAL 字节数超过该阈值后触发快照压缩。
	CompactThreshold int64

	// IncompletePolicy 崩溃恢复时对不完整链路的处理策略。
	IncompletePolicy IncompletePolicy
	// AbandonTimeout 运行时一条链路超过该时长仍未封存，则视为废弃（不完整）。
	AbandonTimeout time.Duration
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		SampleRate:       1.0,
		KeepError:        true,
		RetentionPeriod:  time.Hour,
		MaxTraces:        10000,
		ReapInterval:     time.Minute,
		WALSync:          true,
		CompactThreshold: 16 << 20,
		IncompletePolicy: IncompleteDiscard,
		AbandonTimeout:   5 * time.Minute,
	}
}

func (c *Config) normalize() {
	if c.SampleRate < 0 {
		c.SampleRate = 0
	}
	if c.SampleRate > 1 {
		c.SampleRate = 1
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = time.Minute
	}
	if c.CompactThreshold <= 0 {
		c.CompactThreshold = 16 << 20
	}
	if c.AbandonTimeout <= 0 {
		c.AbandonTimeout = 5 * time.Minute
	}
}
