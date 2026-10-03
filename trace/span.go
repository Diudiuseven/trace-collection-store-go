// Package trace 提供进程内结构化追踪的数据模型与采集 API。
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// ID 唯一标识一条完整调用链路。
type ID [16]byte

// SpanID 唯一标识链路内的一个环节。
type SpanID [8]byte

// IsZero 报告链路 ID 是否为零值。
func (id ID) IsZero() bool { return id == ID{} }

// String 返回十六进制表示。
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// IsZero 报告环节 ID 是否为零值。
func (id SpanID) IsZero() bool { return id == SpanID{} }

// String 返回十六进制表示。
func (id SpanID) String() string { return hex.EncodeToString(id[:]) }

// newID 生成随机链路 ID。
func newID() ID {
	var id ID
	_, _ = rand.Read(id[:])
	return id
}

// newSpanID 生成随机环节 ID。
func newSpanID() SpanID {
	var id SpanID
	_, _ = rand.Read(id[:])
	return id
}

// Span 描述链路中的单个环节。
type Span struct {
	TraceID  ID
	SpanID   SpanID
	ParentID SpanID // 零值表示根环节
	Name     string
	Start    time.Time
	End      time.Time
	Err      bool
	// Sampled 为链路级采样决策的携带位：true 表示该链路已被采样命中，
	// false 表示仅在异常保留策略下暂存，是否最终保留由存储层裁定。
	Sampled bool
}

// Duration 返回环节耗时。
func (s Span) Duration() time.Duration { return s.End.Sub(s.Start) }

// spanContext 是随 context 传播的链路状态，采集决策在其中贯穿整条链路。
type spanContext struct {
	traceID  ID
	spanID   SpanID
	decision Decision
}

type ctxKey struct{}

// fromContext 取出当前 ctx 上的环节上下文。
func fromContext(ctx context.Context) (spanContext, bool) {
	sc, ok := ctx.Value(ctxKey{}).(spanContext)
	return sc, ok
}

// withContext 把环节上下文写回 ctx。
func withContext(ctx context.Context, sc spanContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, sc)
}
