package trace

import (
	"encoding/hex"
	"time"
)

// TraceID 唯一标识一条执行链路。
type TraceID [16]byte

// SpanID 唯一标识链路内的一个环节。
type SpanID [8]byte

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string { return hex.EncodeToString(s[:]) }

func (t TraceID) IsZero() bool { return t == TraceID{} }
func (s SpanID) IsZero() bool { return s == SpanID{} }

// Status 描述环节执行结果。
type Status uint8

const (
	StatusOK Status = iota
	StatusError
)

// Span 记录单个环节的耗时与归属关系。
type Span struct {
	TraceID  TraceID
	SpanID   SpanID
	ParentID SpanID
	Name     string
	Start    time.Time
	End      time.Time
	Status   Status
	Tags     map[string]string
}

// Duration 返回环节耗时。
func (s *Span) Duration() time.Duration { return s.End.Sub(s.Start) }

// Trace 是同一条链路下全部环节的集合。
type Trace struct {
	TraceID TraceID
	Spans   []*Span
	Start   time.Time
	End     time.Time
	Sealed  bool
}

// RootSpan 返回根环节（无父环节）。
func (t *Trace) RootSpan() *Span {
	for _, s := range t.Spans {
		if s.ParentID.IsZero() {
			return s
		}
	}
	return nil
}

// HasError 报告链路内是否存在异常环节。
func (t *Trace) HasError() bool {
	for _, s := range t.Spans {
		if s.Status == StatusError {
			return true
		}
	}
	return false
}
