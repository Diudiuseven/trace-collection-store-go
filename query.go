package trace

import "time"

// Query 描述查询条件。零值表示不限制。
type Query struct {
	// StartTime、EndTime 按链路开始时间过滤。
	StartTime time.Time
	EndTime   time.Time
	// SpanName 匹配包含该名称环节的链路。
	SpanName string
	// MinDuration、MaxDuration 按环节耗时过滤（任一环节满足即可）。
	MinDuration time.Duration
	MaxDuration time.Duration
	// Limit 单页返回的最大链路数，0 使用默认值。
	Limit int
	// Cursor 分页游标，来自上一页的 NextCursor。
	Cursor string
}

// Page 是一页查询结果。
type Page struct {
	Traces     []*Trace
	NextCursor string
}
