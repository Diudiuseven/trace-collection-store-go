package store

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Diudiuseven/trace-collection-store-go/trace"
)

// defaultPageSize 是未指定 Limit 时的单页大小。
const defaultPageSize = 64

// maxPageSize 限制单页大小，约束单次查询的内存占用。
const maxPageSize = 1024

// Query 描述一次查询条件。
type Query struct {
	From        time.Time     // 链路起始时间下界（含）
	To          time.Time     // 链路起始时间上界（含），零值表示不限
	Name        string        // 按环节名称过滤（链路内含同名环节即匹配），空表示不限
	MinDuration time.Duration // 链路内任一环节耗时不低于该值即匹配
	// IncludeIncomplete 为 true 时才返回只采集到一半的链路
	// （TraceView.Complete=false）；默认只返回完整链路。
	IncludeIncomplete bool
	Limit             int    // 单页大小，<=0 使用默认值，超过上限被收敛
	Cursor            string // 上一页返回的游标，空表示首页
}

// TraceView 是一条链路的查询结果视图。
type TraceView struct {
	TraceID  trace.ID
	Spans    []trace.Span // 按开始时间排序，可还原各环节在链路中的时间位置
	Start    time.Time
	Duration time.Duration // 根环节耗时；根缺失时为链路内最大环节跨度
	Complete bool
	HasError bool
}

// Result 为查询结果页。
type Result struct {
	Traces     []TraceView
	NextCursor string // 空表示没有更多数据
}

// Query 按条件查询。ctx 用于超时控制；命中数量达到单页上限即返回，
// 内存占用与数据总量解耦，只随单页大小增长。
func (s *Store) Query(ctx context.Context, q Query) (Result, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	cursor, err := parseCursor(q.Cursor)
	if err != nil {
		return Result{}, err
	}

	res := Result{Traces: make([]TraceView, 0, limit)}
	s.mu.RLock()
	defer s.mu.RUnlock()

	i := s.seekLocked(cursor)
	scanned := 0
	for ; i < len(s.index); i++ {
		// 周期性检查超时，避免长时间占锁。
		if scanned%128 == 0 {
			if err := ctx.Err(); err != nil {
				return Result{}, err
			}
		}
		scanned++
		ie := s.index[i]
		if !q.From.IsZero() && ie.start.Before(q.From) {
			continue
		}
		if !q.To.IsZero() && ie.start.After(q.To) {
			break // 索引有序，之后的都越界
		}
		e := s.traces[ie.id]
		if e == nil || !e.kept() {
			continue
		}
		if !e.complete && !q.IncludeIncomplete {
			continue
		}
		if !matchSpans(e, q) {
			continue
		}
		res.Traces = append(res.Traces, buildView(ie.id, e))
		if len(res.Traces) >= limit {
			res.NextCursor = makeCursor(ie)
			return res, nil
		}
	}
	return res, nil
}

// seekLocked 找到游标之后的第一个索引位置。
func (s *Store) seekLocked(c *indexEntry) int {
	if c == nil {
		return 0
	}
	i := sort.Search(len(s.index), func(i int) bool {
		e := s.index[i]
		if !e.start.Equal(c.start) {
			return e.start.After(c.start)
		}
		return string(e.id[:]) > string(c.id[:])
	})
	return i
}

// matchSpans 检查环节级过滤条件。
func matchSpans(e *traceEntry, q Query) bool {
	if q.Name == "" && q.MinDuration <= 0 {
		return true
	}
	nameOK := q.Name == ""
	durOK := q.MinDuration <= 0
	for _, sp := range e.spans {
		if !nameOK && sp.Name == q.Name {
			nameOK = true
		}
		if !durOK && sp.Duration() >= q.MinDuration {
			durOK = true
		}
		if nameOK && durOK {
			return true
		}
	}
	return false
}

// buildView 装配链路视图：环节按开始时间排序，计算链路级耗时。
func buildView(id trace.ID, e *traceEntry) TraceView {
	spans := make([]trace.Span, 0, len(e.spans))
	for _, sp := range e.spans {
		spans = append(spans, sp)
	}
	sort.Slice(spans, func(i, j int) bool {
		if !spans[i].Start.Equal(spans[j].Start) {
			return spans[i].Start.Before(spans[j].Start)
		}
		return string(spans[i].SpanID[:]) < string(spans[j].SpanID[:])
	})
	v := TraceView{
		TraceID:  id,
		Spans:    spans,
		Start:    e.start,
		Complete: e.complete,
		HasError: e.hasErr,
	}
	for _, sp := range spans {
		if sp.ParentID.IsZero() {
			v.Duration = sp.Duration()
			break
		}
	}
	if v.Duration == 0 && len(spans) > 0 {
		end := spans[0].End
		for _, sp := range spans[1:] {
			if sp.End.After(end) {
				end = sp.End
			}
		}
		v.Duration = end.Sub(spans[0].Start)
	}
	return v
}

// makeCursor 生成分页游标。
func makeCursor(ie indexEntry) string {
	raw := fmt.Sprintf("%d|%s", ie.start.UnixNano(), ie.id.String())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// parseCursor 解析分页游标；空游标返回 nil。
func parseCursor(s string) (*indexEntry, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("store: 非法游标: %w", err)
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("store: 非法游标格式")
	}
	nanos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("store: 非法游标时间: %w", err)
	}
	var id trace.ID
	b, err := hexDecode(parts[1])
	if err != nil || len(b) != len(id) {
		return nil, fmt.Errorf("store: 非法游标链路 ID")
	}
	copy(id[:], b)
	return &indexEntry{start: time.Unix(0, nanos), id: id}, nil
}
