package trace

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"
)

// traceRecord 是内存中一条链路的内部状态。
type traceRecord struct {
	trace      *Trace
	sealed     bool
	hasError   bool
	lastUpdate time.Time
}

// orderEntry 是有序索引中的一项，冗余存储 start 以避免查找时记录已更新。
type orderEntry struct {
	start time.Time
	id    TraceID
}

// Store 管理链路的内存存储，所有方法并发安全。
type Store struct {
	mu     sync.RWMutex
	cfg    Config
	traces map[TraceID]*traceRecord
	order  []orderEntry // 按开始时间有序，便于分页与回收
}

// NewStore 创建存储。
func NewStore(cfg Config) *Store {
	return &Store{
		cfg:    cfg,
		traces: make(map[TraceID]*traceRecord),
	}
}

// AddSpan 上报一个环节。环节可乱序、迟到，不会被丢弃。
func (s *Store) AddSpan(span *Span) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.traces[span.TraceID]
	if !ok {
		rec = &traceRecord{trace: &Trace{TraceID: span.TraceID}}
		s.traces[span.TraceID] = rec
	}

	prevStart := rec.trace.Start
	rec.trace.Spans = append(rec.trace.Spans, span)
	if rec.trace.Start.IsZero() || span.Start.Before(rec.trace.Start) {
		rec.trace.Start = span.Start
	}
	if span.End.After(rec.trace.End) {
		rec.trace.End = span.End
	}
	if span.Status == StatusError {
		rec.hasError = true
	}
	rec.lastUpdate = time.Now()

	// 维护有序索引。开始时间变化时，移除旧位置并插入新位置。
	if !prevStart.IsZero() && !prevStart.Equal(rec.trace.Start) {
		s.removeOrder(span.TraceID, prevStart)
	}
	if prevStart.IsZero() || !prevStart.Equal(rec.trace.Start) {
		s.addOrder(span.TraceID, rec.trace.Start)
	}
}

// Seal 封存链路，表示采集完成。迟到环节仍可追加。
func (s *Store) Seal(traceID TraceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.traces[traceID]; ok {
		rec.sealed = true
		rec.trace.Sealed = true
		rec.lastUpdate = time.Now()
	}
}

// Get 返回单条链路。
func (s *Store) Get(traceID TraceID) (*Trace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.traces[traceID]
	if !ok {
		return nil, false
	}
	return rec.trace, true
}

// Drop 立即删除一条链路（用于采样未命中的链路）。
func (s *Store) Drop(traceID TraceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.traces[traceID]; ok {
		s.removeOrder(traceID, rec.trace.Start)
		delete(s.traces, traceID)
	}
}

// Query 按条件查询链路，支持分页与超时。单次返回的链路数受 Limit 约束，
// 内存占用不随数据总量无界增长。
func (s *Store) Query(ctx context.Context, q Query) (*Page, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	var cur cursor
	if q.Cursor != "" {
		c, err := decodeCursor(q.Cursor)
		if err != nil {
			return nil, err
		}
		cur = c
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	startIdx := 0
	if q.Cursor != "" {
		startIdx = sort.Search(len(s.order), func(i int) bool {
			return !orderLessEntry(s.order[i], cur.start, cur.id)
		})
	}

	matched := make([]*Trace, 0, limit+1)
	for i := startIdx; i < len(s.order); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e := s.order[i]
		rec := s.traces[e.id]
		if !matchTrace(rec.trace, q) {
			continue
		}
		matched = append(matched, rec.trace)
		if len(matched) > limit {
			break
		}
	}

	page := &Page{Traces: matched}
	if len(matched) > limit {
		extra := matched[limit]
		page.Traces = matched[:limit]
		page.NextCursor = encodeCursor(extra.Start, extra.TraceID)
	}
	return page, nil
}

// reap 按保留策略回收过期链路，返回被删除的链路 ID。
func (s *Store) reap(now time.Time) []TraceID {
	s.mu.Lock()
	defer s.mu.Unlock()

	reapSet := make(map[TraceID]bool)

	if s.cfg.RetentionPeriod > 0 {
		for _, e := range s.order {
			rec := s.traces[e.id]
			if rec.sealed && now.Sub(e.start) > s.cfg.RetentionPeriod {
				reapSet[e.id] = true
			}
		}
	}

	if s.cfg.MaxTraces > 0 && len(s.traces) > s.cfg.MaxTraces {
		for _, e := range s.order {
			if len(s.traces)-len(reapSet) <= s.cfg.MaxTraces {
				break
			}
			rec := s.traces[e.id]
			if rec.sealed {
				reapSet[e.id] = true
			}
		}
		for _, e := range s.order {
			if len(s.traces)-len(reapSet) <= s.cfg.MaxTraces {
				break
			}
			reapSet[e.id] = true
		}
	}

	if s.cfg.AbandonTimeout > 0 {
		for _, e := range s.order {
			rec := s.traces[e.id]
			if !rec.sealed && now.Sub(rec.lastUpdate) > s.cfg.AbandonTimeout {
				reapSet[e.id] = true
			}
		}
	}

	if len(reapSet) == 0 {
		return nil
	}

	removed := make([]TraceID, 0, len(reapSet))
	for id := range reapSet {
		rec := s.traces[id]
		s.removeOrder(id, rec.trace.Start)
		delete(s.traces, id)
		removed = append(removed, id)
	}
	return removed
}

// SnapshotSealed 返回所有已封存链路的深拷贝，用于持久化压缩。
func (s *Store) SnapshotSealed() []*Trace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Trace, 0, len(s.traces))
	for _, e := range s.order {
		rec := s.traces[e.id]
		if !rec.sealed {
			continue
		}
		cp := &Trace{
			TraceID: rec.trace.TraceID,
			Start:   rec.trace.Start,
			End:     rec.trace.End,
			Sealed:  true,
		}
		for _, sp := range rec.trace.Spans {
			spCopy := *sp
			if sp.Tags != nil {
				spCopy.Tags = make(map[string]string, len(sp.Tags))
				for k, v := range sp.Tags {
					spCopy.Tags[k] = v
				}
			}
			cp.Spans = append(cp.Spans, &spCopy)
		}
		out = append(out, cp)
	}
	return out
}

// ApplyRecovery 重放持久化数据，恢复链路状态。
func (s *Store) ApplyRecovery(spans []*Span, seals map[TraceID]bool, dels map[TraceID]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, span := range spans {
		if dels[span.TraceID] {
			continue
		}
		rec, ok := s.traces[span.TraceID]
		if !ok {
			rec = &traceRecord{trace: &Trace{TraceID: span.TraceID}}
			s.traces[span.TraceID] = rec
		}
		prevStart := rec.trace.Start
		rec.trace.Spans = append(rec.trace.Spans, span)
		if rec.trace.Start.IsZero() || span.Start.Before(rec.trace.Start) {
			rec.trace.Start = span.Start
		}
		if span.End.After(rec.trace.End) {
			rec.trace.End = span.End
		}
		if span.Status == StatusError {
			rec.hasError = true
		}
		rec.lastUpdate = time.Now()
		if !prevStart.IsZero() && !prevStart.Equal(rec.trace.Start) {
			s.removeOrder(span.TraceID, prevStart)
		}
		if prevStart.IsZero() || !prevStart.Equal(rec.trace.Start) {
			s.addOrder(span.TraceID, rec.trace.Start)
		}
	}

	for id := range seals {
		if dels[id] {
			continue
		}
		if rec, ok := s.traces[id]; ok {
			rec.sealed = true
			rec.trace.Sealed = true
		}
	}

	for id := range dels {
		if rec, ok := s.traces[id]; ok {
			s.removeOrder(id, rec.trace.Start)
			delete(s.traces, id)
		}
	}
}

// ---- 内部工具 ----

func matchTrace(t *Trace, q Query) bool {
	if !q.StartTime.IsZero() && t.Start.Before(q.StartTime) {
		return false
	}
	if !q.EndTime.IsZero() && t.Start.After(q.EndTime) {
		return false
	}
	if q.SpanName != "" {
		found := false
		for _, sp := range t.Spans {
			if sp.Name == q.SpanName {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if q.MinDuration > 0 || q.MaxDuration > 0 {
		matched := false
		for _, sp := range t.Spans {
			d := sp.Duration()
			if q.MinDuration > 0 && d < q.MinDuration {
				continue
			}
			if q.MaxDuration > 0 && d > q.MaxDuration {
				continue
			}
			matched = true
			break
		}
		if !matched {
			return false
		}
	}
	return true
}

// orderLessEntry 比较 (start, id) 与 (refStart, refID) 的先后。
func orderLessEntry(e orderEntry, refStart time.Time, refID TraceID) bool {
	if !e.start.Equal(refStart) {
		return e.start.Before(refStart)
	}
	return compareTraceID(e.id, refID) < 0
}

func compareTraceID(a, b TraceID) int {
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func (s *Store) addOrder(id TraceID, start time.Time) {
	pos := sort.Search(len(s.order), func(i int) bool {
		return !orderLessEntry(s.order[i], start, id)
	})
	s.order = append(s.order, orderEntry{})
	copy(s.order[pos+1:], s.order[pos:])
	s.order[pos] = orderEntry{start: start, id: id}
}

func (s *Store) removeOrder(id TraceID, start time.Time) {
	pos := sort.Search(len(s.order), func(i int) bool {
		return !orderLessEntry(s.order[i], start, id)
	})
	if pos < len(s.order) && s.order[pos].id == id {
		s.order = append(s.order[:pos], s.order[pos+1:]...)
	}
}

// ---- 游标 ----

type cursor struct {
	start time.Time
	id    TraceID
}

var errInvalidCursor = errors.New("invalid cursor")

func encodeCursor(start time.Time, id TraceID) string {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[:8], uint64(start.UnixNano()))
	copy(buf[8:], id[:])
	return base64.StdEncoding.EncodeToString(buf)
}

func decodeCursor(s string) (cursor, error) {
	buf, err := base64.StdEncoding.DecodeString(s)
	if err != nil || len(buf) != 24 {
		return cursor{}, errInvalidCursor
	}
	var c cursor
	c.start = time.Unix(0, int64(binary.BigEndian.Uint64(buf[:8])))
	copy(c.id[:], buf[8:])
	return c, nil
}

// SnapshotAll 返回所有链路的深拷贝（含未封存），用于持久化压缩。
func (s *Store) SnapshotAll() []*Trace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Trace, 0, len(s.traces))
	for _, e := range s.order {
		rec := s.traces[e.id]
		cp := &Trace{
			TraceID: rec.trace.TraceID,
			Start:   rec.trace.Start,
			End:     rec.trace.End,
			Sealed:  rec.sealed,
		}
		for _, sp := range rec.trace.Spans {
			spCopy := *sp
			if sp.Tags != nil {
				spCopy.Tags = make(map[string]string, len(sp.Tags))
				for k, v := range sp.Tags {
					spCopy.Tags[k] = v
				}
			}
			cp.Spans = append(cp.Spans, &spCopy)
		}
		out = append(out, cp)
	}
	return out
}
