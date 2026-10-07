// Package store 提供追踪数据的并发安全存储、回收、持久化与查询。
package store

import (
	"sort"
	"sync"
	"time"

	"github.com/Diudiuseven/trace-collection-store-go/trace"
)

// IncompletePolicy 指定重启后对只采集到一半的链路的处理策略。
type IncompletePolicy int

const (
	// DropIncomplete 丢弃不完整链路。
	DropIncomplete IncompletePolicy = iota
	// KeepIncomplete 保留不完整链路并标记，不作为完整链路返回。
	KeepIncomplete
)

// Config 为存储配置。
type Config struct {
	Retention        time.Duration    // 数据保留时长，<=0 表示不过期
	MaxBytes         int64            // 空间上限（估算值），<=0 表示不限
	IncompletePolicy IncompletePolicy // 不完整链路处理策略
	WALPath          string           // 预写日志路径，空表示不持久化
}

// spanOverhead 是单个环节存储开销的保守估计（含 map/索引分摊）。
const spanOverhead = 256

// traceEntry 是内存中一条链路的装配状态。
type traceEntry struct {
	spans     map[trace.SpanID]trace.Span
	order     []trace.SpanID // 环节上报顺序，异常提升时按此顺序补落盘
	start     time.Time      // 链路内最早环节开始时间，迟到环节可使其前移
	complete  bool           // 根环节（ParentID 为零）已上报
	tentative bool           // 未命中采样、按异常保留策略暂存中
	hasErr    bool
	size      int64
}

// indexEntry 是按链路开始时间排序的索引项。
type indexEntry struct {
	start time.Time
	id    trace.ID
}

// Store 是并发安全的追踪存储。回收、采集、查询可并发进行，
// 内部以读写锁保证不会读到半截数据。
type Store struct {
	cfg    Config
	mu     sync.RWMutex
	traces map[trace.ID]*traceEntry
	index  []indexEntry // 按 (start, id) 升序
	total  int64        // 估算总字节数
	wal    *wal
	now    func() time.Time
}

// Open 打开（必要时从 WAL 恢复）一个存储。
func Open(cfg Config) (*Store, error) {
	s := &Store{
		cfg:    cfg,
		traces: make(map[trace.ID]*traceEntry),
		now:    time.Now,
	}
	if cfg.WALPath != "" {
		w, err := openWAL(cfg.WALPath)
		if err != nil {
			return nil, err
		}
		s.wal = w
		if err := w.replay(func(sp trace.Span) error {
			// WAL 中只持久化已保留链路（采样命中或异常提升）的环节，
			// 因此恢复时一律按保留链路装配。
			s.addLocked(sp)
			return nil
		}); err != nil {
			_ = w.close()
			return nil, err
		}
		s.applyIncompletePolicy()
	}
	return s, nil
}

// Report 实现 trace.Sink。乱序与迟到上报均可接受：
// 环节按 ID 归并到所属链路，装配在查询时进行。
//
// 持久化策略与采样决策配合，保证未命中且无异常的链路不长期占用任何存储：
//   - 采样命中（sp.Sampled）的环节先落盘再入内存，崩溃可恢复；
//   - 未命中的环节只在内存暂存，不写 WAL；链路正常完成即丢弃；
//     一旦出现错误环节，整条链路此前暂存的环节一次性补落盘，
//     之后与采样命中链路待遇完全一致。
func (s *Store) Report(sp trace.Span) {
	if sp.Sampled {
		// 先落盘再入内存，保证崩溃后不丢已确认的环节。
		if s.wal != nil {
			_ = s.wal.append(sp)
		}
		s.mu.Lock()
		s.addLocked(sp)
		s.evictLocked()
		s.mu.Unlock()
		return
	}

	// 未命中采样：只在内存中暂存，不落盘。
	// 若本环节触发异常提升，需要把整条链路（含此前暂存环节）补落盘。
	var flush []trace.Span
	var promoted bool
	s.mu.Lock()
	flush, promoted = s.addTentativeLocked(sp)
	s.evictLocked()
	s.mu.Unlock()
	if promoted && s.wal != nil {
		// 提升时一次性按链路内顺序补落盘，崩溃后恢复出的链路结构完整。
		for _, psp := range flush {
			_ = s.wal.append(psp)
		}
	}
}

// addLocked 把一个采样命中（或 WAL 恢复）的环节归并进所属链路。
// 这类环节所属链路一律视为保留链路。调用方需持有写锁（恢复流程在 Open
// 单线程阶段调用）。
func (s *Store) addLocked(sp trace.Span) {
	e, ok := s.traces[sp.TraceID]
	if !ok {
		e = &traceEntry{spans: make(map[trace.SpanID]trace.Span), start: sp.Start}
		s.traces[sp.TraceID] = e
		s.indexInsert(sp.TraceID, sp.Start)
	}
	// 采样决策全链路一致：收到带采样标记的环节即确定为保留链路。
	// 若它此前按未命中暂存（理论上不会发生），防御性地提升并清除暂存标记。
	e.tentative = false
	s.mergeLocked(e, sp)
}

// addTentativeLocked 归并一个未命中采样的暂存环节。
// 返回值 flush 为链路被异常提升时需要补落盘的全部环节（按上报顺序），
// promoted 报告是否发生了提升。调用方需持有写锁。
func (s *Store) addTentativeLocked(sp trace.Span) (flush []trace.Span, promoted bool) {
	e, ok := s.traces[sp.TraceID]
	if !ok {
		e = &traceEntry{
			spans:     make(map[trace.SpanID]trace.Span),
			start:     sp.Start,
			tentative: true,
		}
		s.traces[sp.TraceID] = e
		s.indexInsert(sp.TraceID, sp.Start)
	}
	if !e.tentative {
		// 链路此前已提升（出现过错误环节）：直接按保留链路归并，
		// 并由调用方将本环节落盘。
		s.mergeLocked(e, sp)
		return []trace.Span{sp}, true
	}
	if _, dup := e.spans[sp.SpanID]; dup {
		return nil, false // 幂等：重复上报不产生副作用
	}
	e.order = append(e.order, sp.SpanID)
	e.spans[sp.SpanID] = sp
	e.size += spanOverhead + int64(len(sp.Name))
	s.total += spanOverhead + int64(len(sp.Name))
	if sp.ParentID.IsZero() {
		e.complete = true
	}
	if sp.Err {
		e.hasErr = true
		// 异常保留：暂存链路一旦出现错误环节即整条提升为保留，
		// 此前暂存的全部环节需要补落盘。
		e.tentative = false
		flush = make([]trace.Span, 0, len(e.order))
		for _, sid := range e.order {
			flush = append(flush, e.spans[sid])
		}
		return flush, true
	}
	// 链路完整且无异常：暂存缓冲即刻丢弃，不占用保留空间，也不落盘。
	if e.complete {
		s.removeLocked(sp.TraceID)
		return nil, false
	}
	if sp.Start.Before(e.start) {
		s.indexRemove(sp.TraceID, e.start)
		e.start = sp.Start
		s.indexInsert(sp.TraceID, sp.Start)
	}
	return nil, false
}

// mergeLocked 把环节幂等归并进已确定保留的链路，并维护完整/异常标记。
// 调用方需持有写锁。
func (s *Store) mergeLocked(e *traceEntry, sp trace.Span) {
	if _, dup := e.spans[sp.SpanID]; dup {
		return // 幂等：重复上报（如 WAL 重放）不产生副作用
	}
	e.order = append(e.order, sp.SpanID)
	e.spans[sp.SpanID] = sp
	e.size += spanOverhead + int64(len(sp.Name))
	s.total += spanOverhead + int64(len(sp.Name))
	if sp.ParentID.IsZero() {
		e.complete = true
	}
	if sp.Err {
		e.hasErr = true
	}
	if sp.Start.Before(e.start) {
		s.indexRemove(sp.TraceID, e.start)
		e.start = sp.Start
		s.indexInsert(sp.TraceID, sp.Start)
	}
}

// kept 报告链路是否应出现在查询结果中（不含完整性判断）。
func (e *traceEntry) kept() bool { return !e.tentative }

// applyIncompletePolicy 在恢复后按既定策略处理半截链路。
func (s *Store) applyIncompletePolicy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.IncompletePolicy == KeepIncomplete {
		return // 保留并标记 Complete=false，不作为完整链路返回
	}
	for id, e := range s.traces {
		if !e.complete {
			s.removeLocked(id)
		}
	}
}

// evictLocked 执行保留策略与空间上限回收，调用方需持有写锁。
func (s *Store) evictLocked() {
	if s.cfg.Retention > 0 {
		cutoff := s.now().Add(-s.cfg.Retention)
		// 索引按开始时间升序，从头扫描过期项。
		for len(s.index) > 0 && s.index[0].start.Before(cutoff) {
			s.removeLocked(s.index[0].id)
		}
	}
	if s.cfg.MaxBytes > 0 {
		for s.total > s.cfg.MaxBytes && len(s.index) > 0 {
			s.removeLocked(s.index[0].id) // 最旧的先回收
		}
	}
}

// removeLocked 删除一条链路及其索引，调用方需持有写锁。
func (s *Store) removeLocked(id trace.ID) {
	e, ok := s.traces[id]
	if !ok {
		return
	}
	s.total -= e.size
	delete(s.traces, id)
	s.indexRemove(id, e.start)
}

// indexInsert 维护按 (start, id) 升序的索引，调用方需持有写锁。
func (s *Store) indexInsert(id trace.ID, start time.Time) {
	i := sort.Search(len(s.index), func(i int) bool {
		e := s.index[i]
		if !e.start.Equal(start) {
			return e.start.After(start)
		}
		return string(e.id[:]) >= string(id[:])
	})
	s.index = append(s.index, indexEntry{})
	copy(s.index[i+1:], s.index[i:])
	s.index[i] = indexEntry{start: start, id: id}
}

// indexRemove 从索引移除一项，调用方需持有写锁。
func (s *Store) indexRemove(id trace.ID, start time.Time) {
	i := sort.Search(len(s.index), func(i int) bool {
		e := s.index[i]
		if !e.start.Equal(start) {
			return e.start.After(start)
		}
		return string(e.id[:]) >= string(id[:])
	})
	if i < len(s.index) && s.index[i].id == id && s.index[i].start.Equal(start) {
		s.index = append(s.index[:i], s.index[i+1:]...)
	}
}

// Close 关闭存储并释放资源。关闭后不得再使用。
func (s *Store) Close() error {
	if s.wal != nil {
		return s.wal.close()
	}
	return nil
}
