package trace

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	walFileName  = "wal.log"
	snapFileName = "snapshot.log"
	snapTmpName  = "snapshot.tmp"
)

// Persister 负责 WAL 追加与快照，支持崩溃恢复。
type Persister struct {
	mu       sync.Mutex
	cfg      Config
	wal      *os.File
	buf      *bufio.Writer
	walPath  string
	snapPath string
	walSize  int64
}

// NewPersister 创建持久化器。DataDir 为空时返回 nil。
func NewPersister(cfg Config) (*Persister, error) {
	if cfg.DataDir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(cfg.DataDir, walFileName)
	snapPath := filepath.Join(cfg.DataDir, snapFileName)

	wal, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := wal.Stat()
	if err != nil {
		wal.Close()
		return nil, err
	}

	return &Persister{
		cfg:      cfg,
		wal:      wal,
		buf:      bufio.NewWriter(wal),
		walPath:  walPath,
		snapPath: snapPath,
		walSize:  info.Size(),
	}, nil
}

// AppendSpan 追加一条环节记录。
func (p *Persister) AppendSpan(span *Span) error {
	rec := spanRecord{
		Type:   "s",
		Trace:  span.TraceID.String(),
		Span:   span.SpanID.String(),
		Parent: span.ParentID.String(),
		Name:   span.Name,
		Start:  span.Start.Format(time.RFC3339Nano),
		End:    span.End.Format(time.RFC3339Nano),
		Status: span.Status,
		Tags:   span.Tags,
	}
	return p.append(rec)
}

// AppendSeal 追加封存记录。
func (p *Persister) AppendSeal(traceID TraceID) error {
	return p.append(spanRecord{Type: "seal", Trace: traceID.String()})
}

// AppendDelete 追加删除记录。
func (p *Persister) AppendDelete(traceID TraceID) error {
	return p.append(spanRecord{Type: "del", Trace: traceID.String()})
}

func (p *Persister) append(rec spanRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	p.mu.Lock()
	defer p.mu.Unlock()
	n, err := p.buf.Write(data)
	if err != nil {
		return err
	}
	p.walSize += int64(n)
	if rec.Type == "seal" && p.cfg.WALSync {
		if err := p.buf.Flush(); err != nil {
			return err
		}
		return p.wal.Sync()
	}
	return nil
}

// Flush 强制刷盘。
func (p *Persister) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.buf.Flush(); err != nil {
		return err
	}
	return p.wal.Sync()
}

// Close 关闭文件。
func (p *Persister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.buf.Flush(); err != nil {
		p.wal.Close()
		return err
	}
	if err := p.wal.Sync(); err != nil {
		p.wal.Close()
		return err
	}
	return p.wal.Close()
}

// WALSize 返回当前 WAL 字节数。
func (p *Persister) WALSize() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.walSize
}

// Recover 重放 WAL 与快照，恢复存储状态。
func (p *Persister) Recover(store *Store) error {
	spans, seals, dels, err := p.loadSnapshot()
	if err != nil {
		return err
	}
	walSpans, walSeals, walDels, err := p.replayWAL()
	if err != nil {
		return err
	}
	spans = append(spans, walSpans...)
	for id := range walSeals {
		seals[id] = true
	}
	for id := range walDels {
		dels[id] = true
	}

	// 对不完整链路应用策略：丢弃或保留（保留时也不封存）。
	if p.cfg.IncompletePolicy == IncompleteDiscard {
		traces := make(map[TraceID]bool)
		for _, sp := range spans {
			traces[sp.TraceID] = true
		}
		for id := range traces {
			if !seals[id] {
				dels[id] = true
			}
		}
	}

	store.ApplyRecovery(spans, seals, dels)
	// 应用保留策略，清理过期与废弃链路。
	store.reap(time.Now())
	// 压缩快照，清理 WAL。
	return p.compact(store)
}

// compact 写出快照并截断 WAL。整个过程持有 persister 锁，
// 保证快照与 WAL 一致：压缩期间不会有新记录追加，避免截断丢失数据。
func (p *Persister) compact(store *Store) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	all := store.SnapshotAll()

	tmpPath := filepath.Join(p.cfg.DataDir, snapTmpName)
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(tmp)
	for _, tr := range all {
		rec := snapRecord{
			Type:   "trace",
			Trace:  tr.TraceID.String(),
			Start:  tr.Start.Format(time.RFC3339Nano),
			End:    tr.End.Format(time.RFC3339Nano),
			Sealed: tr.Sealed,
		}
		for _, sp := range tr.Spans {
			rec.Spans = append(rec.Spans, spanRecord{
				Type:   "s",
				Trace:  sp.TraceID.String(),
				Span:   sp.SpanID.String(),
				Parent: sp.ParentID.String(),
				Name:   sp.Name,
				Start:  sp.Start.Format(time.RFC3339Nano),
				End:    sp.End.Format(time.RFC3339Nano),
				Status: sp.Status,
				Tags:   sp.Tags,
			})
		}
		data, err := json.Marshal(rec)
		if err != nil {
			tmp.Close()
			return err
		}
		if _, err := bw.Write(append(data, '\n')); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, p.snapPath); err != nil {
		return err
	}
	// 截断 WAL 并重置缓冲。
	if err := p.wal.Truncate(0); err != nil {
		return err
	}
	if _, err := p.wal.Seek(0, 0); err != nil {
		return err
	}
	p.buf.Reset(p.wal)
	p.walSize = 0
	return nil
}

// maybeCompact 在 WAL 超阈值时触发压缩。
func (p *Persister) maybeCompact(store *Store) error {
	if p.WALSize() <= p.cfg.CompactThreshold {
		return nil
	}
	return p.compact(store)
}

// ---- 内部 ----

type spanRecord struct {
	Type   string            `json:"type"`
	Trace  string            `json:"trace,omitempty"`
	Span   string            `json:"span,omitempty"`
	Parent string            `json:"parent,omitempty"`
	Name   string            `json:"name,omitempty"`
	Start  string            `json:"start,omitempty"`
	End    string            `json:"end,omitempty"`
	Status Status            `json:"status,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

type snapRecord struct {
	Type   string       `json:"type"`
	Trace  string       `json:"trace"`
	Start  string       `json:"start"`
	End    string       `json:"end"`
	Sealed bool         `json:"sealed"`
	Spans  []spanRecord `json:"spans"`
}

func parseHex(s string, n int) ([]byte, error) {
	if len(s) != n*2 {
		return nil, fmt.Errorf("invalid hex length %d, want %d", len(s), n*2)
	}
	return hex.DecodeString(s)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func (p *Persister) loadSnapshot() ([]*Span, map[TraceID]bool, map[TraceID]bool, error) {
	spans := make([]*Span, 0)
	seals := make(map[TraceID]bool)
	dels := make(map[TraceID]bool)

	f, err := os.Open(p.snapPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return spans, seals, dels, nil
		}
		return nil, nil, nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	for scanner.Scan() {
		var rec snapRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return nil, nil, nil, err
		}
		tidBytes, err := parseHex(rec.Trace, 16)
		if err != nil {
			return nil, nil, nil, err
		}
		var tid TraceID
		copy(tid[:], tidBytes)
		if rec.Sealed {
			seals[tid] = true
		}
		for _, sr := range rec.Spans {
			sp, err := spanFromRecord(sr)
			if err != nil {
				return nil, nil, nil, err
			}
			spans = append(spans, sp)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, nil, err
	}
	return spans, seals, dels, nil
}

func (p *Persister) replayWAL() ([]*Span, map[TraceID]bool, map[TraceID]bool, error) {
	spans := make([]*Span, 0)
	seals := make(map[TraceID]bool)
	dels := make(map[TraceID]bool)

	f, err := os.Open(p.walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return spans, seals, dels, nil
		}
		return nil, nil, nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 16<<20)
	for scanner.Scan() {
		var rec spanRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return nil, nil, nil, err
		}
		switch rec.Type {
		case "s":
			sp, err := spanFromRecord(rec)
			if err != nil {
				return nil, nil, nil, err
			}
			spans = append(spans, sp)
		case "seal":
			b, err := parseHex(rec.Trace, 16)
			if err != nil {
				return nil, nil, nil, err
			}
			var tid TraceID
			copy(tid[:], b)
			seals[tid] = true
		case "del":
			b, err := parseHex(rec.Trace, 16)
			if err != nil {
				return nil, nil, nil, err
			}
			var tid TraceID
			copy(tid[:], b)
			dels[tid] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, nil, err
	}
	return spans, seals, dels, nil
}

func spanFromRecord(rec spanRecord) (*Span, error) {
	tidBytes, err := parseHex(rec.Trace, 16)
	if err != nil {
		return nil, err
	}
	sidBytes, err := parseHex(rec.Span, 8)
	if err != nil {
		return nil, err
	}
	var tid TraceID
	var sid SpanID
	copy(tid[:], tidBytes)
	copy(sid[:], sidBytes)
	var pid SpanID
	if rec.Parent != "" {
		pb, err := parseHex(rec.Parent, 8)
		if err != nil {
			return nil, err
		}
		copy(pid[:], pb)
	}
	start, err := parseTime(rec.Start)
	if err != nil {
		return nil, err
	}
	end, err := parseTime(rec.End)
	if err != nil {
		return nil, err
	}
	return &Span{
		TraceID:  tid,
		SpanID:   sid,
		ParentID: pid,
		Name:     rec.Name,
		Start:    start,
		End:      end,
		Status:   rec.Status,
		Tags:     rec.Tags,
	}, nil
}
