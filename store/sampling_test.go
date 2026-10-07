package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Diudiuseven/trace-collection-store-go/trace"
)

// unsampledTrace 构造一条 Sampled=false 的完整链路（root+2 子环节，乱序上报），
// fail 时把其中一个子环节标记为异常。
func unsampledTrace(n int, base time.Time, fail bool) []trace.Span {
	spans := makeTraceN(n, base)
	for i := range spans {
		spans[i].Sampled = false
	}
	if fail {
		spans[0].Err = true
	}
	return spans
}

// TestTentativeTracesNotPersisted 未命中采样且无异常的链路既不留在内存，
// 也不得写入 WAL：磁盘占用不随链路总量增长。
func TestTentativeTracesNotPersisted(t *testing.T) {
	path := walPath(t)
	base := time.Now()

	s, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		for _, sp := range unsampledTrace(i, base.Add(time.Duration(i)*time.Millisecond), false) {
			s.Report(sp)
		}
	}
	// 内存中已全部丢弃
	res, _ := s.Query(context.Background(), Query{IncludeIncomplete: true})
	if len(res.Traces) != 0 {
		t.Fatalf("未命中且无异常的链路不应留在内存，剩余 %d 条", len(res.Traces))
	}
	if s.total != 0 || len(s.traces) != 0 {
		t.Fatalf("暂存链路未释放: total=%d traces=%d", s.total, len(s.traces))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// WAL 文件不应包含任何暂存链路数据
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("未命中链路写入了 WAL，文件大小=%d 字节", info.Size())
	}

	// 重启后同样为空
	s2, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, _ = s2.Query(context.Background(), Query{IncludeIncomplete: true})
	if len(res.Traces) != 0 {
		t.Fatalf("重启后恢复出了应被丢弃的链路 %d 条", len(res.Traces))
	}
}

// TestErrorPromotedTracePersisted 未命中采样但出现异常的链路在提升时
// 整链补落盘，重启后完整恢复，状态不翻转。
func TestErrorPromotedTracePersisted(t *testing.T) {
	path := walPath(t)
	base := time.Now()

	s, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	// 大量正常暂存链路 + 1 条异常链路
	for i := 2; i <= 50; i++ {
		for _, sp := range unsampledTrace(i, base.Add(time.Duration(i)*time.Millisecond), false) {
			s.Report(sp)
		}
	}
	errTrace := unsampledTrace(1, base, true)
	for _, sp := range errTrace { // 子环节先报、错误最先出现
		s.Report(sp)
	}
	res, _ := s.Query(context.Background(), Query{})
	if len(res.Traces) != 1 || res.Traces[0].TraceID != mkTraceIDN(1) {
		t.Fatalf("提升前仅应有 1 条异常链路，得到 %+v", res.Traces)
	}
	if !res.Traces[0].HasError || len(res.Traces[0].Spans) != 3 {
		t.Fatalf("异常链路不完整: %+v", res.Traces[0])
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：异常链路完整恢复，正常暂存链路一条都不应出现
	s2, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, _ = s2.Query(context.Background(), Query{IncludeIncomplete: true})
	if len(res.Traces) != 1 {
		t.Fatalf("重启后期望 1 条链路，得到 %d", len(res.Traces))
	}
	v := res.Traces[0]
	if v.TraceID != mkTraceIDN(1) || !v.Complete || !v.HasError || len(v.Spans) != 3 {
		t.Fatalf("重启后异常链路状态翻转或不完整: %+v", v)
	}
}

// TestSampledTracesPersisted 采样命中的链路始终落盘并可恢复。
func TestSampledTracesPersisted(t *testing.T) {
	path := walPath(t)
	base := time.Now()

	s, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	for b := byte(1); b <= 5; b++ {
		for _, sp := range makeTrace(b, base.Add(time.Duration(b)*time.Minute)) {
			s.Report(sp) // mkSpan 默认 Sampled=true
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, _ := s2.Query(context.Background(), Query{})
	if len(res.Traces) != 5 {
		t.Fatalf("采样链路重启后期望 5 条，得到 %d", len(res.Traces))
	}
}

// TestStorageGrowthProportionalToKept 存储占用随实际保留比例增长，
// 而非随链路总量增长。
func TestStorageGrowthProportionalToKept(t *testing.T) {
	base := time.Now()

	// 只有异常提升链路：100 条正常暂存 + 5 条异常
	s, _ := Open(Config{})
	defer s.Close()
	for i := 0; i < 100; i++ {
		for _, sp := range unsampledTrace(i+1000, base.Add(time.Duration(i)*time.Millisecond), false) {
			s.Report(sp)
		}
	}
	if s.total != 0 {
		t.Fatalf("全部正常暂存链路后 total 应为 0，得到 %d", s.total)
	}
	for i := 0; i < 5; i++ {
		for _, sp := range unsampledTrace(i+2000, base.Add(time.Duration(i)*time.Millisecond), true) {
			s.Report(sp)
		}
	}
	afterErrors := s.total
	const oneTraceOverhead = int64(3) * (spanOverhead + 2) // 3 个短名环节
	if afterErrors < 4*oneTraceOverhead || afterErrors > 6*oneTraceOverhead {
		t.Fatalf("存储占用应只反映 5 条异常链路，得到 %d（预期约 %d）", afterErrors, 5*oneTraceOverhead)
	}

	// 采样命中场景：100 条全部命中时占用应与链路总量同阶，
	// 而 100 条全部未命中（无异常）时为 0，二者必须有数量级差异。
	sampled, _ := Open(Config{})
	defer sampled.Close()
	for i := 0; i < 100; i++ {
		for _, sp := range makeTraceN(i+3000, base.Add(time.Duration(i)*time.Millisecond)) {
			sampled.Report(sp)
		}
	}
	if sampled.total < 10*afterErrors {
		t.Fatalf("全量采集的占用(%d)与仅异常保留(%d)拉不开差距", sampled.total, afterErrors)
	}
}

// TestErrorPromotionLateError 链路在根环节上报之后、尚未被回收前又有
// 异常环节迟到（同一进程内）：提升仍应生效且整链完整。
func TestErrorPromotionMidTrace(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	base := time.Now()
	spans := unsampledTrace(1, base, true)
	// spans 顺序为 childB(含 Err), childA, root：错误环节最先到达，
	// 随后所有环节合并进已提升的链路。
	for _, sp := range spans {
		s.Report(sp)
	}
	res, _ := s.Query(context.Background(), Query{})
	if len(res.Traces) != 1 || len(res.Traces[0].Spans) != 3 || !res.Traces[0].HasError {
		t.Fatalf("异常提升链路装配错误: %+v", res.Traces)
	}
}
