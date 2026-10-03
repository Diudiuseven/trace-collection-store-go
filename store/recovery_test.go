package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func walPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "trace.wal")
}

func TestRestartRecovery(t *testing.T) {
	path := walPath(t)
	base := time.Now().Add(-time.Hour)

	s, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	for b := byte(1); b <= 3; b++ {
		for _, sp := range makeTrace(b, base.Add(time.Duration(b)*time.Minute)) {
			s.Report(sp)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启后完整链路仍可查询
	s2, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, err := s2.Query(context.Background(), Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Traces) != 3 {
		t.Fatalf("重启后期望恢复 3 条链路，得到 %d", len(res.Traces))
	}
	for _, v := range res.Traces {
		if !v.Complete || len(v.Spans) != 3 {
			t.Fatalf("恢复数据不完整: %+v", v)
		}
	}
}

func TestRecoveryDropsIncompleteByDefault(t *testing.T) {
	path := walPath(t)
	base := time.Now().Add(-time.Hour)

	s, _ := Open(Config{WALPath: path})
	// 完整链路
	for _, sp := range makeTrace(1, base) {
		s.Report(sp)
	}
	// 半截链路：进程“崩溃”前只来得及上报子环节
	half := makeTrace(2, base.Add(time.Minute))
	s.Report(half[0])
	s.Report(half[1])
	s.Close()

	s2, err := Open(Config{WALPath: path}) // 默认 DropIncomplete
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, _ := s2.Query(context.Background(), Query{IncludeIncomplete: true})
	if len(res.Traces) != 1 || res.Traces[0].TraceID != mkTraceID(1) {
		t.Fatalf("半截链路应被丢弃，得到 %+v", res.Traces)
	}
}

func TestRecoveryKeepIncompletePolicy(t *testing.T) {
	path := walPath(t)
	base := time.Now().Add(-time.Hour)

	s, _ := Open(Config{WALPath: path})
	half := makeTrace(2, base)
	s.Report(half[0])
	s.Report(half[1])
	s.Close()

	s2, err := Open(Config{WALPath: path, IncompletePolicy: KeepIncomplete})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// 默认查询不得返回半截链路
	res, _ := s2.Query(context.Background(), Query{})
	if len(res.Traces) != 0 {
		t.Fatalf("半截链路不得当作完整链路返回: %+v", res.Traces)
	}
	// 显式请求时可见且被标记
	res2, _ := s2.Query(context.Background(), Query{IncludeIncomplete: true})
	if len(res2.Traces) != 1 || res2.Traces[0].Complete {
		t.Fatalf("半截链路应保留并标记为不完整: %+v", res2.Traces)
	}
}

// TestRecoveryToleratesTruncatedWAL 模拟崩溃在 WAL 写入中途。
func TestRecoveryToleratesTruncatedWAL(t *testing.T) {
	path := walPath(t)
	base := time.Now().Add(-time.Hour)

	s, _ := Open(Config{WALPath: path})
	for _, sp := range makeTrace(1, base) {
		s.Report(sp)
	}
	s.Close()

	// 在文件尾部追加半条记录，模拟崩溃时的不完整写入
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 100, '{', '"'}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, _ := s2.Query(context.Background(), Query{})
	if len(res.Traces) != 1 || !res.Traces[0].Complete {
		t.Fatalf("半条 WAL 记录不应影响已完整采集的数据: %+v", res.Traces)
	}
}

// TestRecoveryIdempotentReplay WAL 重放幂等：重复记录不产生重复环节。
func TestRecoveryIdempotentReplay(t *testing.T) {
	path := walPath(t)
	base := time.Now().Add(-time.Hour)

	s, _ := Open(Config{WALPath: path})
	for _, sp := range makeTrace(1, base) {
		s.Report(sp)
	}
	s.Close()
	// 手动把同样的记录再追加一遍（模拟重复上报）
	w, _ := openWAL(path)
	for _, sp := range makeTrace(1, base) {
		w.append(sp)
	}
	w.close()

	s2, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, _ := s2.Query(context.Background(), Query{})
	if len(res.Traces) != 1 || len(res.Traces[0].Spans) != 3 {
		t.Fatalf("重放应幂等，得到 %d 条链路 %d 个环节",
			len(res.Traces), len(res.Traces[0].Spans))
	}
}
