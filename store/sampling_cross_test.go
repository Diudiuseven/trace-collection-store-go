package store

import (
	"context"
	"testing"
	"time"

	"github.com/Diudiuseven/trace-collection-store-go/trace"
)

// reportTrace 上报一条完整链路（root+a+b，乱序），可指定采样位与异常。
func reportTrace(s *Store, n int, base time.Time, sampled bool, errOn byte) {
	spans := makeTraceN(n, base)
	for i := range spans {
		spans[i].Sampled = sampled
		if errOn != 0 && spans[i].SpanID[7] == errOn {
			spans[i].Err = true
		}
		s.Report(spans[i])
	}
}

// TestKeepErrorsOffErrorsDoNotBypassSampling 开关关闭时，
// 异常链路不得绕过采样率被额外保留（经 tracer 端到端验证：
// 未命中且 KeepErrors=false 的链路根本不会向 store 上报任何环节）。
func TestKeepErrorsOffErrorsDoNotBypassSampling(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	tr := trace.NewTracer(s, trace.RatioSampler{Rate: 0, KeepErrors: false})

	runFail := func(n int) {
		ctx, endRoot := tr.Start(context.Background(), "root")
		_, endA := tr.Start(ctx, "a")
		_, endB := tr.Start(ctx, "b")
		endA(true)
		endB(false)
		endRoot(true)
		_ = n
	}
	for n := 0; n < 50; n++ {
		runFail(n)
	}

	res, _ := s.Query(context.Background(), Query{IncludeIncomplete: true})
	if len(res.Traces) != 0 {
		t.Fatalf("KeepErrors 关闭时异常链路不得绕过采样，得到 %d 条", len(res.Traces))
	}
	if s.total != 0 || len(s.traces) != 0 {
		t.Fatalf("未保留链路不得占用存储: total=%d traces=%d", s.total, len(s.traces))
	}
}


// TestKeepErrorsOnLowRateStillSavesErrors 采样率再低（0），只要开关打开，
// 异常链路完整保留；无异常的未命中链路完成即丢弃。
func TestKeepErrorsOnLowRateStillSavesErrors(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	base := time.Now().Add(-time.Hour)

	for n := 1; n <= 20; n++ {
		reportTrace(s, n, base.Add(time.Duration(n)*time.Minute), false, 0) // 全部正常 → 丢弃
	}
	reportTrace(s, 21, base.Add(21*time.Minute), false, 2) // 异常 → 整条保留
	reportTrace(s, 22, base.Add(22*time.Minute), false, 1) // 根环节异常 → 同样保留

	res, _ := s.Query(context.Background(), Query{})
	if len(res.Traces) != 2 {
		t.Fatalf("期望仅 2 条异常链路被保留，得到 %d", len(res.Traces))
	}
	for _, v := range res.Traces {
		if !v.HasError || !v.Complete || len(v.Spans) != 3 {
			t.Fatalf("异常链路应完整保留: %+v", v)
		}
	}
}

// TestSampledTracesKeptRegardlessOfErrorFlag 命中采样的链路，
// 无论是否异常、KeepErrors 是否打开，都完整保留。
func TestSampledTracesKeptRegardlessOfErrorFlag(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	base := time.Now().Add(-time.Hour)
	// Rate=1 的语义：全部保留。KeepErrors 关闭也不应影响命中链路。
	reportTrace(s, 1, base, true, 0)
	reportTrace(s, 2, base.Add(time.Minute), true, 3)
	res, _ := s.Query(context.Background(), Query{})
	if len(res.Traces) != 2 {
		t.Fatalf("Rate=1 应全部保留，得到 %d 条", len(res.Traces))
	}
}

// TestStorageScalesWithKeptRatioNotTotal 低采样率下，未命中且无异常的链路
// 完成后不得长期占用存储：存储规模随实际保留比例而非链路总量增长。
func TestStorageScalesWithKeptRatioNotTotal(t *testing.T) {
	s, _ := Open(Config{})
	defer s.Close()
	base := time.Now().Add(-time.Hour)

	// 400 条未命中、无异常的暂存链路，全部正常完成。
	for n := 1; n <= 400; n++ {
		reportTrace(s, n, base.Add(time.Duration(n)*time.Second), false, 0)
	}
	if len(s.traces) != 0 || s.total != 0 {
		t.Fatalf("未命中无异常链路完成后应释放存储: traces=%d total=%d", len(s.traces), s.total)
	}

	// 再加入 10 条命中链路，存储只应反映这 10 条。
	for n := 401; n <= 410; n++ {
		reportTrace(s, n, base.Add(time.Duration(n)*time.Second), true, 0)
	}
	if len(s.traces) != 10 {
		t.Fatalf("期望仅保留 10 条命中链路，得到 %d 条", len(s.traces))
	}
	var spanCount int
	for _, e := range s.traces {
		spanCount += len(e.spans)
	}
	if spanCount != 30 {
		t.Fatalf("期望 30 个环节，得到 %d", spanCount)
	}
}

// TestSamplingStateStableAcrossRestart 同一链路的采集状态经 WAL 落盘、
// 进程重启恢复后必须与重启前一致，不得翻转：
// 命中的依旧命中保留；未命中且无异常的不产生持久化残留。
func TestSamplingStateStableAcrossRestart(t *testing.T) {
	path := walPath(t)
	base := time.Now().Add(-time.Hour)

	s, _ := Open(Config{WALPath: path})
	reportTrace(s, 1, base, true, 0)   // 命中
	reportTrace(s, 2, base.Add(time.Minute), false, 0) // 未命中正常：内存中已丢弃
	reportTrace(s, 3, base.Add(2*time.Minute), false, 2) // 未命中但异常：保留
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(Config{WALPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	res, _ := s2.Query(context.Background(), Query{IncludeIncomplete: true})
	// 注意：未命中正常链路的环节曾先写入 WAL，重放后同样在“完成即丢弃”路径被移除。
	got := map[trace.ID]bool{}
	for _, v := range res.Traces {
		got[v.TraceID] = v.HasError
	}
	if len(got) != 2 {
		t.Fatalf("重启后期望 2 条链路（1 条命中 + 1 条异常），得到 %d: %v", len(got), got)
	}
	if _, ok := got[mkTraceIDN(1)]; !ok {
		t.Fatal("命中链路重启后丢失，采样状态翻转")
	}
	if isErr, ok := got[mkTraceIDN(3)]; !ok || !isErr {
		t.Fatal("异常保留链路重启后丢失或标记错误")
	}
	if _, ok := got[mkTraceIDN(2)]; ok {
		t.Fatal("未命中且无异常链路不应被持久化恢复")
	}
}
