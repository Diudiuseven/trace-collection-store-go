package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Diudiuseven/trace-collection-store-go/trace"
)

func mkSpan(id trace.ID, sid byte, parent trace.SpanID, name string, start, end time.Time) trace.Span {
	var spanID trace.SpanID
	spanID[7] = sid
	return trace.Span{TraceID: id, SpanID: spanID, ParentID: parent, Name: name, Start: start, End: end, Sampled: true}
}

func mkTraceID(b byte) trace.ID {
	var id trace.ID
	id[15] = b
	return id
}

// mkTraceIDN 生成不会与单字节 ID 冲突的链路 ID。
func mkTraceIDN(n int) trace.ID {
	var id trace.ID
	id[13] = byte(n >> 16)
	id[14] = byte(n >> 8)
	id[15] = byte(n)
	return id
}

// makeTrace 生成 root + 两个并行子环节，返回乱序（子先父后）的环节序列。
func makeTrace(idByte byte, base time.Time) []trace.Span {
	return makeTraceN(int(idByte), base)
}

func makeTraceN(n int, base time.Time) []trace.Span {
	id := mkTraceIDN(n)
	var rootSID trace.SpanID
	rootSID[7] = 1
	childA := mkSpan(id, 2, rootSID, "a", base.Add(10*time.Millisecond), base.Add(40*time.Millisecond))
	childB := mkSpan(id, 3, rootSID, "b", base.Add(20*time.Millisecond), base.Add(30*time.Millisecond))
	root := mkSpan(id, 1, trace.SpanID{}, "root", base, base.Add(50*time.Millisecond))
	return []trace.Span{childB, childA, root} // 乱序：子环节先结束上报
}

func TestOutOfOrderAndLateAssembly(t *testing.T) {
	s, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Now().Add(-time.Hour)

	spans := makeTrace(1, base)
	for _, sp := range spans {
		s.Report(sp)
	}

	res, err := s.Query(context.Background(), Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Traces) != 1 {
		t.Fatalf("期望 1 条链路，得到 %d", len(res.Traces))
	}
	v := res.Traces[0]
	if !v.Complete {
		t.Fatal("链路应完整")
	}
	if len(v.Spans) != 3 {
		t.Fatalf("期望 3 个环节，得到 %d", len(v.Spans))
	}
	// 视图按开始时间排序，可还原时间位置
	if v.Spans[0].Name != "root" || v.Spans[1].Name != "a" || v.Spans[2].Name != "b" {
		t.Fatalf("环节顺序错误: %s %s %s", v.Spans[0].Name, v.Spans[1].Name, v.Spans[2].Name)
	}
	if v.Duration != 50*time.Millisecond {
		t.Fatalf("链路耗时 %v，期望 50ms", v.Duration)
	}

	// 迟到环节：链路完整后仍上报，不得被丢弃
	late := mkSpan(mkTraceID(1), 9, v.Spans[0].SpanID, "late", base.Add(45*time.Millisecond), base.Add(48*time.Millisecond))
	s.Report(late)
	res, _ = s.Query(context.Background(), Query{})
	if len(res.Traces[0].Spans) != 4 {
		t.Fatalf("迟到环节丢失，期望 4 个环节，得到 %d", len(res.Traces[0].Spans))
	}
}

// TestLateEqualsOnTime 验证乱序+迟到上报的查询结果与全部按时上报一致。
func TestLateEqualsOnTime(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	build := func(shuffle bool) []TraceView {
		s, _ := Open(Config{})
		defer s.Close()
		for b := byte(1); b <= 5; b++ {
			spans := makeTrace(b, base.Add(time.Duration(b)*time.Minute))
			if shuffle {
				// 先上报部分，查询前再补迟到的
				for _, sp := range spans[:1] {
					s.Report(sp)
				}
				for _, sp := range spans[1:] {
					s.Report(sp)
				}
			} else {
				for _, sp := range spans {
					s.Report(sp)
				}
			}
		}
		res, err := s.Query(context.Background(), Query{})
		if err != nil {
			t.Fatal(err)
		}
		return res.Traces
	}
	onTime := build(false)
	shuffled := build(true)
	if len(onTime) != len(shuffled) {
		t.Fatalf("结果条数不一致: %d vs %d", len(onTime), len(shuffled))
	}
	for i := range onTime {
		if onTime[i].TraceID != shuffled[i].TraceID ||
			len(onTime[i].Spans) != len(shuffled[i].Spans) ||
			onTime[i].Duration != shuffled[i].Duration {
			t.Fatalf("第 %d 条结果不一致", i)
		}
	}
}

func TestConcurrentCollectAndQuery(t *testing.T) {
	s, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Now().Add(-time.Hour)
	const workers = 16
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(2)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				for _, sp := range makeTraceN(w*20+i+1, base.Add(time.Duration(i)*time.Second)) {
					s.Report(sp)
				}
			}
		}(w)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := s.Query(context.Background(), Query{Limit: 4}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	res, err := s.Query(context.Background(), Query{Limit: maxPageSize})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Traces) != workers*20 {
		t.Fatalf("期望 %d 条链路，得到 %d", workers*20, len(res.Traces))
	}
	for _, v := range res.Traces {
		if !v.Complete || len(v.Spans) != 3 {
			t.Fatalf("链路 %s 数据不完整: complete=%v spans=%d", v.TraceID, v.Complete, len(v.Spans))
		}
	}
}

func TestRetentionAndMaxBytes(t *testing.T) {
	now := time.Now()
	s, err := Open(Config{Retention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.now = func() time.Time { return now } // 可控时钟
	// 一条过期链路 + 一条新鲜链路
	for _, sp := range makeTrace(1, now.Add(-2*time.Minute)) {
		s.Report(sp)
	}
	for _, sp := range makeTrace(2, now) {
		s.Report(sp)
	}
	res, _ := s.Query(context.Background(), Query{})
	if len(res.Traces) != 1 || res.Traces[0].TraceID != mkTraceID(2) {
		t.Fatalf("过期数据未回收，剩余 %d 条", len(res.Traces))
	}

	// 空间上限：最旧的先被回收
	s2, _ := Open(Config{MaxBytes: 3 * (spanOverhead + 16)})
	defer s2.Close()
	for b := byte(1); b <= 4; b++ {
		for _, sp := range makeTrace(b, now.Add(time.Duration(b)*time.Second)) {
			s2.Report(sp)
		}
	}
	res2, _ := s2.Query(context.Background(), Query{})
	if len(res2.Traces) == 4 {
		t.Fatal("空间上限未生效")
	}
	for _, v := range res2.Traces {
		if v.TraceID == mkTraceID(1) {
			t.Fatal("最旧的链路应被优先回收")
		}
	}
}

// TestEvictionConcurrentWithQuery 回收与采集、查询并发进行。
func TestEvictionConcurrentWithQuery(t *testing.T) {
	s, err := Open(Config{MaxBytes: 100 * spanOverhead})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Now().Add(-time.Hour)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				for _, sp := range makeTraceN(w*1000+i+1, base.Add(time.Duration(w*100+i)*time.Millisecond)) {
					s.Report(sp)
				}
			}
		}(w)
	}
	for q := 0; q < 4; q++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				res, err := s.Query(context.Background(), Query{Limit: 8})
				if err != nil {
					t.Error(err)
					return
				}
				// 读到的每条链路视图必须自洽，不得是半截数据
				for _, v := range res.Traces {
					if len(v.Spans) == 0 {
						t.Error("读到空链路")
					}
					for _, sp := range v.Spans {
						if sp.TraceID != v.TraceID {
							t.Error("读到串扰数据")
						}
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestSamplingKeepErrors(t *testing.T) {
	s, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Now().Add(-time.Hour)
	// 未采样但保留异常：正常链路完成即丢弃
	normal := makeTrace(1, base)
	for i := range normal {
		normal[i].Sampled = false
		s.Report(normal[i])
	}
	// 未采样但含错误：整条保留
	errSpans := makeTrace(2, base)
	for i := range errSpans {
		errSpans[i].Sampled = false
	}
	errSpans[0].Err = true // 子环节出错（先上报）
	for _, sp := range errSpans {
		s.Report(sp)
	}
	res, _ := s.Query(context.Background(), Query{})
	if len(res.Traces) != 1 {
		t.Fatalf("期望只保留 1 条异常链路，得到 %d", len(res.Traces))
	}
	v := res.Traces[0]
	if v.TraceID != mkTraceID(2) || !v.HasError || len(v.Spans) != 3 {
		t.Fatalf("异常链路保留不完整: %+v", v)
	}
}

func TestIncompleteNotReturnedAsComplete(t *testing.T) {
	s, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Now().Add(-time.Hour)
	// 半截链路：只有子环节，根环节永远不上报（模拟崩溃）
	half := makeTrace(1, base)
	s.Report(half[0])
	s.Report(half[1])
	// 完整链路
	for _, sp := range makeTrace(2, base.Add(time.Minute)) {
		s.Report(sp)
	}
	res, _ := s.Query(context.Background(), Query{})
	if len(res.Traces) != 1 || !res.Traces[0].Complete {
		t.Fatalf("半截链路被当作完整链路返回: %+v", res.Traces)
	}
	// 显式请求时可见且被标记
	res2, _ := s.Query(context.Background(), Query{IncludeIncomplete: true})
	if len(res2.Traces) != 2 {
		t.Fatalf("期望 2 条链路，得到 %d", len(res2.Traces))
	}
	var found bool
	for _, v := range res2.Traces {
		if v.TraceID == mkTraceID(1) {
			found = true
			if v.Complete {
				t.Error("半截链路应标记为不完整")
			}
		}
	}
	if !found {
		t.Error("半截链路未按策略保留")
	}
}

func TestQueryFiltersAndPagination(t *testing.T) {
	s, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Now().Add(-time.Hour)
	for b := byte(1); b <= 10; b++ {
		for _, sp := range makeTrace(b, base.Add(time.Duration(b)*time.Minute)) {
			s.Report(sp)
		}
	}
	ctx := context.Background()

	// 时间范围
	res, _ := s.Query(ctx, Query{From: base.Add(3 * time.Minute), To: base.Add(5 * time.Minute)})
	if len(res.Traces) != 3 {
		t.Fatalf("时间范围过滤期望 3 条，得到 %d", len(res.Traces))
	}
	// 环节名称
	res, _ = s.Query(ctx, Query{Name: "a"})
	if len(res.Traces) != 10 {
		t.Fatalf("名称过滤期望 10 条，得到 %d", len(res.Traces))
	}
	res, _ = s.Query(ctx, Query{Name: "nonexistent"})
	if len(res.Traces) != 0 {
		t.Fatal("不存在的名称应无结果")
	}
	// 耗时阈值：环节 a 耗时 30ms
	res, _ = s.Query(ctx, Query{MinDuration: 35 * time.Millisecond})
	if len(res.Traces) != 10 { // root 50ms 满足
		t.Fatalf("耗时阈值期望 10 条，得到 %d", len(res.Traces))
	}
	res, _ = s.Query(ctx, Query{MinDuration: 51 * time.Millisecond})
	if len(res.Traces) != 0 {
		t.Fatal("超过所有环节耗时的阈值应无结果")
	}

	// 分页遍历全集
	seen := map[trace.ID]bool{}
	cursor := ""
	for {
		page, err := s.Query(ctx, Query{Limit: 3, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range page.Traces {
			if seen[v.TraceID] {
				t.Fatal("分页出现重复")
			}
			seen[v.TraceID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 10 {
		t.Fatalf("分页遍历期望 10 条，得到 %d", len(seen))
	}

	// 超时
	toCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Query(toCtx, Query{}); err == nil {
		t.Fatal("已取消的 ctx 应返回错误")
	}
}

func TestQueryTimeoutUnderLoad(t *testing.T) {
	s, err := Open(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := time.Now().Add(-time.Hour)
	for b := 0; b < 200; b++ {
		for _, sp := range makeTraceN(b+1, base.Add(time.Duration(b)*time.Second)) {
			s.Report(sp)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := s.Query(ctx, Query{Limit: maxPageSize})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Traces) != 200 {
		t.Fatalf("期望 200 条，得到 %d", len(res.Traces))
	}
}
