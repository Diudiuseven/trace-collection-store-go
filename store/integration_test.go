package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Diudiuseven/trace-collection-store-go/store"
	"github.com/Diudiuseven/trace-collection-store-go/trace"
)

// 被测业务：两级调用，内层并发。
func workload(ctx context.Context, tr *trace.Tracer, fail bool) (int, error) {
	return trace.Do(ctx, tr, "outer", func(ctx context.Context) (int, error) {
		var wg sync.WaitGroup
		results := make([]int, 4)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				v, _ := trace.Do(ctx, tr, "inner", func(ctx context.Context) (int, error) {
					time.Sleep(time.Millisecond)
					return i, nil
				})
				results[i] = v
			}(i)
		}
		wg.Wait()
		sum := 0
		for _, v := range results {
			sum += v
		}
		if fail {
			return sum, errors.New("workload failed")
		}
		return sum, nil
	})
}

// TestEndToEnd 全链路：并发采集不改变返回值，结构完整可查询。
func TestEndToEnd(t *testing.T) {
	st, err := store.Open(store.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	tr := trace.NewTracer(st, trace.RatioSampler{Rate: 1})

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := workload(context.Background(), tr, false)
			if v != 6 || err != nil { // 0+1+2+3
				t.Errorf("采集改变了被测代码的返回值: %v, %v", v, err)
			}
		}()
	}
	wg.Wait()

	res, err := st.Query(context.Background(), store.Query{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Traces) != n {
		t.Fatalf("期望 %d 条链路，得到 %d", n, len(res.Traces))
	}
	for _, v := range res.Traces {
		if !v.Complete {
			t.Fatal("链路应完整")
		}
		if len(v.Spans) != 5 { // 1 outer + 4 inner
			t.Fatalf("期望 5 个环节，得到 %d", len(v.Spans))
		}
		// 父子关系正确
		var root trace.Span
		inners := 0
		for _, sp := range v.Spans {
			if sp.Name == "outer" {
				root = sp
			}
		}
		for _, sp := range v.Spans {
			if sp.Name == "inner" {
				inners++
				if sp.ParentID != root.SpanID {
					t.Fatal("inner 环节的父环节应为 outer")
				}
			}
		}
	}
}

// TestEndToEndSampling 采样率 0 + 保留异常：只有异常链路被保留。
func TestEndToEndSampling(t *testing.T) {
	st, err := store.Open(store.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	tr := trace.NewTracer(st, trace.RatioSampler{Rate: 0, KeepErrors: true})

	for i := 0; i < 5; i++ {
		_, _ = workload(context.Background(), tr, false) // 正常，应被丢弃
	}
	_, err = workload(context.Background(), tr, true) // 异常，应被保留
	if err == nil {
		t.Fatal("被测代码的 error 返回值被改变")
	}

	res, err := st.Query(context.Background(), store.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Traces) != 1 {
		t.Fatalf("期望只保留 1 条异常链路，得到 %d", len(res.Traces))
	}
	v := res.Traces[0]
	if !v.HasError || !v.Complete || len(v.Spans) != 5 {
		t.Fatalf("异常链路不完整: %+v", v)
	}
}
