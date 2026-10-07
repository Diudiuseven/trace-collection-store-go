# trace-collection-store-go

面向本地进程的结构化性能追踪采集与查询库。在保留"整体耗时记录"能力（根环节耗时即整体耗时）的基础上，提供多环节调用关系采集、并发安全存储、崩溃恢复与分页查询。

## 快速开始

```go
st, _ := store.Open(store.Config{
    Retention:        time.Hour,          // 保留时长
    MaxBytes:         64 << 20,           // 空间上限（估算）
    IncompletePolicy: store.DropIncomplete,
    WALPath:          "/var/lib/myapp/trace.wal",
})
defer st.Close()

tr := trace.NewTracer(st, trace.RatioSampler{Rate: 0.1, KeepErrors: true})

// 方式一：Start/End，End 可 defer，不改变被测代码返回值
ctx, end := tr.Start(ctx, "handle-request")
defer end(err != nil)

// 方式二：Do 包装，原样返回被测代码的结果
v, err := trace.Do(ctx, tr, "load-user", func(ctx context.Context) (User, error) {
    return loadUser(ctx, id)
})
```

## 数据模型

- **Span（环节）**：一次执行中的单个环节，含 `TraceID / SpanID / ParentID / Name / Start / End / Err`。
  父子关系通过 `ParentID` 表达；共享同一父环节、时间重叠的环节即并行关系。
- **TraceView（链路视图）**：查询返回的单位，`Spans` 按开始时间排序，结合各环节的
  `Start/End` 可还原每个环节在整条链路里的时间位置；`Duration` 为根环节耗时（即整体耗时）。

## 采样语义

- 采样决策在**链路根环节开始时一次性确定**：先生成链路 ID，再对该 ID 计算决策，
  通过 context 贯穿整条链路。不会出现部分环节被采样而其余丢失，也不会中途翻转。
- `RatioSampler.Rate`：取值 `[0,1]` 的小数采样率，基于链路 ID 的 FNV-1a 散列，
  把散列空间等分为 10000 个桶后按整数阈值比较（无浮点边界误差）。
  - 链路 ID 由密码学随机数生成，不同链路的散列桶近似独立且均匀，因此
    **大量链路的实际采集比例长期贴近配置值**（0.1 约一成、0.9 约九成），
    各链路的决策相互独立，与调用顺序、并发规模无关。
  - 决策是链路 ID 的纯函数：同一链路 ID 反复计算结果恒定；进程重启或从
    WAL 恢复后，同一链路的采集状态与重启前一致，不会翻转。
  - 边界取值确定：`0` 一条不采，`1` 全部采集；越界值按边界收敛
    （负数→0，大于 1→1，`NaN`→0），不存在未定义行为。
  - 采样率最小量化粒度为 1/10000。
- `RatioSampler.KeepErrors`：异常保留开关，与 `Rate` **相互独立**。
  - 为 `true` 时，未命中采样率的链路仍会在内存**暂存**其环节（不写盘）；
    链路内任一环节上报错误即整条**提升为保留**，暂存的全部环节一次性补落盘；
    链路正常结束则暂存数据立即丢弃。因此采样率再低，异常链路都会被完整保留。
  - 为 `false` 时，异常链路不享有任何额外保留，与正常链路一样按 `Rate` 决定去留。
  - 已知限制：根环节已上报、暂存链路被判定"正常完成并丢弃"之后才迟到的错误
    环节无法找回此前数据。
- 未命中采样且未开启 `KeepErrors` 的链路开销极小：不上报、不分配环节记录，
  单次决策约 10~20ns、零分配；已开启但最终正常的暂存链路不占用保留存储，
  **整体内存与磁盘占用随实际保留比例增长，而不是随链路总量增长**。

## 乱序与迟到上报

环节在结束时上报，因此并发下结束顺序与开始顺序无关。存储层按 `(TraceID, SpanID)`
归并，装配在查询时进行：迟到环节不会被丢弃，乱序/迟到上报的查询结果与全部按时上报一致。
上报幂等：重复上报同一环节（如 WAL 重放）不产生重复数据。

## 保留策略与空间上限

- `Config.Retention`：链路开始时间早于 `now-Retention` 的数据在下一次写入时被回收。
- `Config.MaxBytes`：内存估算上限，超限时按开始时间从最旧开始回收。
- 回收与采集、查询并发安全：内部读写锁保证查询不会读到半截数据，也不会访问已释放的资源。

## 崩溃恢复

- 配置 `WALPath` 后，**采样命中**的环节先落盘（含 fsync）再入内存；
  未命中采样、仅按 `KeepErrors` 暂存的环节不写 WAL，只有在链路出现错误
  整体提升时才补落盘。进程异常退出后重启自动回放恢复，WAL 体积只随
  实际保留的链路增长。
- 崩溃可能在 WAL 尾部留下半条记录，回放时安全忽略，不影响已完整采集的数据。
- 重启后只采集到一半的链路（根环节缺失）按 `IncompletePolicy` 处理：
  - `DropIncomplete`（默认）：恢复时丢弃；
  - `KeepIncomplete`：保留并标记 `Complete=false`，默认查询不返回，
    仅当 `Query.IncludeIncomplete=true` 时可见，绝不会被当作完整链路返回。

## 查询

```go
res, err := st.Query(ctx, store.Query{
    From:        time.Now().Add(-time.Hour),
    To:          time.Now(),
    Name:        "load-user",        // 链路内含该环节即匹配
    MinDuration: 100 * time.Millisecond, // 链路内任一环节耗时阈值
    Limit:       50,
    Cursor:      prev.NextCursor,    // 分页
})
```

- 支持时间范围、环节名称、耗时阈值过滤；`ctx` 控制超时。
- 游标分页：单页大小上限 1024，单次查询内存占用只随单页大小增长，与数据总量解耦。
- 默认只返回完整链路；`IncludeIncomplete` 可显式包含半截链路（带标记）。

## 并发与开销

- 采集接口（`Tracer.Start` / `Store.Report`）可任意并发调用；
  链路状态通过 `context.Context` 传播，同一进程内不同执行链路互不串扰。
- 采集不改变被测代码的返回值（见 `trace.Do` 与集成测试）。
- 热路径开销：每次环节一次 context 值写入 + 一次 map 插入；未采样链路仅一次决策计算。

## 测试

```sh
go test ./... -race
```

覆盖：并发采集无串扰、乱序与迟到上报一致性、半截链路识别、重启恢复（含截断 WAL）、
回收与查询并发、采样语义、分页与超时。

### 本地验证采样率是否真正生效

小数采样率是统计行为，建议用重复运行 + 比例断言来验证，而不是只看单条链路：

```sh
# 1) 采样器单测：0.1/0.25/0.5/0.9 各跑 2 万条随机链路，断言实际比例 ±3% 内；
#    并验证不同采样率命中数严格可区分（防止采样率退化为开关）
go test ./trace/ -run 'TestRatioSamplerRates|TestRatioSamplerDistinctRates' -v

# 2) 并发独立性与整链路一致性（含未采样链路零环节、采样链路环节完整）
go test ./trace/ -run 'Concurrent' -race -v

# 3) 端到端比例：tracer+store 真实并发跑 2000 条链路并分页统计保留比例 ±5%
go test ./store/ -run 'TestEndToEndFractionalRate' -v

# 4) 多轮重复确认统计断言稳定，不出现偶发翻转
go test ./... -count=10 -race

# 5) 决策开销：应稳定在纳秒级、0 分配，且与 Rate/并发规模无关
go test ./trace/ -run '^$' -bench BenchmarkSamplerDecide -benchmem
```

也可在业务里临时统计：记录一段时间内开启的新链路总数与
`st.Query` 返回（分页拉全）的链路数之比，0.1 下应长期接近 1:10，
0.9 下接近 9:10；同时观察 WAL 文件大小只随实际保留比例增长。

异常保留与采样的交叉场景（`Rate=0, KeepErrors=true` 只留异常链路；
`KeepErrors=false` 时异常不绕过采样）见 `TestEndToEndSampling`、
`TestKeepErrorsOnErrorTracePreserved`、`TestKeepErrorsOffNoBypass`；
边界 `0/1/越界/NaN` 见 `TestRatioSamplerBoundaries`。
