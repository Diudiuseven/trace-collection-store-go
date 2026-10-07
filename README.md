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

采样决策在**链路根环节开始时一次性确定**，通过 context 贯穿整条链路，
不会出现部分环节被采样而其余缺失的情况。

### `RatioSampler.Rate`：确定性比率采样

- 取值范围 `[0,1]`：`0` 表示一条不留，`1` 表示全部保留；
  越界值按边界收敛（负数→0，大于 1→1，`NaN`→0，`±Inf`→对应边界），无未定义行为。
- 对链路 ID 做 **FNV-1a 64 位散列**后在全 64 位空间上做定点阈值比较（不取模、无取模偏差）。
  随机链路 ID 的散列值近似独立均匀分布，因此长时间运行下**实际被采集的链路比例贴近配置值**：
  配 `0.1` 约一成、`0.25` 约四分之一、`0.5` 约一半、`0.9` 约九成，而不是只有"全采/全不采"。
- 决策**只依赖链路 ID**，不含随机源、时间或调用顺序：
  - 同一进程内并发生成的众多链路各自独立决策，不存在成批同进同出；
  - 同一条链路反复计算结果恒定；进程重启（或从 WAL 恢复）后，同一链路 ID 的决策不翻转。
- 热路径零分配：决策为 16 字节内联散列 + 一次整数比较，约 10ns，且开销不随采样率变化（见基准测试）。

### `RatioSampler.KeepErrors`：异常保留（与采样率相互独立）

- 为 `true` 时：**未命中**采样率的链路仍会暂存其环节；链路内任一环节上报错误，
  整条链路立即提升为保留；链路正常结束则暂存数据立刻丢弃，不长期占用存储。
- 为 `false`（默认）时：未命中的链路根本不上报任何环节，**异常链路也不会绕过采样率被额外保留**。
- 两个维度的关系可概括为：

  | 采样命中 | KeepErrors | 链路含错误 | 结果 |
  |---|---|---|---|
  | 是 | 任意 | 任意 | 完整保留 |
  | 否 | `true` | 是 | 完整保留（异常提升） |
  | 否 | `true` | 否 | 正常结束即丢弃 |
  | 否 | `false` | 任意（含错误） | 不上报、不保留 |

  已知限制：若错误环节在链路已被判定"正常完成并丢弃"之后才迟到上报，
  此前的环节已无法找回，该链路将以不完整形式保留。

### 本地验证采样率是否真正生效

```sh
# 全量测试（含小数比例、并发独立性、边界、异常交叉、重启一致性），-race 检查数据竞争
go test ./... -race

# 重复运行确认比例型断言的稳定性（测试采用大样本与宽容忍带）
go test ./... -count=20

# 查看单次决策开销（应稳定在纳秒级、0 allocs/op，与 Rate 取值无关）
go test ./trace -bench BenchmarkDecide -benchmem
```

关键测试：`TestRatioSamplerFractionRates`（0.1/0.25/0.5/0.9 实际比例，容忍带 ±0.02）、
`TestTracerFractionRatesEndToEnd`（经 Tracer 端到端比例）、
`TestTracerConcurrentDecisionIndependentAndSticky`（并发独立 + 整链路一致）、
`TestRatioSamplerBoundaries`（0/1/越界/NaN）、
`TestRatioSamplerDeterministic`（重复与重启不翻转）、
`TestKeepErrors*` / `TestStorageScalesWithKeptRatioNotTotal`（异常保留交叉与存储不随总量增长）。

## 乱序与迟到上报

环节在结束时上报，因此并发下结束顺序与开始顺序无关。存储层按 `(TraceID, SpanID)`
归并，装配在查询时进行：迟到环节不会被丢弃，乱序/迟到上报的查询结果与全部按时上报一致。
上报幂等：重复上报同一环节（如 WAL 重放）不产生重复数据。

## 保留策略与空间上限

- `Config.Retention`：链路开始时间早于 `now-Retention` 的数据在下一次写入时被回收。
- `Config.MaxBytes`：内存估算上限，超限时按开始时间从最旧开始回收。
- 回收与采集、查询并发安全：内部读写锁保证查询不会读到半截数据，也不会访问已释放的资源。

## 崩溃恢复

- 配置 `WALPath` 后，每个环节先落盘（含 fsync）再入内存，进程异常退出后重启自动回放恢复。
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
