# trace-collection-store-go

面向本地进程的性能追踪采集与查询库。在保留整体耗时记录的前提下，提供结构化的链路追踪能力：跨环节的调用关系、并发执行的因果归属、乱序迟到上报、采样、保留策略、崩溃恢复与分页查询。

## 核心概念

- **TraceID**：一条执行链路的唯一标识，128 位随机数。
- **SpanID**：链路内一个环节的唯一标识，64 位随机数。
- **Span（环节）**：记录单个环节的名称、开始/结束时间、状态与标签，以及父环节 ID。
- **Trace（链路）**：同一 TraceID 下全部环节的集合。根环节（无父环节）结束即封存整条链路。

## 快速开始

```go
cfg := trace.DefaultConfig()
cfg.DataDir = "/tmp/traces" // 可选：启用持久化
c, err := trace.NewCollector(cfg)
if err != nil { log.Fatal(err) }
defer c.Shutdown(context.Background())

// 开始一条链路
ctx, root := c.StartSpan(context.Background(), "handle-request")
defer root.End()

// 开始子环节
ctx2, db := c.StartSpan(ctx, "db-query")
db.SetTag("table", "users")
db.End()

// 查询
page, err := c.Store().Query(context.Background(), trace.Query{
    SpanName: "db-query",
    Limit:    10,
})
```

## 采样语义

采样决策在**链路开始时一次性确定**，并通过 context 传播贯穿整条链路：同一链路的所有环节共享同一采样决策，不会出现部分环节被采样而其余环节丢失。

- `SampleRate`：采样率，取值 `[0,1]`。`1.0` 全采，`0` 全不采。采样决策基于 TraceID 哈希，同一 TraceID 必然得到一致结果。
- `KeepError`：是否保留包含异常环节的链路。即使采样未命中，只要链路内有环节状态为 `StatusError`，整条链路仍被保留。

未命中采样且不保留异常时，采集走廉价路径（不写存储），对被测代码耗时量级无显著影响。

## 保留策略

- `RetentionPeriod`：链路保留时长，超过后回收已封存链路。
- `MaxTraces`：内存中最多保留的链路数，超出后从最老的链路开始回收（优先已封存）。
- `AbandonTimeout`：运行时一条链路超过该时长仍未封存（根环节未结束），视为废弃并回收。
- `ReapInterval`：后台回收周期。

回收协程与采集、查询并发运行，通过读写锁保证不会读到半截数据，也不会访问已释放的资源。

## 持久化与崩溃恢复

- `DataDir`：持久化目录。为空时仅内存存储。
- 采用 **WAL（预写日志）+ 快照** 机制：
  - 每个环节结束追加一条 WAL 记录；链路封存追加封存记录；链路删除追加删除记录。
  - WAL 字节数超过 `CompactThreshold` 时，将已封存链路写入快照并截断 WAL。
  - `WALSync` 为 true 时，链路封存强制刷盘，保证已封存链路重启后可查。
- 重启时加载快照并重放 WAL，恢复链路状态。

### 不完整链路处理

只采集到一半的链路（有环节上报但未封存）按 `IncompletePolicy` 处理：

- `IncompleteDiscard`（默认）：丢弃，不作为完整链路返回。
- `IncompleteKeep`：保留但标记为未封存（`Sealed=false`），绝不作为完整链路返回。

## 查询

`Store().Query(ctx, Query)` 支持以下过滤条件（零值不限制）：

- `StartTime` / `EndTime`：按链路开始时间过滤。
- `SpanName`：匹配包含该名称环节的链路。
- `MinDuration` / `MaxDuration`：按环节耗时过滤（任一环节满足即可）。
- `Limit`：单页返回的最大链路数（默认 100，上限 1000）。
- `Cursor`：分页游标，来自上一页的 `NextCursor`。

查询通过 `ctx` 支持超时；单次返回的链路数受 `Limit` 约束，内存占用不随数据总量无界增长。

## 配置项

| 字段 | 默认值 | 说明 |
|------|--------|------|
| SampleRate | 1.0 | 采样率 [0,1] |
| KeepError | true | 保留异常链路 |
| RetentionPeriod | 1h | 链路保留时长 |
| MaxTraces | 10000 | 最大链路数 |
| ReapInterval | 1m | 回收周期 |
| DataDir | "" | 持久化目录 |
| WALSync | true | 封存时强制刷盘 |
| CompactThreshold | 16MB | WAL 压缩阈值 |
| IncompletePolicy | Discard | 不完整链路策略 |
| AbandonTimeout | 5m | 废弃链路超时 |

## 架构

```
Collector
  ├── Sampler        采样决策
  ├── Store          内存存储（map + 有序索引）
  ├── Persister      WAL + 快照
  └── runReaper      后台回收
```

- 环节结束时：上报 Store（并发安全），追加 WAL。
- 根环节结束时：封存链路，追加封存记录。
- 回收协程：按策略回收过期链路，追加删除记录，触发压缩。
- 查询：基于有序索引分页，有界内存。
