# event-correlation-engine-go

本地流式安全事件关联检测引擎：按版本化规则对事件流做基于事件时间的窗口化聚合与告警判定，支持乱序到达、规则热更新与有界状态；并提供**按版本固定的主动收尾**、**有界缓冲 + 可配置并发 + 背压**、**可取回的淘汰报告**，让运行中的判定结果任何时候都"收得干净、看得清楚"。

## 规则结构与版本兼容策略

规则集为 JSON 文档，顶层必须显式声明 `version`：

```json
{
  "version": 2,
  "rules": [
    {
      "id": "login-fail",
      "match": [{"field": "type", "op": "eq", "value": "login_fail"}],
      "window": "10m",
      "threshold": 3,
      "group_by": "src_ip",
      "severity": "high"
    }
  ]
}
```

- `match[].op` 支持 `eq` / `ne` / `gt` / `lt` / `contains`（`gt`/`lt` 优先数值比较，失败回退字符串序）。
- `window` 为 Go duration 字符串（如 `"5m"`）；`threshold` 为窗口内触发告警的最小计数。
- `group_by` 为关联键字段，缺省为全局单键（稳定默认值 `""`）；`severity` 缺省为 `"info"`。新增可选字段的默认值是常量，不随规则加载顺序变化（有测试保证）。

### 版本兼容

| 版本 | 说明 |
|---|---|
| 1 | 初版；`field` 作为分组键字段 |
| 2 | 新增可选字段 `severity`、`group_by`；`field` 废弃 |

废弃字段 `field` 按**明确映射**继续生效：加载时迁移到 `group_by`。若 `field` 与 `group_by` 同时出现且取值冲突，加载被**明确拒绝**（`ErrDeprecatedField`），绝不静默二选一。

### 可区分的加载错误

加载使用严格解码（`DisallowUnknownFields`），任何异常都以 `*rules.LoadError` 返回，`Kind` 可区分：

| Kind | 含义 |
|---|---|
| `ErrUnknownField` | 未知字段（顶层或规则内） |
| `ErrBadVersion` | 缺失或不支持的 `version` |
| `ErrInvalidRef` | 非法引用：空规则 ID、重复规则 ID |
| `ErrIncomplete` | 不完整条件：空 match、非法 op、非法 window/threshold、空规则集、JSON 格式错误 |
| `ErrDeprecatedField` | 废弃字段与替代字段冲突 |

未知字段、非法引用、不完整条件一律报错，不会被静默忽略。

## 水位与迟到语义

- 引擎维护事件时间水位：`watermark = max(已见事件时间) - AllowedLateness`。
- **乱序/容忍范围内迟到**（事件时间 > 水位）：正常纳入对应滚动窗口（`timestamp.Truncate(window)`），按事件 ID 去重，同一事件重复到达不重复计数；每个窗口达到阈值只告警一次。
- **超范围迟到**（事件时间 ≤ 水位）：不进入任何窗口、不推进水位、不污染已确认结果，而是记录为 `LateEvent`（含事件、当前水位、原因），通过 `Engine.LateEvents()` 单独获取。
- 窗口结束时间 ≤ 水位后，其状态被安全回收。

## 热更新与版本固定

- 规则集通过原子指针整体替换（`Engine.Reload`），单次判定只读取一次指针，因此**单次判定只看到某一完整版本的规则集**，不会新旧规则混合；重载期间事件处理不中断。
- **窗口级版本固定**：每个窗口在首次开启时，就把当时那一版规则的**规则集版本、阈值、窗口长度**固定下来。之后即便 Reload 进来新版本（调阈值、改窗口长度），已开启的窗口仍按旧版本累计与判定，直到它自然关闭；新版本只对切版本之后开启的新窗口生效。
  - 判定依据中的版本、阈值、窗口跨度取自窗口固定值，因此"同一窗口不会被新旧规则混算"。
  - 注意边界：窗口仍按 `timestamp.Truncate(窗口长度)` 对齐。新旧版本窗口长度不同时，同一事件可能对齐到不同起点，从而分属各自版本的窗口（这是预期行为，两版窗口独立结算，互不改写）。
- 每条告警携带 `RuleVersion`（窗口固定的规则集版本）与 `Reason`（判定依据：规则、阈值、窗口、键、计数），结论可解释。
- 事件处理在互斥锁下串行化状态演进（`Engine.Apply`），告警、迟到报告、状态演进均无数据竞争（`go test -race` 验证）。纯匹配计算（`Engine.Match`）在锁外完成，可被多个 worker 并发执行。

## 主动收尾（Drain）

切流、停机或确认某一版规则要下线时，可主动让引擎把当前所有还没触发告警的在途窗口一次性结算完，并直接拿到结果：

- `Engine.Drain() []Settlement`：结算**全部**在途窗口，清空在途状态，返回每个窗口的最终结论。
- `Engine.DrainVersion(version) []Settlement`：只结算固定规则版本等于 `version` 的窗口（用于确认某一版本规则下线），其他版本的在途窗口保持不变。

`Settlement` 至少包含：`RuleID`、`RuleVersion`（该窗口开启时固定的版本）、`Key`、`WindowStart/End`、`Count`、`Threshold`（固定阈值）、`Fired`（按固定版本判定是否达标）、`Reason`（判定依据 + "为何此刻才结算"：因人工 drain 而非水位推进，附当前水位）。

要点：

- 收尾严格按每个窗口**开启时那一版规则**判定，累计事件、阈值、结论不被新版本改写；结果中可直接看出它属于哪一版、为何到这一步才结算。
- 收尾后在途状态被清空，重复 Drain 不会产生重复结论；历史结算结论也可通过 `Engine.Settlements()` 审计取回。
- 防御性：若某窗口在收尾时计数才达到固定阈值，会补发一条告警（`Reason` 标注 "fired during manual drain"），绝不漏判。

## 有界缓冲、并发处理与背压（Runner）

在 `Engine` 之上，`Runner` 提供有界输入缓冲、可配置并发与优雅关闭：

```go
r := engine.NewRunner(eng, engine.RunnerConfig{
    BufferSize:   10000,                 // 有界输入缓冲，内存可控
    Workers:      4,                     // 并发处理 worker 数
    Backpressure: engine.BackpressureReject, // 或 BackpressureBlock
})
r.Start()
defer r.Close()

err := r.Submit(ev)   // Block：满则等待；Reject：满则立即返回 ErrQueueFull
```

- **缓冲有界，绝不无限占内存，也不静默丢事件**：
  - `BackpressureBlock`：缓冲满时 `Submit` 等待空位。
  - `BackpressureReject`：缓冲满时立即返回可区分的 `ErrQueueFull`（事件**没有被接收**，调用方可安全重试或转存）。`TrySubmit` 始终非阻塞。
  - 引擎关闭期间提交统一返回 `ErrNotRunning`，同样明确表示"未被接收"。
- **关闭排空，不丢已接收事件**：`Close()` 停止接收新事件，等待所有在途提交定论（入队或被拒），然后把缓冲里**已接收**的事件全部处理完才返回（幂等可重复调用）。
- 只要 `Submit` 返回 `nil`，事件即被接收；可通过 `Runner.Stats()` 核对 `Accepted == Processed`，测试据此验证无丢失。

## 有界状态与可审计的淘汰报告

- 状态按键（`ruleID|groupKey`）维护，LRU 淘汰保证内存有界（`Config.MaxKeys`）。
- 内存吃紧淘汰关联状态时，其中**尚未结算（未触发告警、未被水位确认）的窗口不再静默丢弃**，而是逐窗生成 `Eviction` 报告，通过 `Engine.Evictions()` 取回。报告至少包含：关联键（`Key`）、`RuleID`、`RuleVersion`（固定版本）、窗口区间、`Count/Threshold`、`Reason`（说明因 `MaxKeys` 被 LRU 淘汰、既未告警也未被水位确认），便于事后判断有无漏判。
- 已触发过告警的窗口结论已在 `Alerts()` 中可查，被淘汰时不再重复出报告；已被水位确认（窗口结束时间 ≤ 水位，不可能再有事件进入）的窗口由 GC 安全回收。

## 多版本兼容范围（面向长期并存）

- 规则文档仍支持 `version: 1 / 2`（加载、校验、废弃字段映射规则不变，见下节）。
- 长期并存主要体现在**运行时**：热更新不要求旧版本窗口立即迁移；每个在途窗口固定其开启版本，`Drain` / `DrainVersion` / `Eviction` / `Alert` 都带 `RuleVersion`，因此灰度期间 v1、v2 的在途窗口可以同时存在、分别判定、分别收尾与审计。
- 当前版本不支持把一个已开启的旧窗口"重算"为新版本语义（那会破坏判定一致性）；需要新语义时，让新事件开启新版本窗口，并对旧版本窗口调用 `DrainVersion(旧版本)` 干净下线。

## 本地验证

```bash
go build ./...
go vet ./...
go test -race -count=1 -v ./...
```

测试在日志中打印输入事件、命中的规则版本与判定依据（`engine_test.go` 的 `logAlerts`，以及 Drain/Eviction/Runner 用例中的结构化 `t.Logf`），可从日志看出处理了哪些事件、命中哪一版规则、计数/阈值与窗口区间。覆盖场景：

1. **乱序与超范围迟到**（`engine/engine_test.go`）：乱序纳入与去重、超范围迟到单独报告且不污染结果、有界状态不越界。
2. **非法及跨版本规则加载**（`rules/loader_test.go`）：五类加载错误、废弃字段映射/拒绝、默认值跨加载顺序稳定。
3. **处理过程中并发热更新**（`TestConcurrentHotReload`）：多 goroutine 交替 Reload 与处理，断言所有告警版本均为完整版本，竞态检测下通过。
4. **切换版本不混算**（`TestVersionPinningAcrossReload`）：旧窗口固定 v1 阈值/窗口，降到 v2 阈值也不提前告警，达标时归属 v1；切版本后的新窗口才按 v2 判定。
5. **主动收尾按版本归位**（`drain_test.go`）：v1/v2 残留窗口分别按各自固定版本结算，`Settlement` 含版本与"为何此刻才结算"；`DrainVersion` 只下线指定版本；收尾清空不重复、不漏判。
6. **背压与关闭排空不丢事件**（`runner_test.go`）：缓冲满时 Reject 得 `ErrQueueFull`、Block 等待且关闭时得 `ErrNotRunning`；`Close` 后排空所有已接收事件（`Accepted == Processed`）；多 worker 并发写入无丢失。
7. **淘汰报告可取回**（`eviction_test.go`）：LRU 淘汰的未结算窗口可通过 `Evictions()` 取回（键、版本、计数、原因齐全）；已告警窗口不重复出报告。
