# event-correlation-engine-go

本地流式事件关联检测引擎：按版本化规则对安全事件流做基于事件时间的窗口化聚合与告警判定，支持乱序到达、规则热更新与有界并发处理。

## 结构

- `event/` — 事件模型（事件时间、关联键、扩展字段）。
- `rules/` — 多版本规则集的加载、校验与兼容处理。
- `engine/` — 水位推进、窗口聚合、迟到报告、有界状态、热更新与背压。

## 规则结构与版本兼容策略

规则集为 JSON 文档，`version` 必填，目前支持 `1` 与 `2`：

```json
{
  "version": 2,
  "filters": {"internal": [{"field": "net", "equals": "10.0.0.0/8"}]},
  "rules": [{
    "id": "r1", "event_type": "login_fail",
    "key_field": "user", "window_seconds": 60,
    "threshold": 3, "severity": "high",
    "description": "暴力破解", "filter": "internal",
    "match": [{"field": "status", "equals": "denied"}]
  }]
}
```

语义与兼容规则：

- **窗口**：以事件时间为基准的滚动窗口 `[start, start+window_seconds)`，按 `key_field`
  提取关联键（`Fields[key_field]` 缺失时兜底为 `Event.Key`），窗口内按事件 `ID` 去重计数，
  达到 `threshold` 即产生告警。
- **未知字段**：所有版本一律拒绝（`ErrUnknownField`），不做静默忽略。
- **非法引用**：v2 规则引用未定义的命名 `filter` 返回 `ErrInvalidReference`。
- **不完整条件**：`match`/`filters` 中缺少 `field` 或 `equals` 返回 `ErrIncompleteCondition`。
- **废弃字段**：`window`（秒）在 **v1** 中按 1:1 映射到 `window_seconds` 继续生效，
  与 `window_seconds` 同时出现且冲突时报错；在 **v2** 中被明确拒绝（`ErrDeprecatedField`）。
- **稳定默认值**：`key_field="source"`、`threshold=1`、`severity="medium"`、`match=[]`，
  与规则书写/加载顺序无关；规则在加载后按 `id` 排序，评估顺序确定。
- 其它错误类型：`ErrUnsupportedVersion`、`ErrInvalidRule`（缺必填项、阈值/窗口非正、ID 重复）、
  `ErrMalformed`（JSON 无法解析）。所有错误均为可类型断言的 `*rules.LoadError`，
  携带 `Kind`、`RuleID`、`Field`，便于区分处理。

## 水位与迟到语义

- 每个处理分区维护水位 `watermark = max(已见事件时间) - AllowedLateness`。
- 事件时间 **不早于水位** 的乱序/迟到事件被正常纳入对应窗口；窗口内按事件 ID 去重，
  重复投递不会重复计数。
- 事件时间 **早于水位** 的事件为超范围迟到：不进入任何窗口、不污染已确认结果，
  而是以 `ResultLateEvent` 单独报告（携带原事件与水位依据）。
- 窗口终点不超过水位时闭合：去重计数达到阈值则发出 `ResultAlert`，随后状态被回收。
- 引擎关闭（`Close`）时以无穷大水位闭合所有残留窗口，结果不缺失。

## 热更新与并发保证

- 规则集经 `Reload` 解析校验后通过 `atomic.Pointer` **整体替换**；解析失败时旧规则集保持不变。
- 任一事件的判定只读取一次快照，只会看到替换前或替换后的**某一个完整版本**，不会新旧混合。
- 开窗时快照规则的阈值、级别与规则集版本，且规则集版本参与窗口键的构成：
  重载前后打开的窗口互不合并，各自按开窗时的规则版本结算，判定依据可解释。
- 处理并发有界：固定 `Workers` 个 goroutine，按事件类型哈希分区，分区内状态无锁；
  每个分区输入缓冲有界，写满时 `Submit` 阻塞形成背压（引擎关闭时返回 `ErrClosed`）。
- 内存有界：每个分区按 `MaxKeys` 上限以 LRU 维护键状态；被淘汰键的未闭合窗口
  以 `ResultEvicted` **强制结算输出**（含已计数与阈值），背压或淘汰不会导致结果缺失。

## 输出

统一的 `Result`：`Kind`（`ResultAlert` / `ResultLateEvent` / `ResultEvicted`）、
`RuleID`、`RuleSetVersion`、`Key`、`Count`、窗口区间与 `Basis`（人类可读的判定依据）。

## 本地验证

```bash
go build ./...
go test ./...            # 单元测试
go test -race ./...      # 竞态检测（含并发热更新场景）
go test -v ./engine/     # 查看输入事件、命中规则版本与判定依据日志
```

测试覆盖：乱序与超范围迟到事件、非法及跨版本规则加载/重载、处理过程中的并发热更新、
内存上限下的键淘汰结算。
