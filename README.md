# event-correlation-engine-go

本地流式安全事件关联检测引擎：按版本化规则对事件流做基于事件时间的窗口化聚合与告警判定，支持乱序到达、规则热更新与有界状态。

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

## 热更新与并发保证

- 规则集通过原子指针整体替换（`Engine.Reload`），`Process` 单次判定只读取一次指针，因此**单次判定只看到某一完整版本的规则集**，不会新旧规则混合；重载期间事件处理不中断。
- 事件处理在互斥锁下串行化，告警、迟到报告、状态演进均无数据竞争（`go test -race` 验证）。
- 每条告警携带 `RuleVersion`（命中时规则集版本）与 `Reason`（判定依据：规则、阈值、窗口、键、计数），结论可解释。
- 状态按键（`ruleID|groupKey`）维护，LRU 淘汰保证内存有界（`Config.MaxKeys`）；告警在计数达阈值时即时触发，因此淘汰不会丢失已确认结果，也不会因背压产生结果缺失。

## 本地验证

```bash
go build ./...
go vet ./...
go test -race -count=1 -v ./...
```

测试覆盖三类关键场景，日志中打印输入事件、命中的规则版本与判定依据：

1. **乱序与超范围迟到**（`engine/engine_test.go`）：乱序纳入与去重、超范围迟到单独报告且不污染结果、有界状态淘汰。
2. **非法及跨版本规则加载**（`rules/loader_test.go`）：五类加载错误、废弃字段映射/拒绝、默认值跨加载顺序稳定。
3. **处理过程中并发热更新**（`TestConcurrentHotReload`）：多 goroutine 交替 Reload 与处理，断言所有告警版本均为完整版本，竞态检测下通过。
