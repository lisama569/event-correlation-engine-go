// Package engine 实现基于事件时间的窗口关联、水位推进与规则热更新。
package engine

import "time"

// Event 一条安全事件。
type Event struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"` // 事件时间
	Fields    map[string]string `json:"fields"`
}

// Alert 一次规则命中告警。
type Alert struct {
	RuleID      string    `json:"rule_id"`
	RuleVersion int       `json:"rule_version"` // 命中时使用的规则集版本
	Key         string    `json:"key"`          // 关联键（group_by 值）
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Count       int       `json:"count"`
	Reason      string    `json:"reason"` // 判定依据（可解释）
}

// LateEvent 超出容忍范围的迟到事件报告。
type LateEvent struct {
	Event     Event  `json:"event"`
	Watermark int64  `json:"watermark"` // 当前水位（UnixNano）
	Reason    string `json:"reason"`
}

// Settlement 主动收尾（drain）时，对一个尚未触发告警的在途窗口给出的最终判定结论。
// 结论严格按该窗口开启时固定下来的那一版规则（版本、阈值、窗口长度）给出，
// 不会被之后热更新进来的新版本改写。
type Settlement struct {
	RuleID      string    `json:"rule_id"`
	RuleVersion int       `json:"rule_version"` // 窗口开启时固定的规则集版本
	Key         string    `json:"key"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Count       int       `json:"count"`     // 窗口内累计的去重事件数
	Threshold   int       `json:"threshold"` // 开启窗口时那一版规则的阈值
	Fired       bool      `json:"fired"`     // 按固定版本判定是否达到阈值
	Reason      string    `json:"reason"`    // 判定依据与"为何此刻才结算"
}

// Eviction 内存吃紧、状态被 LRU 淘汰时，对被迫丢弃的未结算窗口出具的可审计报告。
// 被淘汰的关联不会静默消失，可通过 Engine.Evictions() 单独取回。
type Eviction struct {
	RuleID      string    `json:"rule_id"`
	RuleVersion int       `json:"rule_version"` // 被淘汰窗口固定的规则集版本
	Key         string    `json:"key"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Count       int       `json:"count"`
	Threshold   int       `json:"threshold"`
	Reason      string    `json:"reason"` // 淘汰原因
}
