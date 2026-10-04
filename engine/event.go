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

// EvictionReport 关联状态被内存上限淘汰时的报告。
// 已确认告警在计数达阈值时已即时触发，不在淘汰损失范围内；
// 报告暴露的是被淘汰时仍未结算的窗口，便于事后判断有没有漏判。
type EvictionReport struct {
	RuleID         string `json:"rule_id"`
	RuleVersion    int    `json:"rule_version"` // 状态开启时使用的规则集版本
	Key            string `json:"key"`          // 关联键（group_by 值）
	PendingWindows int    `json:"pending_windows"`
	DroppedEvents  int    `json:"dropped_events"`
	Reason         string `json:"reason"`
}
