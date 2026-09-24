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
