// Package event 定义安全事件的基本模型。
package event

import "time"

// Event 表示一条安全事件，Timestamp 为事件时间（而非到达时间）。
type Event struct {
	ID        string            // 全局唯一 ID，用于窗口内去重
	Type      string            // 事件类型，对应规则的 event_type
	Key       string            // 默认关联键（当规则 key_field 在 Fields 中缺失时兜底）
	Timestamp time.Time         // 事件时间，用于窗口与水位计算
	Fields    map[string]string // 扩展属性，供 match 条件与 key_field 引用
}
