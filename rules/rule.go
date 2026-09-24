// Package rules 定义规则集的数据结构、版本兼容加载与校验。
package rules

import "time"

// 支持的规则定义版本。
const (
	VersionV1 = 1 // 初版：threshold 窗口计数
	VersionV2 = 2 // 新增可选字段 severity / group_by，废弃字段 field -> group_by
)

// Condition 描述单条匹配条件：事件字段 op 值。
type Condition struct {
	Field string `json:"field"`
	Op    string `json:"op"` // eq | ne | gt | lt | contains
	Value string `json:"value"`
}

// Rule 一条关联规则。
type Rule struct {
	ID        string        `json:"id"`
	Match     []Condition   `json:"match"`
	Window    time.Duration `json:"-"`
	WindowRaw string        `json:"window"` // 如 "5m"
	Threshold int           `json:"threshold"`
	// v2 新增可选字段（稳定默认值，不随加载顺序变化）
	Severity string `json:"severity,omitempty"`
	GroupBy  string `json:"group_by,omitempty"`
	// v1 废弃字段：field，按明确映射迁移到 group_by
	LegacyField string `json:"field,omitempty"`
}

// RuleSet 某一完整版本的规则集快照。
type RuleSet struct {
	Version int
	Rules   []Rule
	byID    map[string]int // id -> index
}

// ByID 按规则 ID 查找。
func (rs *RuleSet) ByID(id string) (Rule, bool) {
	i, ok := rs.byID[id]
	if !ok {
		return Rule{}, false
	}
	return rs.Rules[i], true
}
