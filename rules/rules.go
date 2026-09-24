// Package rules 负责多版本规则集的加载、校验与兼容处理。
//
// 兼容策略：
//   - version 为必填字段，目前支持 1 与 2；其它版本返回 ErrUnsupportedVersion。
//   - 所有版本均拒绝未知字段（ErrUnknownField），不做静默忽略。
//   - v1 中废弃字段 "window" 按 1:1 映射到 window_seconds 继续生效；
//     若与 window_seconds 同时出现且取值冲突，返回 ErrDeprecatedField。
//   - v2 中 "window" 被明确拒绝（ErrDeprecatedField），必须使用 window_seconds。
//   - 可选字段的默认值与加载顺序无关：key_field 默认 "source"，
//     threshold 默认 1，severity 默认 "medium"，match 默认为空。
package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// 稳定默认值，不随加载顺序变化。
const (
	DefaultKeyField  = "source"
	DefaultThreshold = 1
	DefaultSeverity  = "medium"
)

// Rule 为编译后的单条关联规则（与版本无关的统一内部形态）。
type Rule struct {
	ID            string
	EventType     string
	KeyField      string
	WindowSeconds int64
	Threshold     int
	Severity      string
	Description   string
	Match         []Condition
}

// Condition 表示一个字段等值匹配条件。
type Condition struct {
	Field  string
	Equals string
}

// Set 为一次完整加载的规则集快照，热更新时整体替换。
type Set struct {
	Version int
	Rules   []Rule
}

// Load 解析并校验规则定义，根据 version 字段分派到对应版本的解析器。
func Load(data []byte) (*Set, error) {
	var head struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, &LoadError{Kind: ErrMalformed, Msg: "rules: invalid JSON: " + err.Error()}
	}
	switch head.Version {
	case 1:
		return loadV1(data)
	case 2:
		return loadV2(data)
	default:
		return nil, &LoadError{Kind: ErrUnsupportedVersion, Field: "version",
			Msg: fmt.Sprintf("rules: unsupported version %d (supported: 1, 2)", head.Version)}
	}
}

// strictDecode 解码 JSON 并拒绝未知字段。
func strictDecode(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return &LoadError{Kind: ErrUnknownField, Msg: "rules: " + err.Error()}
	}
	return nil
}

// conditionDTO 为 match 条件的传输形态。
type conditionDTO struct {
	Field  string `json:"field"`
	Equals string `json:"equals"`
}

func compileConditions(ruleID string, dtos []conditionDTO) ([]Condition, error) {
	conds := make([]Condition, 0, len(dtos))
	for i, c := range dtos {
		if c.Field == "" || c.Equals == "" {
			return nil, &LoadError{Kind: ErrIncompleteCondition, RuleID: ruleID, Field: "match",
				Msg: fmt.Sprintf("rules: rule %q: match[%d] requires non-empty field and equals", ruleID, i)}
		}
		conds = append(conds, Condition{Field: c.Field, Equals: c.Equals})
	}
	return conds, nil
}

// validateSet 做与版本无关的公共校验：必填项、取值范围、重复 ID。
func validateSet(version int, rs []Rule) (*Set, error) {
	if len(rs) == 0 {
		return nil, &LoadError{Kind: ErrInvalidRule, Msg: "rules: at least one rule is required"}
	}
	seen := make(map[string]struct{}, len(rs))
	for _, r := range rs {
		if r.ID == "" || r.EventType == "" {
			return nil, &LoadError{Kind: ErrInvalidRule, RuleID: r.ID,
				Msg: fmt.Sprintf("rules: rule %q: id and event_type are required", r.ID)}
		}
		if _, dup := seen[r.ID]; dup {
			return nil, &LoadError{Kind: ErrInvalidRule, RuleID: r.ID,
				Msg: fmt.Sprintf("rules: duplicate rule id %q", r.ID)}
		}
		seen[r.ID] = struct{}{}
		if r.WindowSeconds <= 0 {
			return nil, &LoadError{Kind: ErrInvalidRule, RuleID: r.ID, Field: "window_seconds",
				Msg: fmt.Sprintf("rules: rule %q: window_seconds must be > 0", r.ID)}
		}
		if r.Threshold <= 0 {
			return nil, &LoadError{Kind: ErrInvalidRule, RuleID: r.ID, Field: "threshold",
				Msg: fmt.Sprintf("rules: rule %q: threshold must be > 0", r.ID)}
		}
	}
	// 按 ID 排序，保证规则评估顺序与加载顺序无关。
	sort.Slice(rs, func(i, j int) bool { return rs[i].ID < rs[j].ID })
	return &Set{Version: version, Rules: rs}, nil
}
