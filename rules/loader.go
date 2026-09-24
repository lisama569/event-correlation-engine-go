package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// LoadErrorKind 区分不同类型的加载错误。
type LoadErrorKind int

const (
	ErrUnknownField    LoadErrorKind = iota + 1 // 未知字段
	ErrBadVersion                               // 不支持的版本
	ErrInvalidRef                               // 非法引用（如重复 ID、空 ID）
	ErrIncomplete                               // 不完整条件（缺字段、非法 op、非法 window 等）
	ErrDeprecatedField                          // 废弃字段用法冲突，被明确拒绝
)

// LoadError 可区分的规则加载错误。
type LoadError struct {
	Kind   LoadErrorKind
	Detail string
}

func (e *LoadError) Error() string {
	return fmt.Sprintf("rules load error(kind=%d): %s", e.Kind, e.Detail)
}

// 稳定默认值：不随加载顺序变化。
const (
	DefaultSeverity = "info"
	DefaultGroupBy  = "" // 空表示全局单键关联
)

var validOps = map[string]bool{
	"eq": true, "ne": true, "gt": true, "lt": true, "contains": true,
}

type envelope struct {
	Version int    `json:"version"`
	Rules   []Rule `json:"rules"`
}

// Load 解析并校验规则定义，按 version 字段做版本兼容处理。
// 未知字段、非法引用、不完整条件都会返回可区分的 *LoadError，绝不静默忽略。
func Load(data []byte) (*RuleSet, error) {
	var env envelope
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // 未知字段一律报错
	if err := dec.Decode(&env); err != nil {
		if strings.Contains(err.Error(), "unknown field") {
			return nil, &LoadError{Kind: ErrUnknownField, Detail: err.Error()}
		}
		return nil, &LoadError{Kind: ErrIncomplete, Detail: "malformed json: " + err.Error()}
	}

	switch env.Version {
	case VersionV1, VersionV2:
		// 支持的版本
	default:
		return nil, &LoadError{Kind: ErrBadVersion, Detail: fmt.Sprintf("unsupported version %d (want %d or %d)", env.Version, VersionV1, VersionV2)}
	}

	if len(env.Rules) == 0 {
		return nil, &LoadError{Kind: ErrIncomplete, Detail: "rules must not be empty"}
	}

	rs := &RuleSet{Version: env.Version, byID: make(map[string]int, len(env.Rules))}
	for i := range env.Rules {
		r := env.Rules[i]
		if err := validateAndNormalize(&r, env.Version); err != nil {
			return nil, err
		}
		if _, dup := rs.byID[r.ID]; dup {
			return nil, &LoadError{Kind: ErrInvalidRef, Detail: "duplicate rule id: " + r.ID}
		}
		rs.byID[r.ID] = len(rs.Rules)
		rs.Rules = append(rs.Rules, r)
	}
	return rs, nil
}

// validateAndNormalize 校验单条规则并应用版本兼容映射与稳定默认值。
func validateAndNormalize(r *Rule, version int) error {
	if r.ID == "" {
		return &LoadError{Kind: ErrInvalidRef, Detail: "rule id must not be empty"}
	}

	// 废弃字段 field 的明确映射：v1 中 field 即分组键；v2 中映射到 group_by。
	if r.LegacyField != "" {
		if r.GroupBy != "" && r.GroupBy != r.LegacyField {
			return &LoadError{Kind: ErrDeprecatedField,
				Detail: fmt.Sprintf("rule %s: deprecated 'field' (%q) conflicts with 'group_by' (%q)", r.ID, r.LegacyField, r.GroupBy)}
		}
		r.GroupBy = r.LegacyField
		r.LegacyField = ""
	}

	// 新增可选字段的稳定默认值。
	if r.Severity == "" {
		r.Severity = DefaultSeverity
	}

	// 条件完整性校验。
	if len(r.Match) == 0 {
		return &LoadError{Kind: ErrIncomplete, Detail: "rule " + r.ID + ": match must not be empty"}
	}
	for j, c := range r.Match {
		if c.Field == "" {
			return &LoadError{Kind: ErrIncomplete, Detail: fmt.Sprintf("rule %s: match[%d] missing field", r.ID, j)}
		}
		if !validOps[c.Op] {
			return &LoadError{Kind: ErrIncomplete, Detail: fmt.Sprintf("rule %s: match[%d] invalid op %q", r.ID, j, c.Op)}
		}
	}

	// 窗口与阈值校验。
	if r.WindowRaw == "" {
		return &LoadError{Kind: ErrIncomplete, Detail: "rule " + r.ID + ": window must not be empty"}
	}
	w, err := time.ParseDuration(r.WindowRaw)
	if err != nil || w <= 0 {
		return &LoadError{Kind: ErrIncomplete, Detail: fmt.Sprintf("rule %s: invalid window %q", r.ID, r.WindowRaw)}
	}
	r.Window = w
	if r.Threshold <= 0 {
		return &LoadError{Kind: ErrIncomplete, Detail: fmt.Sprintf("rule %s: threshold must be > 0, got %d", r.ID, r.Threshold)}
	}
	return nil
}
