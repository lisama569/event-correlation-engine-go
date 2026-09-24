package rules

import "fmt"

// ruleV2 为版本 2 的规则传输形态。
// 相对 v1 新增可选字段 description 与 filter（引用规则集级命名过滤器）；
// 废弃字段 "window" 在 v2 中被明确拒绝。
type ruleV2 struct {
	ID            string         `json:"id"`
	EventType     string         `json:"event_type"`
	KeyField      string         `json:"key_field"`
	WindowSeconds int64          `json:"window_seconds"`
	Window        int64          `json:"window"` // 已废弃：v2 中拒绝
	Threshold     int            `json:"threshold"`
	Severity      string         `json:"severity"`
	Description   string         `json:"description"`
	Filter        string         `json:"filter"`
	Match         []conditionDTO `json:"match"`
}

type setV2 struct {
	Version int                       `json:"version"`
	Rules   []ruleV2                  `json:"rules"`
	Filters map[string][]conditionDTO `json:"filters"`
}

func loadV2(data []byte) (*Set, error) {
	var s setV2
	if err := strictDecode(data, &s); err != nil {
		return nil, err
	}
	// 先编译命名过滤器，供规则引用。
	filters := make(map[string][]Condition, len(s.Filters))
	for name, dtos := range s.Filters {
		if name == "" {
			return nil, &LoadError{Kind: ErrInvalidReference, Field: "filters",
				Msg: "rules: filter name must be non-empty"}
		}
		conds, err := compileConditions("filter:"+name, dtos)
		if err != nil {
			return nil, err
		}
		filters[name] = conds
	}
	rs := make([]Rule, 0, len(s.Rules))
	for _, rv := range s.Rules {
		if rv.Window != 0 {
			return nil, &LoadError{Kind: ErrDeprecatedField, RuleID: rv.ID, Field: "window",
				Msg: fmt.Sprintf("rules: rule %q: field \"window\" was removed in version 2, use window_seconds", rv.ID)}
		}
		conds, err := compileConditions(rv.ID, rv.Match)
		if err != nil {
			return nil, err
		}
		if rv.Filter != "" {
			fc, ok := filters[rv.Filter]
			if !ok {
				return nil, &LoadError{Kind: ErrInvalidReference, RuleID: rv.ID, Field: "filter",
					Msg: fmt.Sprintf("rules: rule %q: filter %q is not defined", rv.ID, rv.Filter)}
			}
			conds = append(conds, fc...)
		}
		r := Rule{
			ID:            rv.ID,
			EventType:     rv.EventType,
			KeyField:      rv.KeyField,
			WindowSeconds: rv.WindowSeconds,
			Threshold:     rv.Threshold,
			Severity:      rv.Severity,
			Description:   rv.Description,
			Match:         conds,
		}
		applyDefaults(&r)
		rs = append(rs, r)
	}
	return validateSet(2, rs)
}
