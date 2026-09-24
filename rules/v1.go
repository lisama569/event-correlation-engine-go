package rules

import "fmt"

// ruleV1 为版本 1 的规则传输形态。
// 废弃字段 "window"（秒）按 1:1 映射到 window_seconds 继续生效。
type ruleV1 struct {
	ID            string         `json:"id"`
	EventType     string         `json:"event_type"`
	KeyField      string         `json:"key_field"`
	WindowSeconds int64          `json:"window_seconds"`
	Window        int64          `json:"window"` // 废弃：等价于 window_seconds
	Threshold     int            `json:"threshold"`
	Severity      string         `json:"severity"`
	Match         []conditionDTO `json:"match"`
}

type setV1 struct {
	Version int      `json:"version"`
	Rules   []ruleV1 `json:"rules"`
}

func loadV1(data []byte) (*Set, error) {
	var s setV1
	if err := strictDecode(data, &s); err != nil {
		return nil, err
	}
	rs := make([]Rule, 0, len(s.Rules))
	for _, rv := range s.Rules {
		conds, err := compileConditions(rv.ID, rv.Match)
		if err != nil {
			return nil, err
		}
		win := rv.WindowSeconds
		if rv.Window != 0 {
			if win != 0 && win != rv.Window {
				return nil, &LoadError{Kind: ErrDeprecatedField, RuleID: rv.ID, Field: "window",
					Msg: fmt.Sprintf("rules: rule %q: deprecated field \"window\" (%d) conflicts with window_seconds (%d)", rv.ID, rv.Window, win)}
			}
			win = rv.Window
		}
		r := Rule{
			ID:            rv.ID,
			EventType:     rv.EventType,
			KeyField:      rv.KeyField,
			WindowSeconds: win,
			Threshold:     rv.Threshold,
			Severity:      rv.Severity,
			Match:         conds,
		}
		applyDefaults(&r)
		rs = append(rs, r)
	}
	return validateSet(1, rs)
}

// applyDefaults 应用与加载顺序无关的稳定默认值。
func applyDefaults(r *Rule) {
	if r.KeyField == "" {
		r.KeyField = DefaultKeyField
	}
	if r.Threshold == 0 {
		r.Threshold = DefaultThreshold
	}
	if r.Severity == "" {
		r.Severity = DefaultSeverity
	}
}
