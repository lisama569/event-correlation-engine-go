package engine

import (
	"strconv"
	"strings"

	"github.com/lisama569/event-correlation-engine-go/rules"
)

// matchAll 判断事件是否满足规则的全部条件。
func matchAll(r rules.Rule, ev Event) bool {
	for _, c := range r.Match {
		if !matchCond(c, ev.Fields[c.Field]) {
			return false
		}
	}
	return true
}

func matchCond(c rules.Condition, val string) bool {
	switch c.Op {
	case "eq":
		return val == c.Value
	case "ne":
		return val != c.Value
	case "contains":
		return strings.Contains(val, c.Value)
	case "gt", "lt":
		// 优先数值比较，失败则按字符串序比较。
		if fv, err1 := strconv.ParseFloat(val, 64); err1 == nil {
			if cv, err2 := strconv.ParseFloat(c.Value, 64); err2 == nil {
				if c.Op == "gt" {
					return fv > cv
				}
				return fv < cv
			}
		}
		if c.Op == "gt" {
			return val > c.Value
		}
		return val < c.Value
	default:
		return false
	}
}

// groupKey 计算关联键：group_by 为空时为全局单键。
func groupKey(r rules.Rule, ev Event) string {
	if r.GroupBy == "" {
		return "global"
	}
	if v, ok := ev.Fields[r.GroupBy]; ok {
		return v
	}
	return "<none>"
}
