package engine

import (
	"fmt"
	"sort"
	"time"
)

// Settlement 一次主动收尾（Flush）对单个残留窗口的结算结果。
// 判定依据（窗口长度、阈值、计数）全部来自窗口开启时那一版规则，
// 不受后续规则热更新影响。
type Settlement struct {
	RuleID      string    `json:"rule_id"`
	RuleVersion int       `json:"rule_version"` // 窗口开启时使用的规则集版本
	Key         string    `json:"key"`          // 关联键（group_by 值）
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	Count       int       `json:"count"`
	Threshold   int       `json:"threshold"` // 当版规则的阈值
	Fired       bool      `json:"fired"`     // 收尾时是否达到阈值并触发告警
	Reason      string    `json:"reason"`    // 为什么到这一步才结算
}

// Flush 主动收尾：把当前所有还没触发告警的窗口一次性结算完，返回结算结果。
// 每个窗口仍按它当初开启时那一版规则（版本、窗口长度、阈值）判定；
// 达到阈值的窗口补发告警（计入 Alerts），未达到的以未命中结论收尾。
// 结算后所有残留状态被清空，引擎可继续接收新事件（按当前规则版本重新开启窗口）。
// 用于切流、停机或规则版本下线前的干净收尾。
func (e *Engine) Flush() []Settlement {
	e.mu.Lock()
	defer e.mu.Unlock()

	var out []Settlement
	for key, ks := range e.states {
		for start, ws := range ks.unsettled() {
			wsStart := time.Unix(0, start).UTC()
			wsEnd := wsStart.Add(time.Duration(ks.windowNanos))
			s := Settlement{
				RuleID:      ks.ruleID,
				RuleVersion: ks.ruleVersion,
				Key:         ks.groupKey,
				WindowStart: wsStart,
				WindowEnd:   wsEnd,
				Count:       ws.count,
				Threshold:   ks.threshold,
			}
			if ws.count >= ks.threshold {
				s.Fired = true
				s.Reason = fmt.Sprintf("manual flush: settled before watermark confirmation; rule %q (ruleset v%d): count=%d >= threshold=%d in window [%s,%s) key=%q",
					ks.ruleID, ks.ruleVersion, ws.count, ks.threshold,
					wsStart.Format(time.RFC3339), wsEnd.Format(time.RFC3339), ks.groupKey)
				a := Alert{
					RuleID:      ks.ruleID,
					RuleVersion: ks.ruleVersion,
					Key:         ks.groupKey,
					WindowStart: wsStart,
					WindowEnd:   wsEnd,
					Count:       ws.count,
					Reason:      s.Reason,
				}
				e.alerts = append(e.alerts, a)
			} else {
				s.Reason = fmt.Sprintf("manual flush: settled before watermark confirmation; rule %q (ruleset v%d): count=%d < threshold=%d in window [%s,%s) key=%q, no alert",
					ks.ruleID, ks.ruleVersion, ws.count, ks.threshold,
					wsStart.Format(time.RFC3339), wsEnd.Format(time.RFC3339), ks.groupKey)
			}
			out = append(out, s)
		}
		// 已结算（含此前已告警）的窗口一并清空。
		delete(e.states, key)
		e.lru.remove(ks)
	}

	// 输出排序稳定：按版本、规则、键、窗口起点，便于消费方比对。
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.RuleVersion != b.RuleVersion {
			return a.RuleVersion < b.RuleVersion
		}
		if a.RuleID != b.RuleID {
			return a.RuleID < b.RuleID
		}
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return a.WindowStart.Before(b.WindowStart)
	})
	return out
}
