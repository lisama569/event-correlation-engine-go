package engine

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lisama569/event-correlation-engine-go/rules"
)

// Config 引擎配置。
type Config struct {
	AllowedLateness time.Duration // 允许的迟到范围
	MaxKeys         int           // 内存上限：最多维护的关联键数量
}

// Engine 流式事件关联引擎。
type Engine struct {
	mu          sync.Mutex
	cfg         Config
	rules       atomicRuleSet
	wm          *watermarkTracker
	states      map[string]*keyState // key -> 关联状态
	lru         *lruList
	alerts      []Alert
	lates       []LateEvent
	evictions   []Eviction   // 被内存上限淘汰的未结算窗口报告
	settlements []Settlement // 主动收尾给出的最终判定（留存审计）
}

// matchedRule 一条事件对某条规则的锁外匹配结果（规则在规则集快照中的下标）。
type matchedRule struct {
	index int
	key   string
}

// New 创建引擎。
func New(cfg Config, rs *rules.RuleSet) (*Engine, error) {
	if rs == nil {
		return nil, errors.New("ruleset must not be nil")
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = 1024
	}
	e := &Engine{
		cfg:    cfg,
		wm:     newWatermarkTracker(cfg.AllowedLateness),
		states: make(map[string]*keyState),
		lru:    newLRUList(),
	}
	e.rules.Store(rs)
	return e, nil
}

// Reload 热更新规则集：单次判定只能看到某一完整版本。
func (e *Engine) Reload(rs *rules.RuleSet) error {
	if rs == nil {
		return errors.New("ruleset must not be nil")
	}
	e.rules.Store(rs)
	return nil
}

// RulesVersion 当前规则集版本。
func (e *Engine) RulesVersion() int {
	return e.rules.Load().Version
}

// Match 在锁外完成纯计算：读取一次规则集指针作为本次判定的完整版本快照，
// 对每条规则做条件匹配与分组键计算。可与其他事件的 Match 并发执行。
func (e *Engine) Match(ev Event) (*rules.RuleSet, []matchedRule) {
	rs := e.rules.Load()
	matched := make([]matchedRule, 0, len(rs.Rules))
	for i := range rs.Rules {
		r := &rs.Rules[i]
		if matchAll(*r, ev) {
			matched = append(matched, matchedRule{index: i, key: groupKey(*r, ev)})
		}
	}
	return rs, matched
}

// Process 处理一条事件，返回本次新产生的告警；超范围迟到事件被记录并可通过 LateEvents 获取。
// 单次判定只读取一次规则集指针（在 Match 中），保证看到某一完整版本，不会新旧混合。
func (e *Engine) Process(ev Event) []Alert {
	rs, matched := e.Match(ev)
	return e.Apply(ev, rs, matched)
}

// Apply 在互斥锁下应用一次已完成匹配的判定：迟到判定、水位推进、窗口计数与告警。
// 同一窗口开启后，其版本、阈值、窗口长度即固定，Apply 只通过 ensure 复用这些固定值。
func (e *Engine) Apply(ev Event, rs *rules.RuleSet, matched []matchedRule) []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 超范围迟到：明确区分并单独报告，不推进水位、不污染状态。
	if e.wm.isBeyondLateness(ev.Timestamp) {
		e.lates = append(e.lates, LateEvent{
			Event:     ev,
			Watermark: e.wm.watermark().UnixNano(),
			Reason:    "event time at or before watermark; beyond allowed lateness",
		})
		return nil
	}
	wm := e.wm.observe(ev.Timestamp)

	var fired []Alert
	for _, m := range matched {
		r := &rs.Rules[m.index]
		stateKey := r.ID + "|" + m.key
		ks := e.getOrCreateState(stateKey)
		wsStart := ev.Timestamp.Truncate(r.Window).UnixNano()
		// 已存在的窗口沿用它开启时固定的版本/阈值/窗口长度，不被本次规则集覆盖。
		ws := ks.ensure(wsStart, rs.Version, r.Threshold, r.Window.Nanoseconds())
		count, dup := ws.addEvent(ev.ID)
		if dup {
			continue // 去重：同一事件不重复计数
		}
		if count >= ws.threshold && !ws.alerted {
			ws.alerted = true
			start := time.Unix(0, ws.start)
			a := Alert{
				RuleID:      r.ID,
				RuleVersion: ws.ruleVersion,
				Key:         m.key,
				WindowStart: start,
				WindowEnd:   start.Add(time.Duration(ws.windowNanos)),
				Count:       count,
				Reason: fmt.Sprintf("rule %q (ruleset v%d): count=%d >= threshold=%d in window [%s,%s) key=%q",
					r.ID, ws.ruleVersion, count, ws.threshold,
					start.Format(time.RFC3339), start.Add(time.Duration(ws.windowNanos)).Format(time.RFC3339), m.key),
			}
			fired = append(fired, a)
			e.alerts = append(e.alerts, a)
		}
	}

	e.gc(wm)
	return fired
}

// getOrCreateState 获取或创建键状态，并维护 LRU 与内存上限。
// 调用方必须持有 e.mu。
func (e *Engine) getOrCreateState(stateKey string) *keyState {
	if ks, ok := e.states[stateKey]; ok {
		e.lru.touch(ks)
		return ks
	}
	ks := newKeyState(stateKey)
	e.states[stateKey] = ks
	e.lru.touch(ks)
	// 有界状态：超出上限时淘汰最久未使用的键状态。
	// 被淘汰键中尚未结算的窗口不会静默消失，而会逐窗出具 Eviction 报告。
	for len(e.states) > e.cfg.MaxKeys {
		victim := e.lru.evict()
		if victim == nil {
			break
		}
		e.reportEvicted(victim)
		delete(e.states, victim.stateKey)
	}
	return ks
}

// reportEvicted 为被淘汰键状态中每个尚未触发告警的在途窗口生成可审计报告。
// 已触发告警的窗口结论已在 alerts 中可查，不再重复报告。调用方必须持有 e.mu。
func (e *Engine) reportEvicted(ks *keyState) {
	for _, ws := range ks.windows {
		if ws.alerted {
			continue
		}
		start := time.Unix(0, ws.start)
		end := start.Add(time.Duration(ws.windowNanos))
		e.evictions = append(e.evictions, Eviction{
			RuleID:      ks.ruleID,
			RuleVersion: ws.ruleVersion,
			Key:         ks.groupKey,
			WindowStart: start,
			WindowEnd:   end,
			Count:       ws.count,
			Threshold:   ws.threshold,
			Reason: fmt.Sprintf("state key %q evicted by LRU under MaxKeys=%d: pending window [%s,%s) (count=%d/%d, ruleset v%d) settled neither by alert nor by watermark",
				ks.stateKey, e.cfg.MaxKeys,
				start.Format(time.RFC3339), end.Format(time.RFC3339),
				ws.count, ws.threshold, ws.ruleVersion),
		})
	}
}

// gc 清理已被水位确认的窗口与空键状态。调用方必须持有 e.mu。
// 窗口结束时间 <= 水位后不可能再有事件进入（再来即超范围迟到），结论已定，安全回收。
func (e *Engine) gc(wm time.Time) {
	if wm.IsZero() {
		return
	}
	wmNanos := wm.UnixNano()
	for key, ks := range e.states {
		ks.gcSettled(wmNanos) // 每个窗口按各自固定的窗口长度判定
		if len(ks.windows) == 0 {
			delete(e.states, key)
			e.lru.remove(ks)
		}
	}
}

// Drain 主动收尾：按每个窗口开启时固定的那一版规则，一次性结算当前所有尚未触发
// 告警的在途窗口，清空在途状态，并返回收尾结论。可在切流、停机或规则版本下线时调用。
func (e *Engine) Drain() []Settlement {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.drainLocked(0, false)
}

// DrainVersion 只收尾固定规则版本 == version 的窗口（用于确认某一版本规则下线），
// 其他版本的在途窗口保持不变。
func (e *Engine) DrainVersion(version int) []Settlement {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.drainLocked(version, true)
}

// drainLocked 执行收尾。调用方必须持有 e.mu。
func (e *Engine) drainLocked(onlyVersion int, filter bool) []Settlement {
	wm := e.wm.watermark()
	var out []Settlement
	for stateKey, ks := range e.states {
		for startNanos, ws := range ks.windows {
			if filter && ws.ruleVersion != onlyVersion {
				continue
			}
			start := time.Unix(0, startNanos)
			end := start.Add(time.Duration(ws.windowNanos))
			fired := ws.count >= ws.threshold
			// 防御性：理论上达到阈值的窗口已即时告警；若收尾时才满足则补发，绝不漏判。
			if fired && !ws.alerted {
				ws.alerted = true
				a := Alert{
					RuleID:      ks.ruleID,
					RuleVersion: ws.ruleVersion,
					Key:         ks.groupKey,
					WindowStart: start,
					WindowEnd:   end,
					Count:       ws.count,
					Reason: fmt.Sprintf("rule %q (ruleset v%d): fired during manual drain, count=%d >= threshold=%d",
						ks.ruleID, ws.ruleVersion, ws.count, ws.threshold),
				}
				e.alerts = append(e.alerts, a)
			}
			s := Settlement{
				RuleID:      ks.ruleID,
				RuleVersion: ws.ruleVersion,
				Key:         ks.groupKey,
				WindowStart: start,
				WindowEnd:   end,
				Count:       ws.count,
				Threshold:   ws.threshold,
				Fired:       fired,
				Reason: fmt.Sprintf("manual drain: pending window [%s,%s) of rule %q (ruleset v%d) settled now rather than on watermark (watermark=%s); count=%d threshold=%d fired=%t",
					start.Format(time.RFC3339), end.Format(time.RFC3339),
					ks.ruleID, ws.ruleVersion, wm.Format(time.RFC3339),
					ws.count, ws.threshold, fired),
			}
			out = append(out, s)
			delete(ks.windows, startNanos)
		}
		if len(ks.windows) == 0 {
			delete(e.states, stateKey)
			e.lru.remove(ks)
		}
	}
	// 稳定排序，便于按时间/版本核对收尾结果。
	sort.Slice(out, func(i, j int) bool {
		if !out[i].WindowStart.Equal(out[j].WindowStart) {
			return out[i].WindowStart.Before(out[j].WindowStart)
		}
		if out[i].RuleID != out[j].RuleID {
			return out[i].RuleID < out[j].RuleID
		}
		return out[i].Key < out[j].Key
	})
	e.settlements = append(e.settlements, out...)
	return out
}

// Alerts 返回累计告警快照。
func (e *Engine) Alerts() []Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Alert, len(e.alerts))
	copy(out, e.alerts)
	return out
}

// LateEvents 返回超范围迟到事件报告快照。
func (e *Engine) LateEvents() []LateEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]LateEvent, len(e.lates))
	copy(out, e.lates)
	return out
}

// Evictions 返回因内存上限被淘汰的未结算窗口报告快照。
func (e *Engine) Evictions() []Eviction {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Eviction, len(e.evictions))
	copy(out, e.evictions)
	return out
}

// Settlements 返回历次主动收尾累计的结算结论快照。
func (e *Engine) Settlements() []Settlement {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Settlement, len(e.settlements))
	copy(out, e.settlements)
	return out
}
