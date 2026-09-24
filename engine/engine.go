package engine

import (
	"errors"
	"fmt"
	"strings"
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
	mu     sync.Mutex
	cfg    Config
	rules  atomicRuleSet
	wm     *watermarkTracker
	states map[string]*keyState // key -> 关联状态
	lru    *lruList
	alerts []Alert
	lates  []LateEvent
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

// Process 处理一条事件，返回本次新产生的告警；超范围迟到事件被记录并可通过 LateEvents 获取。
// 单次判定只读取一次规则集指针，保证看到某一完整版本，不会新旧混合。
func (e *Engine) Process(ev Event) []Alert {
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

	rs := e.rules.Load() // 本次判定的完整规则集快照
	var fired []Alert
	for i := range rs.Rules {
		r := &rs.Rules[i]
		if !matchAll(*r, ev) {
			continue
		}
		key := groupKey(*r, ev)
		stateKey := r.ID + "|" + key
		ks := e.getOrCreateState(stateKey)
		ws := ev.Timestamp.Truncate(r.Window).UnixNano()
		count, dup := ks.add(ws, ev.ID)
		if dup {
			continue // 去重：同一事件不重复计数
		}
		if count >= r.Threshold && !ks.alerted(ws) {
			ks.markAlerted(ws)
			start := ev.Timestamp.Truncate(r.Window)
			a := Alert{
				RuleID:      r.ID,
				RuleVersion: rs.Version,
				Key:         key,
				WindowStart: start,
				WindowEnd:   start.Add(r.Window),
				Count:       count,
				Reason: fmt.Sprintf("rule %q (ruleset v%d): count=%d >= threshold=%d in window [%s,%s) key=%q",
					r.ID, rs.Version, count, r.Threshold, start.Format(time.RFC3339), start.Add(r.Window).Format(time.RFC3339), key),
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
	// 有界并发：超出上限时淘汰最久未使用的状态。
	// 告警在计数达到阈值时已即时触发，因此淘汰不会丢失已确认结果。
	for len(e.states) > e.cfg.MaxKeys {
		victim := e.lru.evict()
		if victim == nil {
			break
		}
		delete(e.states, victim.stateKey)
	}
	return ks
}

// gc 清理已被水位确认的窗口与空键状态。调用方必须持有 e.mu。
func (e *Engine) gc(wm time.Time) {
	if wm.IsZero() {
		return
	}
	wmNanos := wm.UnixNano()
	rs := e.rules.Load()
	windows := make(map[string]int64, len(rs.Rules))
	for _, r := range rs.Rules {
		windows[r.ID] = r.Window.Nanoseconds()
	}
	for key, ks := range e.states {
		ruleID, _, _ := strings.Cut(key, "|")
		w, ok := windows[ruleID]
		if !ok {
			// 规则已不在当前版本中：窗口长度未知，保守地按已确认清理交给淘汰机制。
			continue
		}
		ks.gcBefore(wmNanos, w)
		if len(ks.windows) == 0 {
			delete(e.states, key)
			e.lru.remove(ks)
		}
	}
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
