package engine

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/event-correlation-engine-go/rules"
)

// Config 引擎配置。
type Config struct {
	AllowedLateness time.Duration // 允许的迟到范围
	MaxKeys         int           // 内存上限：最多维护的关联键数量
	BufferSize      int           // 异步输入缓冲容量（<=0 时默认 1024）
	Workers         int           // 异步处理并发度（<=0 时关闭异步管线，仅用同步 Process）
}

// Engine 流式事件关联引擎。
type Engine struct {
	mu        sync.Mutex
	cfg       Config
	rules     atomicRuleSet
	wm        *watermarkTracker
	states    map[string]*keyState // key -> 关联状态
	lru       *lruList
	alerts    []Alert
	lates     []LateEvent
	evictions []EvictionReport

	// 异步输入管线（Workers > 0 时启用）
	in      chan Event
	pMu     sync.RWMutex // 保护关闭与发送的互斥：Close 持写锁关闭通道
	pClosed bool
	wg      sync.WaitGroup
}

// New 创建引擎。
func New(cfg Config, rs *rules.RuleSet) (*Engine, error) {
	if rs == nil {
		return nil, errors.New("ruleset must not be nil")
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = 1024
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	e := &Engine{
		cfg:    cfg,
		wm:     newWatermarkTracker(cfg.AllowedLateness),
		states: make(map[string]*keyState),
		lru:    newLRUList(),
	}
	e.rules.Store(rs)
	if cfg.Workers > 0 {
		e.startPipeline()
	}
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
		// 状态键包含规则集版本：不同版本开启的同名窗口各自独立累计，
		// 运行中切换版本不会把新旧规则混算进同一窗口。
		stateKey := fmt.Sprintf("v%d|%s|%s", rs.Version, r.ID, key)
		ks := e.getOrCreateState(stateKey, r, rs.Version, key)
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
// 创建时把当版规则的窗口长度、阈值、版本固化进状态，后续热更新不改写。
// 调用方必须持有 e.mu。
func (e *Engine) getOrCreateState(stateKey string, r *rules.Rule, version int, key string) *keyState {
	if ks, ok := e.states[stateKey]; ok {
		e.lru.touch(ks)
		return ks
	}
	ks := newKeyState(stateKey, r.ID, version, key, r.Window.Nanoseconds(), r.Threshold)
	e.states[stateKey] = ks
	e.lru.touch(ks)
	// 有界并发：超出上限时淘汰最久未使用的状态。
	// 告警在计数达到阈值时已即时触发，因此淘汰不会丢失已确认结果；
	// 但未结算的窗口会以淘汰报告形式暴露，绝不悄悄丢弃。
	for len(e.states) > e.cfg.MaxKeys {
		victim := e.lru.evict()
		if victim == nil {
			break
		}
		delete(e.states, victim.stateKey)
		e.recordEviction(victim)
	}
	return ks
}

// gc 清理已被水位确认的窗口与空键状态。调用方必须持有 e.mu。
// 窗口长度取自状态创建时固化的元数据，与当前规则集版本无关，
// 因此旧版本开启的窗口仍按当版窗口长度确认。
func (e *Engine) gc(wm time.Time) {
	if wm.IsZero() {
		return
	}
	wmNanos := wm.UnixNano()
	for key, ks := range e.states {
		ks.gcBefore(wmNanos, ks.windowNanos)
		if len(ks.windows) == 0 {
			delete(e.states, key)
			e.lru.remove(ks)
		}
	}
}

// recordEviction 为被 LRU 淘汰的键状态生成淘汰报告（仅当存在未结算窗口时）。
// 调用方必须持有 e.mu。
func (e *Engine) recordEviction(ks *keyState) {
	pending := ks.unsettled()
	if len(pending) == 0 {
		return // 没有未结算内容，淘汰无损失，不产生报告
	}
	dropped := 0
	for _, ws := range pending {
		dropped += ws.count
	}
	e.evictions = append(e.evictions, EvictionReport{
		RuleID:         ks.ruleID,
		RuleVersion:    ks.ruleVersion,
		Key:            ks.groupKey,
		PendingWindows: len(pending),
		DroppedEvents:  dropped,
		Reason: fmt.Sprintf("state for rule %q (ruleset v%d) key=%q evicted: MaxKeys=%d exceeded; %d unsettled window(s) with %d event(s) discarded before settlement",
			ks.ruleID, ks.ruleVersion, ks.groupKey, e.cfg.MaxKeys, len(pending), dropped),
	})
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

// Evictions 返回关联状态淘汰报告快照。
func (e *Engine) Evictions() []EvictionReport {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]EvictionReport, len(e.evictions))
	copy(out, e.evictions)
	return out
}
