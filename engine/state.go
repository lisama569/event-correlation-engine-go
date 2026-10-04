package engine

import (
	"container/list"
	"strings"
	"sync/atomic"

	"github.com/lisama569/event-correlation-engine-go/rules"
)

// atomicRuleSet 规则集的原子指针，保证单次判定看到完整一致的版本。
type atomicRuleSet struct {
	v atomic.Pointer[rules.RuleSet]
}

func (a *atomicRuleSet) Store(rs *rules.RuleSet) { a.v.Store(rs) }
func (a *atomicRuleSet) Load() *rules.RuleSet    { return a.v.Load() }

// windowState 单个窗口的计数状态。
// 窗口首次创建时，其规则版本、阈值、窗口长度即被固定下来；
// 后续规则热更新不会改动这些值，保证同一窗口始终按它开启时那一版规则判定。
type windowState struct {
	start       int64 // windowStart.UnixNano()
	count       int
	alerted     bool                // 是否已触发告警（每窗口最多一次）
	seen        map[string]struct{} // 事件 ID 去重，防止重复计数
	ruleVersion int                 // 固定：开启窗口时的规则集版本
	threshold   int                 // 固定：开启窗口时那一版规则的阈值
	windowNanos int64               // 固定：开启窗口时那一版规则的窗口长度
}

// keyState 单个关联键（ruleID|groupKey）的窗口状态集合。
type keyState struct {
	stateKey string
	ruleID   string                 // stateKey 中 '|' 之前的部分，便于生成报告
	groupKey string                 // stateKey 中 '|' 之后的部分
	windows  map[int64]*windowState // windowStart.UnixNano() -> 状态
	elem     *list.Element          // 在 LRU 链表中的位置
}

func newKeyState(stateKey string) *keyState {
	ruleID, groupKey, _ := strings.Cut(stateKey, "|")
	return &keyState{
		stateKey: stateKey,
		ruleID:   ruleID,
		groupKey: groupKey,
		windows:  make(map[int64]*windowState),
	}
}

// ensure 返回指定窗口的状态。窗口首次创建时，按传入的规则版本固定阈值与窗口长度；
// 已存在的窗口原样返回，固定值不被后续版本覆盖。
func (ks *keyState) ensure(windowStart int64, version, threshold int, windowNanos int64) *windowState {
	ws, ok := ks.windows[windowStart]
	if !ok {
		ws = &windowState{
			start:       windowStart,
			seen:        make(map[string]struct{}),
			ruleVersion: version,
			threshold:   threshold,
			windowNanos: windowNanos,
		}
		ks.windows[windowStart] = ws
	}
	return ws
}

// addEvent 在窗口中加入一个事件，返回 (新增计数, 是否重复)。
func (ws *windowState) addEvent(eventID string) (int, bool) {
	if _, dup := ws.seen[eventID]; dup {
		return ws.count, true
	}
	ws.seen[eventID] = struct{}{}
	ws.count++
	return ws.count, false
}

// gcSettled 清理结束时间 <= watermark 的已确认窗口（按每个窗口各自固定的窗口长度判定）。
// 返回被清理掉的窗口，由上层决定是否需要暴露为淘汰报告。
func (ks *keyState) gcSettled(watermark int64) []*windowState {
	var removed []*windowState
	for start, ws := range ks.windows {
		if start+ws.windowNanos <= watermark {
			delete(ks.windows, start)
			removed = append(removed, ws)
		}
	}
	return removed
}

// lruList 基于 container/list 的键 LRU，front 为最近使用。
type lruList struct {
	l *list.List
}

func newLRUList() *lruList { return &lruList{l: list.New()} }

func (l *lruList) touch(ks *keyState) {
	if ks.elem != nil {
		l.l.MoveToFront(ks.elem)
	} else {
		ks.elem = l.l.PushFront(ks)
	}
}

// evict 移除最久未使用的键状态，返回被移除者；空链表返回 nil。
func (l *lruList) evict() *keyState {
	back := l.l.Back()
	if back == nil {
		return nil
	}
	ks := back.Value.(*keyState)
	l.l.Remove(back)
	ks.elem = nil
	return ks
}

func (l *lruList) remove(ks *keyState) {
	if ks.elem != nil {
		l.l.Remove(ks.elem)
		ks.elem = nil
	}
}
