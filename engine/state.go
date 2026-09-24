package engine

import (
	"container/list"
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
type windowState struct {
	count   int
	alerted bool                // 是否已触发告警（每窗口最多一次）
	seen    map[string]struct{} // 事件 ID 去重，防止重复计数
}

// keyState 单个关联键（ruleID|groupKey）的窗口状态集合。
type keyState struct {
	stateKey string
	windows  map[int64]*windowState // windowStart.UnixNano() -> 状态
	elem     *list.Element          // 在 LRU 链表中的位置
}

func newKeyState(stateKey string) *keyState {
	return &keyState{stateKey: stateKey, windows: make(map[int64]*windowState)}
}

// add 在指定窗口中加入一个事件，返回 (新增计数, 是否重复)。
func (ks *keyState) add(windowStart int64, eventID string) (int, bool) {
	ws, ok := ks.windows[windowStart]
	if !ok {
		ws = &windowState{seen: make(map[string]struct{})}
		ks.windows[windowStart] = ws
	}
	if _, dup := ws.seen[eventID]; dup {
		return ws.count, true
	}
	ws.seen[eventID] = struct{}{}
	ws.count++
	return ws.count, false
}

// alerted 查询/设置窗口告警标记。
func (ks *keyState) alerted(windowStart int64) bool {
	if ws, ok := ks.windows[windowStart]; ok {
		return ws.alerted
	}
	return false
}

func (ks *keyState) markAlerted(windowStart int64) {
	if ws, ok := ks.windows[windowStart]; ok {
		ws.alerted = true
	}
}

// gcBefore 清理窗口结束时间 <= watermark 的已确认窗口。
func (ks *keyState) gcBefore(watermark int64, windowNanos int64) {
	for start := range ks.windows {
		if start+windowNanos <= watermark {
			delete(ks.windows, start)
		}
	}
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
