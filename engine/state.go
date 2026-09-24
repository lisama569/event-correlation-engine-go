package engine

import "container/list"

// windowKey 唯一标识一个开窗：规则集版本 + 规则 ID + 窗口起点（纳秒）。
// 版本参与键的构成，保证热更新前后打开的窗口互不混合。
type windowKey struct {
	version int
	ruleID  string
	start   int64
}

// windowState 为一个未闭合窗口的聚合状态。
// 阈值、级别与规则集版本在开窗时快照，保证判定依据可解释且不受后续热更新影响。
type windowState struct {
	end         int64               // 窗口终点（纳秒，左闭右开）
	threshold   int                 // 开窗时规则快照的阈值
	severity    string              // 开窗时规则快照的级别
	ruleVersion int                 // 开窗时的规则集版本
	ids         map[string]struct{} // 事件 ID 去重集合，保证迟到事件不重复计数
}

// keyState 为一个关联键下所有未闭合窗口。
type keyState struct {
	key     string
	windows map[windowKey]*windowState
}

// stateStore 按键维护关联状态，容量有界，超出时按 LRU 淘汰。
// 仅由单个 worker goroutine 访问，无需加锁。
type stateStore struct {
	max   int
	ll    *list.List               // front 为最近使用，元素为 *keyState
	items map[string]*list.Element // key -> list 元素
}

func newStateStore(max int) *stateStore {
	if max <= 0 {
		max = 1024
	}
	return &stateStore{max: max, ll: list.New(), items: make(map[string]*list.Element)}
}

// getOrCreate 返回键对应的状态；若容量超限，淘汰最久未使用的键并返回其状态，
// 调用方负责将被淘汰键的未闭合窗口强制结算，保证结果不缺失。
func (s *stateStore) getOrCreate(key string) (ks *keyState, evicted *keyState) {
	if el, ok := s.items[key]; ok {
		s.ll.MoveToFront(el)
		return el.Value.(*keyState), nil
	}
	ks = &keyState{key: key, windows: make(map[windowKey]*windowState)}
	el := s.ll.PushFront(ks)
	s.items[key] = el
	if s.ll.Len() > s.max {
		back := s.ll.Back()
		if back != nil && back != el {
			evicted = back.Value.(*keyState)
			s.remove(evicted.key)
		}
	}
	return ks, evicted
}

func (s *stateStore) remove(key string) {
	if el, ok := s.items[key]; ok {
		s.ll.Remove(el)
		delete(s.items, key)
	}
}

// forEach 遍历所有键状态（顺序不影响正确性，仅用于窗口闭合扫描）。
func (s *stateStore) forEach(fn func(*keyState)) {
	for el := s.ll.Front(); el != nil; el = el.Next() {
		fn(el.Value.(*keyState))
	}
}
