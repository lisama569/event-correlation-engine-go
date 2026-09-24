package engine

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lisama569/event-correlation-engine-go/event"
	"github.com/lisama569/event-correlation-engine-go/rules"
)

// worker 以单 goroutine 顺序处理分到本分区的事件，
// 状态无锁；规则集通过 atomic.Pointer 读取，单次判定只见一个完整版本。
type worker struct {
	in       chan event.Event
	out      chan<- Result
	rules    *atomic.Pointer[rules.Set]
	lateness int64 // 允许的迟到时长（纳秒）
	store    *stateStore
	maxSeen  int64 // 已见最大事件时间（纳秒）
	started  bool
	wg       *sync.WaitGroup
}

func (w *worker) run() {
	defer w.wg.Done()
	for ev := range w.in {
		w.process(ev)
	}
	// 输入关闭：以无穷大水位闭合所有残留窗口，保证结果不缺失。
	w.fireWindows(math.MaxInt64)
}

func (w *worker) process(ev event.Event) {
	ts := ev.Timestamp.UnixNano()
	if !w.started || ts > w.maxSeen {
		w.maxSeen = ts
		w.started = true
	}
	watermark := w.maxSeen - w.lateness
	rs := w.rules.Load() // 本事件判定期间固定使用此快照
	if ts < watermark {
		// 超出容忍范围的迟到事件：单独报告，不进入任何窗口。
		w.out <- Result{
			Kind:           ResultLateEvent,
			RuleSetVersion: rs.Version,
			Event:          ev,
			Basis: fmt.Sprintf("event time %s is before watermark %s (allowed lateness %s); not counted",
				ev.Timestamp.UTC().Format(time.RFC3339Nano),
				time.Unix(0, watermark).UTC().Format(time.RFC3339Nano),
				time.Duration(w.lateness)),
		}
		return
	}
	for i := range rs.Rules {
		r := &rs.Rules[i]
		if r.EventType != ev.Type || !matchAll(r.Match, ev) {
			continue
		}
		w.addEvent(r, rs.Version, ev, ts)
	}
	w.fireWindows(watermark)
}

// addEvent 将事件计入对应窗口，并处理容量淘汰。
func (w *worker) addEvent(r *rules.Rule, version int, ev event.Event, ts int64) {
	key := keyFor(r, ev)
	ks, evicted := w.store.getOrCreate(key)
	if evicted != nil {
		w.flushEvicted(evicted)
	}
	winNs := time.Duration(r.WindowSeconds) * time.Second
	ws := int64(winNs)
	start := ts - mod(ts, ws)
	wk := windowKey{version: version, ruleID: r.ID, start: start}
	st, ok := ks.windows[wk]
	if !ok {
		st = &windowState{
			end:         start + ws,
			threshold:   r.Threshold,
			severity:    r.Severity,
			ruleVersion: version,
			ids:         make(map[string]struct{}),
		}
		ks.windows[wk] = st
	}
	st.ids[ev.ID] = struct{}{} // 按事件 ID 去重，迟到重投不重复计数
}

// fireWindows 闭合所有终点不超过水位的窗口。
func (w *worker) fireWindows(watermark int64) {
	w.store.forEach(func(ks *keyState) {
		for wk, st := range ks.windows {
			if st.end > watermark {
				continue
			}
			count := len(st.ids)
			if count >= st.threshold {
				w.out <- Result{
					Kind:           ResultAlert,
					RuleID:         wk.ruleID,
					RuleSetVersion: st.ruleVersion,
					Key:            ks.key,
					Count:          count,
					WindowStart:    time.Unix(0, wk.start).UTC(),
					WindowEnd:      time.Unix(0, st.end).UTC(),
					Basis: fmt.Sprintf("window [%s, %s): %d distinct events >= threshold %d (rule %q, ruleset v%d)",
						time.Unix(0, wk.start).UTC().Format(time.RFC3339Nano),
						time.Unix(0, st.end).UTC().Format(time.RFC3339Nano),
						count, st.threshold, wk.ruleID, st.ruleVersion),
				}
			}
			delete(ks.windows, wk)
		}
	})
}

// flushEvicted 强制结算被淘汰键的未闭合窗口，避免结果缺失。
func (w *worker) flushEvicted(ks *keyState) {
	for wk, st := range ks.windows {
		w.out <- Result{
			Kind:           ResultEvicted,
			RuleID:         wk.ruleID,
			RuleSetVersion: st.ruleVersion,
			Key:            ks.key,
			Count:          len(st.ids),
			WindowStart:    time.Unix(0, wk.start).UTC(),
			WindowEnd:      time.Unix(0, st.end).UTC(),
			Basis: fmt.Sprintf("key %q evicted due to memory bound; flushed %d distinct events (threshold %d) before window close",
				ks.key, len(st.ids), st.threshold),
		}
	}
}

func matchAll(conds []rules.Condition, ev event.Event) bool {
	for _, c := range conds {
		if ev.Fields[c.Field] != c.Equals {
			return false
		}
	}
	return true
}

// keyFor 提取关联键：优先取 Fields[key_field]，缺失时兜底为 Event.Key。
func keyFor(r *rules.Rule, ev event.Event) string {
	if v, ok := ev.Fields[r.KeyField]; ok && v != "" {
		return v
	}
	return ev.Key
}

func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
