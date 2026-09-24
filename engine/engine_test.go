package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/event-correlation-engine-go/event"
	"github.com/lisama569/event-correlation-engine-go/rules"
)

const rulesV1 = `{"version":1,"rules":[
	{"id":"r1","event_type":"login_fail","key_field":"user","window_seconds":60,"threshold":3,"severity":"high"}
]}`

const rulesV2 = `{"version":2,"rules":[
	{"id":"r1","event_type":"login_fail","key_field":"user","window_seconds":60,"threshold":2,"severity":"critical","description":"v2 lowered threshold"}
]}`

func mustSet(t *testing.T, doc string) *rules.Set {
	t.Helper()
	rs, err := rules.Load([]byte(doc))
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	return rs
}

func ev(id string, sec int64, user string) event.Event {
	e := event.Event{
		ID:        id,
		Type:      "login_fail",
		Key:       user,
		Timestamp: time.Unix(sec, 0).UTC(),
		Fields:    map[string]string{"user": user},
	}
	return e
}

func submitAll(t *testing.T, e *Engine, evs []event.Event) {
	t.Helper()
	for _, ev := range evs {
		t.Logf("input event: id=%s type=%s ts=%s fields=%v", ev.ID, ev.Type, ev.Timestamp.Format(time.RFC3339), ev.Fields)
		if err := e.Submit(ev); err != nil {
			t.Fatalf("submit %s: %v", ev.ID, err)
		}
	}
}

func drain(e *Engine) []Result {
	var out []Result
	for r := range e.Results() {
		out = append(out, r)
	}
	return out
}

func logResults(t *testing.T, res []Result) {
	t.Helper()
	for _, r := range res {
		t.Logf("result: kind=%d rule=%q ruleset_version=%d key=%q count=%d basis=%s",
			r.Kind, r.RuleID, r.RuleSetVersion, r.Key, r.Count, r.Basis)
	}
}

// 场景一：乱序到达、允许范围内的迟到去重、超范围迟到单独报告。
func TestOutOfOrderAndLateEvents(t *testing.T) {
	eng, err := New(Config{Workers: 1, AllowedLateness: 30 * time.Second}, mustSet(t, rulesV1))
	if err != nil {
		t.Fatal(err)
	}
	submitAll(t, eng, []event.Event{
		ev("e1", 100, "u1"),
		ev("e2", 90, "u1"),  // 乱序，但在容忍范围内
		ev("e2", 90, "u1"),  // 重复投递，不得重复计数
		ev("e3", 105, "u1"), // 窗口 [60,120) 凑满 3 条
		ev("e4", 200, "u1"), // 推进水位至 170，闭合 [60,120)
		ev("e5", 50, "u1"),  // 超出容忍范围的迟到事件
	})
	eng.Close()
	res := drain(eng)
	logResults(t, res)

	var alerts, lates int
	for _, r := range res {
		switch r.Kind {
		case ResultAlert:
			alerts++
			if r.RuleID != "r1" || r.Count != 3 || r.RuleSetVersion != 1 {
				t.Fatalf("unexpected alert: %+v", r)
			}
			if !r.WindowStart.Equal(time.Unix(60, 0).UTC()) || !r.WindowEnd.Equal(time.Unix(120, 0).UTC()) {
				t.Fatalf("unexpected window: %+v", r)
			}
		case ResultLateEvent:
			lates++
			if r.Event.ID != "e5" {
				t.Fatalf("late event = %s, want e5", r.Event.ID)
			}
		default:
			t.Fatalf("unexpected result kind: %+v", r)
		}
	}
	if alerts != 1 || lates != 1 {
		t.Fatalf("alerts=%d lates=%d, want 1/1", alerts, lates)
	}
}

// 场景二：非法规则重载被拒绝且旧规则集继续生效；跨版本重载后按新版本判定。
func TestReloadInvalidAndCrossVersion(t *testing.T) {
	eng, err := New(Config{Workers: 1, AllowedLateness: 30 * time.Second}, mustSet(t, rulesV1))
	if err != nil {
		t.Fatal(err)
	}
	// 非法规则：未知字段
	if err := eng.Reload([]byte(`{"version":1,"rules":[{"id":"x","event_type":"e","window_seconds":5,"bogus":1}]}`)); err == nil {
		t.Fatal("expected reload error for unknown field")
	} else {
		t.Logf("reload rejected as expected: %v", err)
	}
	// 非法规则：不支持的版本
	if err := eng.Reload([]byte(`{"version":9,"rules":[]}`)); err == nil {
		t.Fatal("expected reload error for unsupported version")
	} else {
		t.Logf("reload rejected as expected: %v", err)
	}
	// 旧规则集（v1, threshold=3）仍然生效：2 条不触发，第 3 条触发。
	// a4 推进水位闭合窗口 [60,120)，从输出通道同步读取，确保重载前判定已完成。
	submitAll(t, eng, []event.Event{ev("a1", 100, "u1"), ev("a2", 101, "u1"), ev("a3", 102, "u1"), ev("a4", 200, "u1")})
	r1 := <-eng.Results()
	t.Logf("result: kind=%d rule=%q ruleset_version=%d key=%q count=%d basis=%s",
		r1.Kind, r1.RuleID, r1.RuleSetVersion, r1.Key, r1.Count, r1.Basis)
	if r1.Kind != ResultAlert || r1.RuleSetVersion != 1 || r1.Count != 3 {
		t.Fatalf("expected v1 alert count=3, got %+v", r1)
	}
	// 跨版本热更新到 v2（threshold=2）
	if err := eng.Reload([]byte(rulesV2)); err != nil {
		t.Fatalf("reload v2: %v", err)
	}
	// 新窗口 [180,240)：v2 阈值 2，两条即触发；b3 推进水位闭合该窗口。
	submitAll(t, eng, []event.Event{ev("b1", 190, "u1"), ev("b2", 191, "u1"), ev("b3", 400, "u1")})
	r2 := <-eng.Results()
	t.Logf("result: kind=%d rule=%q ruleset_version=%d key=%q count=%d basis=%s",
		r2.Kind, r2.RuleID, r2.RuleSetVersion, r2.Key, r2.Count, r2.Basis)
	if r2.Kind != ResultAlert || r2.RuleSetVersion != 2 || r2.Count != 2 {
		t.Fatalf("expected v2 alert count=2, got %+v", r2)
	}
	eng.Close()
	for range eng.Results() {
	}
}

// 场景三：处理过程中并发热更新（需 -race 运行），不得出现混合版本或数据竞争。
func TestConcurrentHotReload(t *testing.T) {
	eng, err := New(Config{Workers: 4, AllowedLateness: time.Minute}, mustSet(t, rulesV1))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = eng.Submit(ev(fmt.Sprintf("w%d-%d", w, i), int64(1000+i), fmt.Sprintf("u%d", w)))
			}
		}(w)
	}
	// 并发交替重载 v1/v2
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			if err := eng.Reload([]byte(rulesV2)); err != nil {
				t.Fatalf("reload v2: %v", err)
			}
		} else {
			if err := eng.Reload([]byte(rulesV1)); err != nil {
				t.Fatalf("reload v1: %v", err)
			}
		}
	}
	wg.Wait()
	eng.Close()
	res := drain(eng)
	for _, r := range res {
		if r.RuleSetVersion != 1 && r.RuleSetVersion != 2 {
			t.Fatalf("result references mixed/unknown ruleset version: %+v", r)
		}
	}
	t.Logf("processed with concurrent reloads, results=%d", len(res))
}

// 内存上限下的键淘汰：被淘汰键的未闭合窗口必须强制结算而非丢失。
func TestEvictionFlushesPartialState(t *testing.T) {
	eng, err := New(Config{Workers: 1, MaxKeys: 2, AllowedLateness: time.Hour}, mustSet(t, rulesV1))
	if err != nil {
		t.Fatal(err)
	}
	// 三个不同键，MaxKeys=2，第三个键触发 LRU 淘汰。
	submitAll(t, eng, []event.Event{
		ev("k1", 100, "user-a"),
		ev("k2", 101, "user-b"),
		ev("k3", 102, "user-c"), // 淘汰最久未使用的 user-a
	})
	eng.Close()
	res := drain(eng)
	logResults(t, res)
	var evicted int
	for _, r := range res {
		if r.Kind == ResultEvicted {
			evicted++
			if r.Key != "user-a" || r.Count != 1 {
				t.Fatalf("unexpected eviction: %+v", r)
			}
		}
	}
	if evicted != 1 {
		t.Fatalf("evicted=%d, want 1", evicted)
	}
}
