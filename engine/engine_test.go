package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/event-correlation-engine-go/rules"
)

var base = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

func loadRules(t *testing.T, doc string) *rules.RuleSet {
	t.Helper()
	rs, err := rules.Load([]byte(doc))
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	return rs
}

func ev(id string, offset time.Duration, fields map[string]string) Event {
	return Event{ID: id, Timestamp: base.Add(offset), Fields: fields}
}

func logAlerts(t *testing.T, ev Event, alerts []Alert) {
	t.Helper()
	if len(alerts) == 0 {
		t.Logf("event id=%s ts=%s -> no alert", ev.ID, ev.Timestamp.Format(time.RFC3339))
		return
	}
	for _, a := range alerts {
		t.Logf("event id=%s ts=%s -> ALERT rule=%s ruleset_version=%d key=%s count=%d basis=%q",
			ev.ID, ev.Timestamp.Format(time.RFC3339), a.RuleID, a.RuleVersion, a.Key, a.Count, a.Reason)
	}
}

const rulesV1 = `{"version":1,"rules":[
  {"id":"login-fail","match":[{"field":"type","op":"eq","value":"login_fail"}],"window":"10m","threshold":3,"field":"src_ip"}
]}`

// 乱序到达 + 容忍范围内迟到事件被正确纳入且不重复计数。
func TestOutOfOrderAndLateWithinTolerance(t *testing.T) {
	rs := loadRules(t, rulesV1)
	e, err := New(Config{AllowedLateness: 2 * time.Minute, MaxKeys: 16}, rs)
	if err != nil {
		t.Fatal(err)
	}
	f := map[string]string{"type": "login_fail", "src_ip": "1.1.1.1"}

	// 乱序：先发 8 分钟处的事件，水位推进到 6 分钟。
	logAlerts(t, ev("e3", 8*time.Minute, f), e.Process(ev("e3", 8*time.Minute, f)))
	// 迟到但在容忍范围内（1m > 水位 6m-10m? 1m 窗口起点 0m，水位=6m，1m<=6m 超范围！）
	// 改用 7m（> 水位 6m），属于容忍范围内的乱序事件。
	logAlerts(t, ev("e1", 7*time.Minute, f), e.Process(ev("e1", 7*time.Minute, f)))
	logAlerts(t, ev("e2", 7*time.Minute+30*time.Second, f), e.Process(ev("e2", 7*time.Minute+30*time.Second, f)))

	alerts := e.Alerts()
	if len(alerts) != 1 {
		t.Fatalf("want 1 alert, got %d: %+v", len(alerts), alerts)
	}
	a := alerts[0]
	if a.RuleVersion != 1 || a.RuleID != "login-fail" || a.Key != "1.1.1.1" || a.Count != 3 {
		t.Fatalf("unexpected alert: %+v", a)
	}
	t.Logf("alert basis: %s", a.Reason)

	// 重复事件（相同 ID 乱序重发）不得重复计数或重复告警。
	logAlerts(t, ev("e1", 7*time.Minute, f), e.Process(ev("e1", 7*time.Minute, f)))
	if got := len(e.Alerts()); got != 1 {
		t.Fatalf("duplicate event caused extra alert: %d", got)
	}
	if got := len(e.LateEvents()); got != 0 {
		t.Fatalf("unexpected late events: %d", got)
	}
}

// 超出容忍范围的迟到事件必须被明确区分并单独报告。
func TestBeyondLatenessReported(t *testing.T) {
	rs := loadRules(t, rulesV1)
	e, _ := New(Config{AllowedLateness: 2 * time.Minute, MaxKeys: 16}, rs)
	f := map[string]string{"type": "login_fail", "src_ip": "2.2.2.2"}

	e.Process(ev("a1", 30*time.Minute, f)) // 水位 = 28m
	veryLate := ev("a0", 1*time.Minute, f)
	logAlerts(t, veryLate, e.Process(veryLate))

	lates := e.LateEvents()
	if len(lates) != 1 {
		t.Fatalf("want 1 late event report, got %d", len(lates))
	}
	t.Logf("late event reported: id=%s event_ts=%s watermark=%s reason=%q",
		lates[0].Event.ID, lates[0].Event.Timestamp.Format(time.RFC3339),
		time.Unix(0, lates[0].Watermark).Format(time.RFC3339), lates[0].Reason)
	// 超范围迟到不得污染已确认结果
	if got := len(e.Alerts()); got != 0 {
		t.Fatalf("late event polluted results: %d alerts", got)
	}
	// 水位不被迟到事件回推
	e.Process(ev("a2", 31*time.Minute, f))
	e.Process(ev("a3", 32*time.Minute, f))
	if got := len(e.Alerts()); got != 1 {
		t.Fatalf("want 1 alert from on-time events, got %d", got)
	}
}

// 有界状态：超出 MaxKeys 后淘汰不 panic、不丢已确认告警。
func TestBoundedStateEviction(t *testing.T) {
	rs := loadRules(t, rulesV1)
	e, _ := New(Config{AllowedLateness: time.Minute, MaxKeys: 4}, rs)
	for i := 0; i < 20; i++ {
		f := map[string]string{"type": "login_fail", "src_ip": fmt.Sprintf("10.0.0.%d", i)}
		e.Process(ev(fmt.Sprintf("k%d", i), time.Duration(i)*time.Second, f))
	}
	e.mu.Lock()
	n := len(e.states)
	e.mu.Unlock()
	if n > 4 {
		t.Fatalf("state count %d exceeds MaxKeys=4", n)
	}
	t.Logf("states bounded: %d <= 4", n)
}

// 并发热更新：处理过程中反复 Reload，判定只能看到完整版本，且竞态检测下通过。
func TestConcurrentHotReload(t *testing.T) {
	rsV1 := loadRules(t, rulesV1)
	rsV2 := loadRules(t, `{"version":2,"rules":[
	  {"id":"login-fail","match":[{"field":"type","op":"eq","value":"login_fail"}],"window":"10m","threshold":3,"group_by":"src_ip","severity":"high"}
	]}`)
	e, _ := New(Config{AllowedLateness: 5 * time.Minute, MaxKeys: 64}, rsV1)

	var wg sync.WaitGroup
	// 写入方：交替热更新两个版本
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if (i+g)%2 == 0 {
					_ = e.Reload(rsV1)
				} else {
					_ = e.Reload(rsV2)
				}
			}
		}(g)
	}
	// 处理方：持续处理事件
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				f := map[string]string{"type": "login_fail", "src_ip": fmt.Sprintf("192.168.%d.%d", g, i%8)}
				ev := ev(fmt.Sprintf("g%d-e%d", g, i), time.Duration(i)*time.Second, f)
				for _, a := range e.Process(ev) {
					if a.RuleVersion != 1 && a.RuleVersion != 2 {
						t.Errorf("alert with mixed/unknown ruleset version: %+v", a)
					}
				}
			}
		}(g)
	}
	wg.Wait()

	// 所有累计告警的版本都必须是完整版本之一
	for _, a := range e.Alerts() {
		if a.RuleVersion != 1 && a.RuleVersion != 2 {
			t.Fatalf("invalid rule version in alert: %+v", a)
		}
	}
	t.Logf("concurrent reload done: alerts=%d lates=%d final_rules_version=%d",
		len(e.Alerts()), len(e.LateEvents()), e.RulesVersion())
}
